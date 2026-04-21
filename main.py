from contextlib import asynccontextmanager
import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, Query

from candle_service import MarketDataProvider, CandleRequest, FetchSuccess, FetchFailure
from configs import Config

logging.basicConfig(
    level=Config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global provider instance
provider: Optional[MarketDataProvider] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global provider

    # Initialize the provider (this handles MT5 setup via its own worker thread)
    try:
        provider = MarketDataProvider(Config)
        logger.info("MarketDataProvider initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MarketDataProvider: {e}")
        raise

    yield

    # Clean shutdown
    if provider:
        provider.shutdown()
        logger.info("MarketDataProvider shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    """Health check endpoint"""
    if provider is None:
        return {"status": "unhealthy", "reason": "provider not initialized"}

    try:
        # Verify MT5 is responsive
        provider._worker.ensure_connected()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "reason": str(e)}


@app.post("/time-series")
async def time_series_body(request: CandleRequest):
    """
    Fetch candles using POST with JSON body

    Example:
    {
        "symbols": ["EURUSD", "GBPUSD"],
        "timeframes": ["1h", "4h"],
        "limit": 100,
        "allow_gaps": false,
        "check_staleness": true
    }
    """
    if provider is None:
        return {"error": "Provider not initialized"}

    # Run the blocking call in a thread pool
    # provider.get_multiple() internally uses its own MT5Worker thread,
    # so it's safe to call from any thread
    result = await asyncio.to_thread(provider.get_multiple, request)

    # Convert to serializable format
    serialized = {}
    for symbol, timeframes in result.items():
        serialized[symbol] = {}
        for tf, fetch_result in timeframes.items():
            if isinstance(fetch_result, FetchSuccess):
                serialized[symbol][tf] = {
                    "status": "success",
                    "count": len(fetch_result.candles),
                    "candles": [
                        {
                            "timestamp": c.timestamp,
                            "open": c.open,
                            "high": c.high,
                            "low": c.low,
                            "close": c.close,
                            "volume": c.volume,
                            "is_tick_volume": c.is_tick_volume,
                        }
                        for c in fetch_result.candles
                    ],
                }
            else:
                serialized[symbol][tf] = {
                    "status": "error",
                    "error": fetch_result.error,
                    "error_type": fetch_result.error_type,
                }

    return serialized


@app.get("/time-series")
async def time_series_query(
    symbols: str = Query(
        ..., description="Comma-separated symbols, e.g., EURUSD,GBPUSD"
    ),
    timeframes: str = Query(
        "1h", description="Comma-separated timeframes, e.g., 1h,4h,d1"
    ),
    limit: Optional[int] = Query(
        None, gt=0, le=5000, description="Number of bars to fetch"
    ),
    from_date: Optional[str] = Query(
        None, description="Start date ISO format, e.g., 2024-01-01"
    ),
    to_date: Optional[str] = Query(None, description="End date ISO format"),
    allow_gaps: bool = Query(False, description="Allow gaps in data"),
    check_staleness: bool = Query(True, description="Check for stale data"),
):
    """
    Fetch candles using GET with query parameters

    Examples:
    - /time-series?symbols=EURUSD&timeframes=1h&limit=100
    - /time-series?symbols=EURUSD,GBPUSD&timeframes=1h,4h&from_date=2024-01-01&to_date=2024-01-31
    """
    if provider is None:
        return {"error": "Provider not initialized"}

    request = CandleRequest(
        symbols=[s.strip() for s in symbols.split(",")],
        timeframes=[tf.strip() for tf in timeframes.split(",")],
        limit=limit,
        from_date=from_date,
        to_date=to_date,
        allow_gaps=allow_gaps,
        check_staleness=check_staleness,
    )
    print(f"Received GET request with: {request}")

    result = await asyncio.to_thread(provider.get_multiple, request)

    # Simplified response for GET requests
    response = {}
    for symbol, timeframes in result.items():
        response[symbol] = {}
        for tf, fetch_result in timeframes.items():
            if isinstance(fetch_result, FetchSuccess):
                candles = fetch_result.candles
                response[symbol][tf] = {
                    "count": len(candles),
                    "latest": (
                        {
                            "timestamp": candles[-1].timestamp,
                            "close": candles[-1].close,
                            "volume": candles[-1].volume,
                        }
                        if candles
                        else None
                    ),
                    "time_range": {
                        "from": candles[0].timestamp if candles else None,
                        "to": candles[-1].timestamp if candles else None,
                    },
                }
            else:
                response[symbol][tf] = {"error": fetch_result.error}

    return response


@app.get("/candles/{symbol}")
async def get_single_symbol(
    symbol: str,
    timeframe: str = Query("1h"),
    limit: Optional[int] = Query(100, gt=0, le=5000),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    """
    Simplified endpoint for single symbol queries

    Example:
    - /candles/EURUSD?timeframe=1h&limit=200
    """
    if provider is None:
        return {"error": "Provider not initialized"}

    try:
        candles = await asyncio.to_thread(
            provider.get_candles,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            from_date=from_date,
            to_date=to_date,
        )

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(candles),
            "candles": [
                {
                    "timestamp": c.timestamp,
                    "datetime": c.timestamp / 1000,  # Unix seconds for JSON
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                    "is_tick_volume": c.is_tick_volume,
                }
                for c in candles
            ],
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol, "timeframe": timeframe}


if __name__ == "__main__":
    import uvicorn

    # Note: Don't initialize MT5 here - the lifespan does it
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Set to True for development, but careful with MT5
        log_level="info",
    )
