from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from typing import Optional
import MetaTrader5 as mt5
from candle_service import CandleDataService, CandleRequest, get_broker_utc_offset
from configs import Config
import asyncio
import logging

# Set up logging to track request processing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize MT5 once when the server starts.
    This runs in the main event loop, but the initialization itself is synchronous.
    If MT5 initialization is blocking, it will block the event loop briefly.
    That is acceptable because it happens only at startup.
    """
    logger.info("Initializing MT5...")
    if not mt5.initialize(
        path=Config.PATH_MT5_EXEC,
        password=Config.MT5_ACCOUNT_PASSWORD,
        login=Config.MT5_ACCOUNT_NUMBER,
        server=Config.MT5_ACCOUNT_SERVER,
    ):
        code, desc = mt5.last_error()
        raise RuntimeError(f"MT5 init failed: {code} - {desc}")

    if not mt5.login(
        Config.MT5_ACCOUNT_NUMBER,
        Config.MT5_ACCOUNT_PASSWORD,
        Config.MT5_ACCOUNT_SERVER,
    ):
        code, desc = mt5.last_error()
        raise RuntimeError(f"MT5 login failed: {code} - {desc}")

    logger.info("MT5 logged in")
    get_broker_utc_offset()
    yield
    mt5.shutdown()
    logger.info("MT5 shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    """Quick health check endpoint."""
    return {"status": "healthy"}


@app.post("/time-series", response_model=dict)
async def get_time_series_body(request: CandleRequest):
    """
    Async endpoint that offloads the blocking CandleDataService call
    to a separate thread using asyncio.to_thread.
    """
    try:
        # Offload the blocking function to a thread, allowing the event loop to continue.
        result = await asyncio.to_thread(
            CandleDataService.get_multiple_timeframes, request
        )
        return result
    except Exception as e:
        logger.error(f"Error processing /time-series (body): {e}")
        return {"error": str(e)}


@app.get("/time-series", response_model=dict)
async def get_time_series_query(
    symbols: str = Query(
        ..., description="Comma-separated symbols (e.g., EURUSD,GBPUSD)"
    ),
    timeframes: str = Query(
        "1h", description="Comma-separated timeframes (1m,15m,1h,4h,d1)"
    ),
    limit: Optional[int] = Query(
        None, description="Number of candles to return", gt=0, le=1000
    ),
    from_date: Optional[str] = Query(
        None, description="Start date UTC (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"
    ),
    to_date: Optional[str] = Query(
        None, description="End date UTC (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"
    ),
):
    """
    Async endpoint that offloads the blocking call to a thread.
    """
    try:
        # Prepare the request object
        candle_request = CandleRequest(
            symbols=[s.strip() for s in symbols.split(",")],
            timeframes=[tf.strip() for tf in timeframes.split(",")],
            limit=limit,
            from_date=from_date,
            to_date=to_date,
        )
        result = await asyncio.to_thread(
            CandleDataService.get_multiple_timeframes, candle_request
        )
        return result
    except Exception as e:
        logger.error(f"Error processing /time-series (query): {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    # Run with a single worker; the thread pool handles concurrency.
    # If you need multiple processes, use "main:app" string and workers >1.
    # But note: multiple workers would each create their own MT5 connection.
    uvicorn.run("main:app", host="0.0.0.0", port=8000)  # workers=1 by default
