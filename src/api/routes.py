"""FastAPI routes for MT5 market data endpoints."""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Query, Request

from src.core import (
    CandleRequest,
    FetchSuccess,
    MarketDataProvider,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["market-data"])


@router.get("/health")
async def health(request: Request):
    """Health check endpoint"""
    provider: MarketDataProvider = request.app.state.provider
    try:
        provider._worker.ensure_connected()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "reason": str(e)}


@router.post("/time-series")
async def time_series_body(request: CandleRequest, http_request: Request):
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
    provider: MarketDataProvider = http_request.app.state.provider
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


@router.get("/time-series")
async def time_series_query(
    symbols: str = Query(..., description="Comma-separated symbols, e.g., EURUSD,GBPUSD"),
    timeframes: str = Query("1h", description="Comma-separated timeframes, e.g., 1h,4h,d1"),
    limit: Optional[int] = Query(None, gt=0, le=5000, description="Number of bars to fetch"),
    from_date: Optional[str] = Query(None, description="Start date ISO format, e.g., 2024-01-01"),
    to_date: Optional[str] = Query(None, description="End date ISO format"),
    allow_gaps: bool = Query(False, description="Allow gaps in data"),
    check_staleness: bool = Query(True, description="Check for stale data"),
    request: Request = None,
):
    """
    Fetch candles using GET with query parameters

    Examples:
    - /api/v1/time-series?symbols=EURUSD&timeframes=1h&limit=100
    - /api/v1/time-series?symbols=EURUSD,GBPUSD&timeframes=1h,4h&from_date=2024-01-01&to_date=2024-01-31
    """
    provider: MarketDataProvider = request.app.state.provider

    candle_request = CandleRequest(
        symbols=[s.strip() for s in symbols.split(",")],
        timeframes=[tf.strip() for tf in timeframes.split(",")],
        limit=limit,
        from_date=from_date,
        to_date=to_date,
        allow_gaps=allow_gaps,
        check_staleness=check_staleness,
    )
    logger.debug(f"Received GET request: {candle_request}")

    result = await asyncio.to_thread(provider.get_multiple, candle_request)

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


@router.get("/candles/{symbol}")
async def get_single_symbol(
    symbol: str,
    timeframe: str = Query("1h"),
    limit: Optional[int] = Query(100, gt=0, le=5000),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    request: Request = None,
):
    """
    Simplified endpoint for single symbol queries

    Example:
    - /api/v1/candles/EURUSD?timeframe=1h&limit=200
    """
    provider: MarketDataProvider = request.app.state.provider

    try:
        candles = await asyncio.to_thread(
            provider.get_candles,
            symbol,
            timeframe,
            limit,
            from_date,
            to_date,
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
        logger.exception(f"Error fetching candles for {symbol}/{timeframe}")
        return {"error": str(e), "symbol": symbol, "timeframe": timeframe}
