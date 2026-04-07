from contextlib import asynccontextmanager
import asyncio
import logging

import MetaTrader5 as mt5
from fastapi import FastAPI, Query
from typing import Optional

from candle_service import (
    CandleRequest,
    get_multiple,
    preload_symbols,
)
from configs import Config

logging.basicConfig(level=Config.LOG_LEVEL)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not mt5.initialize(
        path=Config.PATH_MT5_EXEC,
        login=Config.MT5_ACCOUNT_NUMBER,
        password=Config.MT5_ACCOUNT_PASSWORD,
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

    await asyncio.to_thread(preload_symbols)
    logger.info("MT5 ready")

    yield

    mt5.shutdown()
    logger.info("MT5 shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/time-series")
async def time_series_body(request: CandleRequest):
    return await asyncio.to_thread(get_multiple, request)


@app.get("/time-series")
async def time_series_query(
    symbols: str = Query(...),
    timeframes: str = Query("1h"),
    limit: Optional[int] = Query(None, gt=0, le=1000),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    request = CandleRequest(
        symbols=[s.strip() for s in symbols.split(",")],
        timeframes=[tf.strip() for tf in timeframes.split(",")],
        limit=limit,
        from_date=from_date,
        to_date=to_date,
    )
    return await asyncio.to_thread(get_multiple, request)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
