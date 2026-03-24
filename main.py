from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import MetaTrader5 as mt5
from candle_service import (
    CandleDataService,
    CandleRequest,
    get_broker_utc_offset,
    preload_symbols,
)
from configs import Config
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================
# LIFECYCLE
# =========================


@asynccontextmanager
async def lifespan(app: FastAPI):
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

    # Run preload + offset in thread to avoid blocking event loop
    await asyncio.to_thread(preload_symbols)
    await asyncio.to_thread(get_broker_utc_offset)

    yield

    mt5.shutdown()
    logger.info("MT5 shut down")


app = FastAPI(lifespan=lifespan)


# =========================
# HEALTH
# =========================


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# =========================
# BODY ENDPOINT
# =========================


@app.post("/time-series", response_model=dict)
async def get_time_series_body(request: CandleRequest):
    try:
        result = await asyncio.to_thread(
            CandleDataService.get_multiple_timeframes, request
        )
        return result

    except Exception:
        logger.exception("Error processing /time-series (body)")
        raise HTTPException(status_code=500, detail="Internal server error")


# =========================
# QUERY ENDPOINT
# =========================


@app.get("/time-series", response_model=dict)
async def get_time_series_query(
    symbols: str = Query(...),
    timeframes: str = Query("1h"),
    limit: Optional[int] = Query(None, gt=0, le=1000),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    try:
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

    except Exception:
        logger.exception("Error processing /time-series (query)")
        raise HTTPException(status_code=500, detail="Internal server error")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    import uvicorn

    # IMPORTANT: keep workers=1 (MT5 is not multi-process safe)
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
