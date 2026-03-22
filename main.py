from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from typing import Optional
import MetaTrader5 as mt5
from candle_service import CandleDataService, CandleRequest, get_broker_utc_offset
from configs import Config


@asynccontextmanager
async def lifespan(app: FastAPI):
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

    print("MT5 logged in")
    get_broker_utc_offset()
    yield
    mt5.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/time-series", response_model=dict)
async def get_time_series_body(request: CandleRequest):
    return CandleDataService.get_multiple_timeframes(request)


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
        None, description="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"
    ),
    to_date: Optional[str] = Query(
        None, description="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"
    ),
    timezone: str = Query("UTC", description="Timezone for timestamps"),
):
    return CandleDataService.get_multiple_timeframes(
        CandleRequest(
            symbols=[s.strip() for s in symbols.split(",")],
            timeframes=[tf.strip() for tf in timeframes.split(",")],
            limit=limit,
            from_date=from_date,
            to_date=to_date,
            timezone=timezone,
        )
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
