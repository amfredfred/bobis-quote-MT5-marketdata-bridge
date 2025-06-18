from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import MetaTrader5 as mt5
from trading import TradingService
from candle_service import CandleDataService, Candle, CandleRequest
from typing import List
from configs import Config

app = FastAPI()

isInitialized = mt5.initialize(path=Config.PATH_TO_MT5_EXE, porable=True)
if not isInitialized:
    error_code, description = mt5.last_error()
    raise Exception(f"MT5 Initialization Failed: {error_code} - {description}")
isLoggedIn = mt5.login(
    Config.MT5_ACCOUNT_NUMBER, Config.MT5_ACCOUNT_PASSWORD, Config.MT5_ACCOUNT_SERVER
)   
if isInitialized and isLoggedIn:
    print("Logged in")
else:
    error_code, description = mt5.last_error()
    raise Exception(f"MT5 login Failed!: {error_code} - {description}")


class TakeProfitLevel(BaseModel):
    name: str  # TP1, TP2, etc.
    price: float
    percentage: Optional[float] = None
    reason: Optional[str] = None


class Entry(BaseModel):
    price: float
    type: str  # "limit", "stop", "market"
    validUntil: Optional[str] = None


class TradeSignal(BaseModel):
    symbol: str
    direction: str  # "BUY", "SELL", "HOLD"
    entry: Entry
    stopLoss: float
    takeProfits: List[TakeProfitLevel]
    confidence: float
    reason: str
    timestamp: str


@app.post("/trade")
async def execute_trade(signal: TradeSignal):
    """Endpoint to execute trades"""
    try:
        return TradingService.process_signal(signal)
    finally:
        print("Waht wen wrong")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


# ========== API ENDPOINTS ==========
@app.post("/time-series", response_model=dict)
async def get_time_series_body(request: CandleRequest):
    """
    Get candle data via POST request with body
    Example: {"symbols": ["EURUSD", "GBPUSD"], "timeframes": ["1h", "4h"], "limit": 50}
    """ 
    return CandleDataService.get_multiple_timeframes(request)


@app.get("/time-series", response_model=dict)
async def get_time_series_query(
    symbols: str = Query(
        ..., description="Comma-separated symbols (e.g., EURUSD,GBPUSD)"
    ),
    timeframes: str = Query(
        "1h", description="Comma-separated timeframes (1m,15m,1h,4h,d1)"
    ),
    limit: int = Query(100, description="Number of candles to return", gt=0, le=1000),
    timezone: str = Query("Africa/Lagos", description="Timezone for timestamps"),
):
    """
    Get candle data via GET request with query parameters
    Example: /time-series?symbols=EURUSD,GBPUSD&timeframes=1h,4h&limit=50
    """
    return CandleDataService.get_multiple_timeframes(
        CandleRequest(
            symbols=[s.strip() for s in symbols.split(",")],
            timeframes=[tf.strip() for tf in timeframes.split(",")],
            limit=limit,
            timezone=timezone,
        )
    )


# ========== MAIN ==========
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
