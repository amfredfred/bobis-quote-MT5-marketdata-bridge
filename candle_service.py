import logging
import MetaTrader5 as mt5
import pytz
from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel, validator
from fastapi import HTTPException
from configs import Config
from utils import clean_symbol

logger = logging.getLogger(__name__)

_BROKER_UTC_OFFSET: Optional[int] = None


def get_broker_utc_offset() -> int:
    global _BROKER_UTC_OFFSET
    if _BROKER_UTC_OFFSET is not None:
        return _BROKER_UTC_OFFSET

    server_time = mt5.symbol_info_tick("EURUSD")
    if server_time is None:
        symbols = mt5.symbols_get()
        for s in symbols or []:
            tick = mt5.symbol_info_tick(s.name)
            if tick:
                server_time = tick
                break

    if server_time is None:
        logger.warning("Could not get tick time — assuming broker UTC+0")
        return 0

    true_utc_now = int(datetime.now(pytz.UTC).timestamp())
    broker_now = server_time.time
    offset = round((broker_now - true_utc_now) / 3600)
    _BROKER_UTC_OFFSET = offset
    logger.info(
        "Broker UTC offset derived: UTC+%d  (broker=%d, utc=%d)",
        offset,
        broker_now,
        true_utc_now,
    )
    return _BROKER_UTC_OFFSET


# ========== MODELS ==========


class Candle(BaseModel):
    timestamp: int  # UTC milliseconds
    open: float
    high: float
    low: float
    close: float
    volume: float


class CandleRequest(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    limit: Optional[int] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    timezone: str = Config.TIMEZONE()

    @validator("timeframes")
    def validate_timeframes(cls, v):
        valid_tfs = {"1m", "5m", "15m", "30m", "1h", "4h", "d1", "w1", "mn1"}
        for tf in v:
            if tf.lower() not in valid_tfs:
                raise ValueError(f"Invalid timeframe: {tf}")
        return v


# ========== UTILITIES ==========


class TimeframeConverter:
    @staticmethod
    def to_mt5(timeframe: str) -> int:
        timeframe = timeframe.lower()
        if timeframe.endswith("m"):
            minutes = int(timeframe[:-1])
            return {
                1: mt5.TIMEFRAME_M1,
                5: mt5.TIMEFRAME_M5,
                15: mt5.TIMEFRAME_M15,
                30: mt5.TIMEFRAME_M30,
            }.get(minutes, mt5.TIMEFRAME_M1)
        elif timeframe.endswith("h"):
            hours = int(timeframe[:-1])
            return {
                1: mt5.TIMEFRAME_H1,
                4: mt5.TIMEFRAME_H4,
            }.get(hours, mt5.TIMEFRAME_H1)
        elif timeframe == "d1":
            return mt5.TIMEFRAME_D1
        elif timeframe == "w1":
            return mt5.TIMEFRAME_W1
        elif timeframe == "mn1":
            return mt5.TIMEFRAME_MN1
        raise ValueError(f"Unsupported timeframe: {timeframe}")


# ========== SERVICE ==========


class CandleDataService:

    @staticmethod
    def broker_ts_to_utc_ms(server_timestamp: int) -> int:
        """Convert a broker server timestamp to UTC milliseconds."""
        offset = get_broker_utc_offset()
        utc_sec = server_timestamp - (offset * 3600)
        return utc_sec * 1000

    @staticmethod
    def parse_date_string(date_str: str, timezone: str = Config.TIMEZONE()) -> datetime:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                tz_obj = pytz.timezone(timezone)
                return tz_obj.localize(dt)
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date string: {date_str!r}")

    @staticmethod
    def get_candles(
        symbol: str,
        timeframe: str,
        limit: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timezone: str = Config.TIMEZONE(),
    ) -> List[Candle]:
        try:
            timeframe_enum = TimeframeConverter.to_mt5(timeframe)

            dt_from: Optional[datetime] = None
            dt_to: Optional[datetime] = None

            if from_date:
                dt_from = CandleDataService.parse_date_string(from_date, timezone)
                dt_from = dt_from.astimezone(pytz.UTC)
                dt_from += timedelta(hours=get_broker_utc_offset())

            if to_date:
                dt_to = CandleDataService.parse_date_string(to_date, timezone)
                dt_to = dt_to.astimezone(pytz.UTC)
                dt_to += timedelta(hours=get_broker_utc_offset())

            if dt_from and dt_to:
                rates = mt5.copy_rates_range(symbol, timeframe_enum, dt_from, dt_to)
            elif dt_from:
                dt_to = datetime.now(pytz.UTC) + timedelta(
                    hours=get_broker_utc_offset()
                )
                rates = mt5.copy_rates_range(symbol, timeframe_enum, dt_from, dt_to)
            elif limit:
                rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, limit)
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Provide at least one of: from_date, to_date, or limit.",
                )

            if rates is None or len(rates) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data for {symbol} {timeframe}",
                )

            candles: List[Candle] = []
            for rate in rates:
                volume = (
                    rate["real_volume"]
                    if "real_volume" in rate.dtype.names
                    else rate["tick_volume"]
                )
                candles.append(
                    Candle(
                        timestamp=CandleDataService.broker_ts_to_utc_ms(rate["time"]),
                        open=float(rate["open"]),
                        high=float(rate["high"]),
                        low=float(rate["low"]),
                        close=float(rate["close"]),
                        volume=float(volume),
                    )
                )

            candles.sort(key=lambda c: c.timestamp)
            return candles

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching data for {symbol} {timeframe}: {e}",
            )

    @staticmethod
    def get_multiple_timeframes(request: CandleRequest) -> dict:
        result: dict = {}
        for symbol in request.symbols:
            symbol_data: dict = {}
            for tf in request.timeframes:
                try:
                    symbol_data[tf] = CandleDataService.get_candles(
                        clean_symbol(symbol),
                        tf,
                        request.limit,
                        request.from_date,
                        request.to_date,
                        request.timezone,
                    )
                except HTTPException as e:
                    logger.error(
                        "MT5 error for %s/%s: %s — mt5_error=%s",
                        symbol,
                        tf,
                        e.detail,
                        mt5.last_error(),
                    )
                    symbol_data[tf] = {"error": str(e.detail)}
            result[symbol] = symbol_data
        return result
