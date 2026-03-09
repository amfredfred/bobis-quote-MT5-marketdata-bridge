import logging
import threading
import MetaTrader5 as mt5
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from pydantic import BaseModel, validator
from fastapi import HTTPException
from configs import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import zoneinfo

logger = logging.getLogger(__name__)

_BROKER_UTC_OFFSET: Optional[int] = None
_OFFSET_SYMBOLS = ["BTCUSD", "ETHUSD", "BTCUSDT", "ETHUSDT"]
_MT5_LOCK = threading.Lock()
_THREAD_POOL = ThreadPoolExecutor(max_workers=20)


def clean_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace("_", "")


def get_broker_utc_offset() -> int:
    global _BROKER_UTC_OFFSET
    if _BROKER_UTC_OFFSET is not None:
        return _BROKER_UTC_OFFSET

    tick = None
    for symbol in _OFFSET_SYMBOLS:
        t = mt5.symbol_info_tick(symbol)
        if t is not None:
            tick = t
            break

    if tick is None:
        logger.warning("No crypto tick available — assuming broker UTC+0")
        return 0

    true_utc_now = datetime.now(timezone.utc).timestamp()
    broker_ts = tick.time_msc / 1000.0 if tick.time_msc else float(tick.time)
    raw_offset = (broker_ts - true_utc_now) / 3600
    _BROKER_UTC_OFFSET = round(raw_offset)
    logger.info(
        "raw broker_ts=%.3f, true_utc=%.3f, raw_offset=%.4f, rounded=%d",
        broker_ts,
        true_utc_now,
        raw_offset,
        _BROKER_UTC_OFFSET,
    )
    logger.info(
        "Broker UTC offset derived: UTC+%d (raw=%.4f)", _BROKER_UTC_OFFSET, raw_offset
    )
    return _BROKER_UTC_OFFSET


# ========== MODELS ==========


class Candle(BaseModel):
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    class Config:
        frozen = True


class CandleRequest(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    limit: Optional[int] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    timezone: str = Config.TIMEZONE

    @validator("timeframes")
    def validate_timeframes(cls, v):
        valid_tfs = {"1m", "5m", "15m", "30m", "1h", "4h", "d1", "w1", "mn1"}
        for tf in v:
            if tf.lower() not in valid_tfs:
                raise ValueError(f"Invalid timeframe: {tf}")
        return v


# ========== UTILITIES ==========


class TimeframeConverter:
    _MAP = {
        "1m": mt5.TIMEFRAME_M1,
        "5m": mt5.TIMEFRAME_M5,
        "15m": mt5.TIMEFRAME_M15,
        "30m": mt5.TIMEFRAME_M30,
        "1h": mt5.TIMEFRAME_H1,
        "4h": mt5.TIMEFRAME_H4,
        "d1": mt5.TIMEFRAME_D1,
        "w1": mt5.TIMEFRAME_W1,
        "mn1": mt5.TIMEFRAME_MN1,
    }

    @classmethod
    def to_mt5(cls, timeframe: str) -> int:
        key = timeframe.lower()
        if key not in cls._MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return cls._MAP[key]


# ========== SERVICE ==========

_DATE_FORMATS = [
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

_TZ_CACHE: dict[str, zoneinfo.ZoneInfo] = {}


def _get_tz(tz_name: str) -> zoneinfo.ZoneInfo:
    if tz_name not in _TZ_CACHE:
        _TZ_CACHE[tz_name] = zoneinfo.ZoneInfo(tz_name)
    return _TZ_CACHE[tz_name]


class CandleDataService:

    @staticmethod
    def broker_ts_to_utc_ms(server_timestamp: int) -> int:
        offset = get_broker_utc_offset()
        return (server_timestamp - offset * 3600) * 1000

    @staticmethod
    def parse_date_string(date_str: str, tz_name: str = Config.TIMEZONE) -> datetime:
        tz = _get_tz(tz_name)
        for fmt in _DATE_FORMATS:
            try:
                return (
                    datetime.strptime(date_str, fmt)
                    .replace(tzinfo=tz)
                    .astimezone(timezone.utc)
                )
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date string: {date_str!r}")

    @staticmethod
    def _build_candles(rates, offset: int) -> List[Candle]:
        has_real_volume = "real_volume" in rates.dtype.names
        candles = []
        for rate in rates:
            volume = rate["real_volume"] if has_real_volume else rate["tick_volume"]
            candles.append(
                Candle(
                    timestamp=(rate["time"] - offset * 3600) * 1000,
                    open=float(rate["open"]),
                    high=float(rate["high"]),
                    low=float(rate["low"]),
                    close=float(rate["close"]),
                    volume=float(volume),
                )
            )
        return candles

    @staticmethod
    def get_candles(
        symbol: str,
        timeframe: str,
        limit: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        tz_name: str = Config.TIMEZONE,
    ) -> List[Candle]:
        try:
            timeframe_enum = TimeframeConverter.to_mt5(timeframe)
            offset = get_broker_utc_offset()

            dt_from: Optional[datetime] = None
            dt_to: Optional[datetime] = None

            if from_date:
                dt_from = CandleDataService.parse_date_string(from_date, tz_name)
                dt_from += timedelta(hours=offset)
            if to_date:
                dt_to = CandleDataService.parse_date_string(to_date, tz_name)
                dt_to += timedelta(days=1)

            with _MT5_LOCK:
                if dt_from:
                    if not dt_to:
                        dt_to = datetime.now(timezone.utc) + timedelta(days=1)
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
                    status_code=404, detail=f"No data for {symbol} {timeframe}"
                )

            candles = CandleDataService._build_candles(rates, offset)
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

        def fetch(symbol: str, tf: str):
            try:
                return (
                    symbol,
                    tf,
                    CandleDataService.get_candles(
                        clean_symbol(symbol),
                        tf,
                        request.limit,
                        request.from_date,
                        request.to_date,
                        request.timezone,
                    ),
                )
            except HTTPException as e:
                logger.error(
                    "MT5 error for %s/%s: %s — mt5_error=%s",
                    symbol,
                    tf,
                    e.detail,
                    mt5.last_error(),
                )
                return symbol, tf, {"error": str(e.detail)}

        tasks = [(s, tf) for s in request.symbols for tf in request.timeframes]
        futures = {_THREAD_POOL.submit(fetch, s, tf): (s, tf) for s, tf in tasks}
        for future in as_completed(futures):
            symbol, tf, data = future.result()
            result.setdefault(symbol, {})[tf] = data

        return result
