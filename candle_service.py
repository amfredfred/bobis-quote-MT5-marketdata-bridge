import atexit
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import MetaTrader5 as mt5
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from configs import Config

logger = logging.getLogger(__name__)

# =========================
# GLOBALS
# =========================

_MT5_LOCK = threading.Lock()
_CACHE_LOCK = threading.RLock()
_SYMBOL_CACHE: dict[str, str] = {}
_ALL_SYMBOLS: list[str] = []

_THREAD_POOL = ThreadPoolExecutor(max_workers=3)
atexit.register(_THREAD_POOL.shutdown, wait=True)

# =========================
# BROKER OFFSET
# =========================

_BROKER_OFFSET_SECONDS: Optional[float] = None
_OFFSET_INITIALIZED = False
_OFFSET_INIT_LOCK = threading.Lock()


def _detect_broker_offset_once() -> float:
    global _BROKER_OFFSET_SECONDS, _OFFSET_INITIALIZED

    with _OFFSET_INIT_LOCK:
        if _OFFSET_INITIALIZED:
            return _BROKER_OFFSET_SECONDS or 0.0

        raw = Config.BROKER_UTC_OFFSET_HOURS
        if raw is None:
            raise RuntimeError(
                "BROKER_UTC_OFFSET_HOURS is not set. "
                "Add it to your .env (e.g. BROKER_UTC_OFFSET_HOURS=2 for FBS)."
            )

        try:
            offset = float(raw) * 3600
        except ValueError:
            raise RuntimeError(
                f"Invalid BROKER_UTC_OFFSET_HOURS={raw!r} — must be a number (e.g. 2 or 2.5)."
            )

        _BROKER_OFFSET_SECONDS = offset
        _OFFSET_INITIALIZED = True
        logger.info("✅ Broker offset: %+.0fs (%+.1fh)", offset, offset / 3600)
        return offset


def get_broker_offset() -> float:
    if not _OFFSET_INITIALIZED:
        return _detect_broker_offset_once()
    return _BROKER_OFFSET_SECONDS or 0.0


# =========================
# TIMEFRAMES
# =========================

_TIMEFRAME_MAP = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "6m": mt5.TIMEFRAME_M6,
    "10m": mt5.TIMEFRAME_M10,
    "15m": mt5.TIMEFRAME_M15,
    "30m": mt5.TIMEFRAME_M30,
    "1h": mt5.TIMEFRAME_H1,
    "4h": mt5.TIMEFRAME_H4,
    "d1": mt5.TIMEFRAME_D1,
    "w1": mt5.TIMEFRAME_W1,
    "mn1": mt5.TIMEFRAME_MN1,
}

_MAX_BATCH = 5000


# =========================
# MODELS
# =========================


class Candle(BaseModel):
    model_config = ConfigDict(frozen=True)
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class CandleRequest(BaseModel):
    symbols: list[str]
    timeframes: list[str]
    limit: Optional[int] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v):
        out = []
        for tf in v:
            k = tf.lower()
            if k not in _TIMEFRAME_MAP:
                raise ValueError(f"Invalid timeframe: {tf}")
            out.append(k)
        return out

    @model_validator(mode="after")
    def validate(self):
        if self.from_date and self.limit:
            raise ValueError("Provide from_date OR limit")
        if not self.from_date and not self.limit:
            raise ValueError("Provide from_date or limit")
        return self


CandleResult = dict[str, dict[str, list[Candle] | dict]]


# =========================
# SYMBOLS
# =========================


def preload_symbols():
    global _ALL_SYMBOLS

    with _MT5_LOCK:
        symbols = mt5.symbols_get()
        if not symbols:
            return
        for s in symbols:
            mt5.symbol_select(s.name, True)
        names = [s.name for s in symbols]

    with _CACHE_LOCK:
        _ALL_SYMBOLS = names


def resolve_broker_symbol(symbol: str) -> str:
    clean = symbol.replace("/", "").replace("_", "").upper()

    with _CACHE_LOCK:
        if clean in _SYMBOL_CACHE:
            return _SYMBOL_CACHE[clean]

    with _CACHE_LOCK:
        all_symbols = list(_ALL_SYMBOLS)

    matches = [n for n in all_symbols if clean in n.upper()]
    resolved = matches[0] if matches else clean

    with _CACHE_LOCK:
        _SYMBOL_CACHE[clean] = resolved

    return resolved


# =========================
# DATE
# =========================


def _parse_utc_date(s: str) -> datetime:
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for f in formats:
        try:
            return datetime.strptime(s, f).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {s}")


def _utc_to_broker(dt: datetime, offset: float) -> datetime:
    ts = dt.timestamp() + offset
    return datetime.utcfromtimestamp(ts)


# =========================
# BUILD
# =========================


def _build(rates, offset: float) -> list[Candle]:
    vol = (
        rates["real_volume"]
        if "real_volume" in rates.dtype.names
        else rates["tick_volume"]
    )

    return [
        Candle(
            timestamp=int((float(r["time"]) - offset) * 1000),
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=float(v),
        )
        for r, v in zip(rates, vol)
    ]


# =========================
# CORE
# =========================


def get_candles(
    symbol: str,
    timeframe: str,
    limit: Optional[int] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> list[Candle]:
    tf = _TIMEFRAME_MAP[timeframe.lower()]
    offset = get_broker_offset()

    with _MT5_LOCK:
        if not mt5.symbol_select(symbol, True):
            raise HTTPException(status_code=404, detail="Symbol unavailable")

    if from_date:
        f = _utc_to_broker(_parse_utc_date(from_date), offset)
        t = _utc_to_broker(
            _parse_utc_date(to_date) if to_date else datetime.now(timezone.utc),
            offset,
        )
        with _MT5_LOCK:
            rates = mt5.copy_rates_range(symbol, tf, f, t)
    else:
        with _MT5_LOCK:
            rates = mt5.copy_rates_from_pos(symbol, tf, 0, limit)

    if rates is None or len(rates) == 0:
        raise HTTPException(status_code=404, detail="No data")

    candles = _build(rates, offset)
    candles.sort(key=lambda x: x.timestamp)
    return candles


def get_multiple(request: CandleRequest) -> CandleResult:
    result: CandleResult = {}

    def job(s: str, tf: str):
        try:
            resolved = resolve_broker_symbol(s)
            data = get_candles(
                resolved, tf, request.limit, request.from_date, request.to_date
            )
            return s, tf, data
        except Exception as e:
            return s, tf, {"error": str(e)}

    futures = {
        _THREAD_POOL.submit(job, s, tf): (s, tf)
        for s in request.symbols
        for tf in request.timeframes
    }

    for future in as_completed(futures):
        s, tf, data = future.result()
        result.setdefault(s, {})[tf] = data

    return result
