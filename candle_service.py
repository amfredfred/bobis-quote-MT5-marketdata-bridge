import atexit
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import MetaTrader5 as mt5
import numpy as np
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

logger = logging.getLogger(__name__)

# =========================
# GLOBALS
# =========================

_MT5_LOCK = threading.Lock()
_OFFSET_LOCK = threading.Lock()
_CACHE_LOCK = threading.Lock()
_SYMBOLS_LOCK = threading.RLock()  # dedicated lock for _ALL_SYMBOLS

_THREAD_POOL = ThreadPoolExecutor(max_workers=20)
atexit.register(_THREAD_POOL.shutdown, wait=True)  # FIX: wait=True to avoid partial writes

_SYMBOL_CACHE: dict[str, str] = {}
_ALL_SYMBOLS: list[str] = []

_BROKER_OFFSET_SECONDS: Optional[float] = None
_OFFSET_LAST_SYNC: Optional[float] = None
_OFFSET_REFRESH_INTERVAL = 3600  # 1 hour

_TIMEFRAME_MAP = {
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

_MAX_BATCH = 5000

# =========================
# MODELS
# =========================

class Candle(BaseModel):
    # Pydantic v2 API
    model_config = ConfigDict(frozen=True)

    timestamp: int  # UTC milliseconds
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
    def validate_timeframes(cls, v: list[str]) -> list[str]:
        normalised = []
        for tf in v:
            key = tf.lower()
            if key not in _TIMEFRAME_MAP:
                raise ValueError(f"Invalid timeframe: {tf!r}. Valid values: {sorted(_TIMEFRAME_MAP)}")
            normalised.append(key)  # FIX: return normalised list
        return normalised

    @model_validator(mode="after")
    def validate_date_limit_exclusivity(self) -> "CandleRequest":
        """Exactly one of from_date or limit must be provided."""
        if self.from_date and self.limit:
            raise ValueError(
                "Provide either 'from_date' or 'limit', not both. "
                "'from_date' defines a date range; 'limit' fetches the N most recent bars."
            )
        if not self.from_date and not self.limit:
            raise ValueError("Provide either 'from_date' or 'limit'.")
        return self


# Type alias for the multi-symbol response
CandleResult = dict[str, dict[str, list[Candle] | dict[str, str]]]


# =========================
# SYMBOLS
# =========================

def preload_symbols() -> None:
    global _ALL_SYMBOLS

    with _MT5_LOCK:
        # FIX: fetch once, reuse the result
        symbols = mt5.symbols_get()
        if not symbols:
            logger.warning("No symbols returned from MT5")
            return

        for s in symbols:
            mt5.symbol_select(s.name, True)

        names = [s.name for s in symbols]

    # FIX: write to _ALL_SYMBOLS under its own dedicated lock
    with _SYMBOLS_LOCK:
        _ALL_SYMBOLS = names

    logger.info("Loaded %d symbols", len(names))


def resolve_broker_symbol(symbol: str) -> str:
    clean = symbol.replace("/", "").replace("_", "").upper()

    with _CACHE_LOCK:
        if clean in _SYMBOL_CACHE:
            return _SYMBOL_CACHE[clean]

    # FIX: read _ALL_SYMBOLS under its lock
    with _SYMBOLS_LOCK:
        all_symbols = list(_ALL_SYMBOLS)

    matches = [
        n for n in all_symbols
        if n.upper() == clean
        or n.upper().startswith(clean)
        or n.upper().endswith(clean)
    ]

    if not matches:
        resolved = clean
    elif len(matches) == 1:
        resolved = matches[0]
    else:
        exact = [n for n in matches if n.upper() == clean]
        resolved = exact[0] if exact else sorted(matches, key=len)[0]

    with _CACHE_LOCK:
        _SYMBOL_CACHE[clean] = resolved

    return resolved


# =========================
# OFFSET
# =========================

def get_broker_offset_seconds(symbol: str = "EURUSD") -> float:
    """
    Detects the broker's UTC offset by comparing the last tick timestamp
    to the current UTC wall clock. Result is cached and refreshed hourly
    to handle DST transitions automatically.
    """
    global _BROKER_OFFSET_SECONDS, _OFFSET_LAST_SYNC

    now = time.monotonic()

    # Fast path — read outside lock (intentional: stale reads are safe here,
    # double-check inside the lock before recomputing).
    if (
        _BROKER_OFFSET_SECONDS is not None
        and _OFFSET_LAST_SYNC is not None
        and now - _OFFSET_LAST_SYNC < _OFFSET_REFRESH_INTERVAL
    ):
        return _BROKER_OFFSET_SECONDS

    # FIX: re-validate inside the lock to prevent redundant recomputation
    with _OFFSET_LOCK:
        if (
            _BROKER_OFFSET_SECONDS is not None
            and _OFFSET_LAST_SYNC is not None
            and now - _OFFSET_LAST_SYNC < _OFFSET_REFRESH_INTERVAL
        ):
            return _BROKER_OFFSET_SECONDS

        for attempt in range(5):
            with _MT5_LOCK:
                tick = mt5.symbol_info_tick(symbol)

            if tick and tick.time:
                broker_ts = float(tick.time)
                utc_ts = datetime.now(timezone.utc).timestamp()
                offset = broker_ts - utc_ts

                if -12 * 3600 <= offset <= 14 * 3600:
                    _BROKER_OFFSET_SECONDS = offset
                    _OFFSET_LAST_SYNC = now
                    logger.info("Broker offset: %.2fs (attempt %d)", offset, attempt + 1)
                    return offset

            time.sleep(0.2)

        logger.warning("Broker offset detection failed after 5 attempts — falling back to 0")
        _BROKER_OFFSET_SECONDS = 0.0
        _OFFSET_LAST_SYNC = now
        return 0.0


# =========================
# HELPERS
# =========================

def _parse_date(date_str: str) -> datetime:
    """
    Parse a date string in one of several accepted formats.
    The caller is responsible for attaching timezone info after parsing.
    Accepted formats treat input as UTC; no local-time interpretation is done.
    """
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",   # ISO 8601 with explicit Z
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid date: {date_str!r}. "
        "Expected formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, DD/MM/YYYY, etc."
    )


def _utc_dt_to_broker_naive(dt_utc: datetime, offset_seconds: float) -> datetime:
    """
    Convert a UTC-aware datetime to a broker-local naive datetime.

    MT5's copy_rates_range expects naive datetimes expressed in broker-local time.
    We achieve this by shifting the UTC epoch timestamp by the broker offset and
    then producing a naive datetime via utcfromtimestamp, which does NOT apply
    any local machine timezone — making this safe regardless of server locale.
    """
    # FIX: use utcfromtimestamp to avoid machine-local timezone contamination
    broker_epoch = dt_utc.timestamp() + offset_seconds
    return datetime.utcfromtimestamp(broker_epoch)


def _build_candles(rates: np.ndarray, offset_seconds: float) -> list[Candle]:
    """
    Convert MT5 rate records to Candle objects with UTC millisecond timestamps.

    MT5 stores bar open times in broker-local time. We reverse the offset to
    obtain UTC: utc_ts = broker_ts - offset.
    """
    vol = (
        rates["real_volume"]
        if "real_volume" in rates.dtype.names
        else rates["tick_volume"]
    )

    return [
        Candle(
            timestamp=int((float(r["time"]) - offset_seconds) * 1000),  # UTC ms
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=float(v),
        )
        for r, v in zip(rates, vol)
    ]


def _fetch_rates_batched(symbol: str, tf: int, limit: int) -> Optional[np.ndarray]:
    """
    Fetch up to `limit` bars in MAX_BATCH-sized pages.

    FIX: Acquires _MT5_LOCK per batch rather than holding it across the entire
    loop, keeping lock contention minimal for other threads.
    """
    batches: list[np.ndarray] = []
    fetched = 0

    while fetched < limit:
        size = min(_MAX_BATCH, limit - fetched)

        with _MT5_LOCK:  # FIX: lock per-batch, not across the whole loop
            batch = mt5.copy_rates_from_pos(symbol, tf, fetched, size)

        if batch is None or len(batch) == 0:
            break

        batches.append(batch)
        fetched += len(batch)

        if len(batch) < size:
            break  # MT5 returned fewer bars than requested — we're at the start of history

    if not batches:
        return None

    return np.concatenate(batches) if len(batches) > 1 else batches[0]


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
    """
    Fetch OHLCV candles for a single symbol/timeframe combination.

    Exactly one of `from_date` or `limit` must be provided.
    All returned timestamps are UTC milliseconds.
    """
    if from_date and limit:
        raise HTTPException(
            status_code=400,
            detail="Provide either 'from_date' or 'limit', not both.",
        )
    if not from_date and not limit:
        raise HTTPException(
            status_code=400,
            detail="Provide either 'from_date' or 'limit'.",
        )

    tf = _TIMEFRAME_MAP.get(timeframe.lower())
    if tf is None:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe!r}")

    offset = get_broker_offset_seconds(symbol)

    try:
        with _MT5_LOCK:
            if not mt5.symbol_select(symbol, True):
                code, desc = mt5.last_error()
                raise HTTPException(
                    status_code=404,
                    detail=f"Symbol {symbol!r} not available: [{code}] {desc}",
                )

        if from_date:
            # FIX: parse → attach UTC → convert to broker-local naive
            dt_from_utc = _parse_date(from_date).replace(tzinfo=timezone.utc)
            dt_to_utc = (
                _parse_date(to_date).replace(tzinfo=timezone.utc)
                if to_date
                else datetime.now(timezone.utc)
            )

            broker_from = _utc_dt_to_broker_naive(dt_from_utc, offset)
            broker_to = _utc_dt_to_broker_naive(dt_to_utc, offset)

            with _MT5_LOCK:
                rates = mt5.copy_rates_range(symbol, tf, broker_from, broker_to)
        else:
            rates = _fetch_rates_batched(symbol, tf, limit)

        if rates is None or len(rates) == 0:
            with _MT5_LOCK:
                code, desc = mt5.last_error()
            raise HTTPException(
                status_code=404,
                detail=f"No data returned for {symbol!r} [{timeframe}]: [{code}] {desc}",
            )

        candles = _build_candles(rates, offset)
        candles.sort(key=lambda c: c.timestamp)
        return candles

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error fetching candles for %s [%s]", symbol, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error fetching candles.")


def get_multiple(request: CandleRequest) -> CandleResult:
    """
    Fetch candles for multiple symbol/timeframe combinations concurrently.

    Returns a nested dict: { symbol: { timeframe: [Candle, ...] | {"error": str} } }
    Failures for individual pairs are captured and returned inline rather than
    raising, so a single bad symbol does not abort the entire batch.
    """
    result: CandleResult = {}

    def fetch(symbol: str, tf: str) -> tuple[str, str, list[Candle] | dict]:
        try:
            resolved = resolve_broker_symbol(symbol)
            data = get_candles(resolved, tf, request.limit, request.from_date, request.to_date)
            return symbol, tf, data
        except HTTPException as e:
            return symbol, tf, {"error": e.detail}
        except Exception as e:
            logger.exception("Unexpected error in concurrent fetch for %s [%s]", symbol, tf)
            return symbol, tf, {"error": "Internal error fetching candles."}

    futures = {
        _THREAD_POOL.submit(fetch, s, tf): (s, tf)
        for s in request.symbols
        for tf in request.timeframes
    }

    for future in as_completed(futures):
        # FIX: catch unexpected exceptions from f.result() itself
        try:
            symbol, tf, data = future.result()
        except Exception:
            symbol, tf = futures[future]
            logger.exception("Future raised unexpectedly for %s [%s]", symbol, tf)
            result.setdefault(symbol, {})[tf] = {"error": "Internal error."}
            continue

        result.setdefault(symbol, {})[tf] = data

    return result