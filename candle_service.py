import logging
import threading
import MetaTrader5 as mt5
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from pydantic import BaseModel, validator
from fastapi import HTTPException
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

_BROKER_UTC_OFFSET: Optional[int] = None
_OFFSET_SYMBOLS = ["BTCUSD", "ETHUSD", "BTCUSDT", "ETHUSDT", "EURUSD"]

_MT5_LOCK = threading.Lock()
_THREAD_POOL = ThreadPoolExecutor(max_workers=20)

_SYMBOL_CACHE: dict[str, str] = {}
_ALL_SYMBOLS: list[str] = []


# =========================
# SYMBOL PRELOAD
# =========================


def preload_symbols():
    global _ALL_SYMBOLS

    with _MT5_LOCK:
        symbols = mt5.symbols_get()

        if not symbols:
            logger.warning("No symbols returned from MT5")
            return

        # Force select all symbols (makes hidden ones available)
        for s in symbols:
            mt5.symbol_select(s.name, True)

        # Reload after selection
        symbols = mt5.symbols_get()

        _ALL_SYMBOLS = [s.name for s in symbols]

    logger.info(f"Preloaded {len(_ALL_SYMBOLS)} symbols")


# =========================
# SYMBOL RESOLVER
# =========================


def resolve_broker_symbol(engine_symbol: str) -> str:
    clean = engine_symbol.replace("/", "").replace("_", "").upper()

    # Cache hit
    if clean in _SYMBOL_CACHE:
        return _SYMBOL_CACHE[clean]

    matches = []

    for name in _ALL_SYMBOLS:
        upper_name = name.upper()

        # Exact match
        if upper_name == clean:
            _SYMBOL_CACHE[clean] = name
            return name

        # Prefix OR suffix match
        if upper_name.startswith(clean) or upper_name.endswith(clean):
            matches.append(name)

    if len(matches) == 1:
        resolved = matches[0]
    elif len(matches) > 1:
        resolved = sorted(matches, key=len)[0]
    else:
        resolved = clean

    _SYMBOL_CACHE[clean] = resolved
    return resolved


# =========================
# UTC OFFSET
# =========================


def get_broker_utc_offset() -> int:
    global _BROKER_UTC_OFFSET

    if _BROKER_UTC_OFFSET is not None:
        return _BROKER_UTC_OFFSET

    tick = None

    for symbol in _OFFSET_SYMBOLS:
        resolved = resolve_broker_symbol(symbol)

        with _MT5_LOCK:
            mt5.symbol_select(resolved, True)
            t = mt5.symbol_info_tick(resolved)

        if t is not None:
            tick = t
            break

    if tick is None:
        logger.warning("No tick available — assuming UTC+0")
        _BROKER_UTC_OFFSET = 0
        return 0

    true_utc_now = datetime.now(timezone.utc).timestamp()
    broker_ts = tick.time_msc / 1000.0 if tick.time_msc else float(tick.time)

    raw_offset = (broker_ts - true_utc_now) / 3600
    _BROKER_UTC_OFFSET = round(raw_offset)

    logger.info(
        "Broker UTC offset: UTC+%d (raw=%.4f)",
        _BROKER_UTC_OFFSET,
        raw_offset,
    )

    return _BROKER_UTC_OFFSET


# =========================
# MODELS
# =========================


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

    @validator("timeframes")
    def validate_timeframes(cls, v):
        valid_tfs = {"1m", "5m", "15m", "30m", "1h", "4h", "d1", "w1", "mn1"}
        for tf in v:
            if tf.lower() not in valid_tfs:
                raise ValueError(f"Invalid timeframe: {tf}")
        return v


# =========================
# TIMEFRAME CONVERTER
# =========================


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


# =========================
# DATE PARSER
# =========================

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


def _parse_date_utc(date_str: str) -> datetime:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date string: {date_str!r}")


def _utc_to_broker_naive(dt_utc: datetime, offset: int) -> datetime:
    """
    Convert a UTC-aware datetime to a naive datetime in broker local time.

    MT5's copy_rates_range() expects naive datetimes in broker local time.
    Passing a tz-aware datetime causes MT5 to treat it as UTC, which with
    our offset conversion results in a double-shift (fetching offset hours
    too late). Stripping tzinfo after the conversion prevents this.
    """
    return (dt_utc + timedelta(hours=offset)).replace(tzinfo=None)


# =========================
# SERVICE
# =========================


class CandleDataService:

    @staticmethod
    def broker_ts_to_utc_ms(server_timestamp: int) -> int:
        offset = get_broker_utc_offset()
        return (server_timestamp - offset * 3600) * 1000

    @staticmethod
    def _build_candles(rates, offset: int) -> List[Candle]:
        """
        Convert MT5 rate array to Candle list.

        MT5 copy_rates_* returns rate["time"] as a Unix timestamp encoded in
        broker local time (i.e. the broker-local wall-clock seconds since epoch,
        NOT true UTC seconds). Subtracting the broker UTC offset converts it to
        a true UTC Unix timestamp, which is then stored as milliseconds.
        """
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
    ) -> List[Candle]:
        try:
            timeframe_enum = TimeframeConverter.to_mt5(timeframe)
            offset = get_broker_utc_offset()

            dt_from: Optional[datetime] = None
            dt_to: Optional[datetime] = None

            if from_date:
                # Parse as UTC then convert to naive broker local for MT5.
                # Must be naive — MT5 treats tz-aware datetimes as UTC which
                # would double-apply the offset and shift the window forward.
                dt_from = _utc_to_broker_naive(_parse_date_utc(from_date), offset)

            if to_date:
                dt_to = _utc_to_broker_naive(_parse_date_utc(to_date), offset)

            with _MT5_LOCK:
                if not mt5.symbol_select(symbol, True):
                    code, desc = mt5.last_error()
                    raise HTTPException(
                        status_code=404,
                        detail=f"Symbol {symbol} not available. MT5 error: {code} - {desc}",
                    )

                if dt_from:
                    if not dt_to:
                        # Current broker local time, naive — no +24 padding needed.
                        dt_to = _utc_to_broker_naive(datetime.now(timezone.utc), offset)

                    rates = mt5.copy_rates_range(symbol, timeframe_enum, dt_from, dt_to)

                elif limit:
                    rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, limit)

                else:
                    raise HTTPException(
                        status_code=400,
                        detail="Provide from_date, to_date, or limit.",
                    )

            if rates is None or len(rates) == 0:
                code, desc = mt5.last_error()
                raise HTTPException(
                    status_code=404,
                    detail=f"No data for {symbol}. MT5 error: {code} - {desc}",
                )

            candles = CandleDataService._build_candles(rates, offset)
            candles.sort(key=lambda c: c.timestamp)

            return candles

        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"Error fetching {symbol} {timeframe}")
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching data for {symbol}: {e}",
            )

    @staticmethod
    def get_multiple_timeframes(request: CandleRequest) -> dict:
        result: dict = {}

        def fetch(symbol: str, tf: str):
            try:
                resolved = resolve_broker_symbol(symbol)

                return (
                    symbol,
                    tf,
                    CandleDataService.get_candles(
                        resolved,
                        tf,
                        request.limit,
                        request.from_date,
                        request.to_date,
                    ),
                )

            except HTTPException as e:
                code, desc = mt5.last_error()
                logger.error(
                    "MT5 error for %s/%s: %s (%s, %s)",
                    symbol,
                    tf,
                    e.detail,
                    code,
                    desc,
                )
                return symbol, tf, {"error": str(e.detail)}

        tasks = [(s, tf) for s in request.symbols for tf in request.timeframes]

        futures = {_THREAD_POOL.submit(fetch, s, tf): (s, tf) for s, tf in tasks}

        for future in as_completed(futures):
            symbol, tf, data = future.result()
            result.setdefault(symbol, {})[tf] = data

        return result
