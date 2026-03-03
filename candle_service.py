import logging
import MetaTrader5 as mt5
import pytz
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, validator
from fastapi import HTTPException
from configs import Config
from utils import clean_symbol
from datetime import timedelta

logger = logging.getLogger(__name__)

# ── Broker UTC offset — resolved lazily after MT5 login ──────────────────────
# MT5 rate["time"] is in broker server time (e.g. UTC+3 for FBS).
# terminal_info().trade_server_timezone gives the offset so we can convert
# to true UTC without hardcoding. Resolution is deferred until the first
# candle fetch so that mt5.initialize() + mt5.login() have already run.

_BROKER_UTC_OFFSET: Optional[int] = None


def get_broker_utc_offset() -> int:
    """
    Derive broker UTC offset by comparing MT5 server time against true UTC.
    Works regardless of MT5 build or broker — no reliance on terminal_info fields.
    """
    global _BROKER_UTC_OFFSET
    if _BROKER_UTC_OFFSET is not None:
        return _BROKER_UTC_OFFSET

    server_time = mt5.symbol_info_tick("EURUSD")
    if server_time is None:
        # Fallback: try any available symbol
        symbols = mt5.symbols_get()
        for s in symbols or []:
            tick = mt5.symbol_info_tick(s.name)
            if tick:
                server_time = tick
                break

    if server_time is None:
        logger.warning("Could not get tick time — assuming broker UTC+0")
        return 0

    # tick.time is broker server time (Unix seconds)
    # datetime.now(UTC) is true UTC
    true_utc_now = int(datetime.now(pytz.UTC).timestamp())
    broker_now = server_time.time

    # Round to nearest hour
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
    timestamp: str
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

    @validator("from_date", "to_date", pre=True, always=True)
    def validate_dates(cls, v, values):
        if "limit" in values and values["limit"] is not None and v is not None:
            logger.warning("Both limit and date range provided — using date range.")
        if v is None and ("limit" not in values or values["limit"] is None):
            values["limit"] = 100
        return v


# ========== UTILITIES ==========


class TimeframeConverter:
    @staticmethod
    def to_mt5(timeframe: str) -> int:
        """Convert trader-friendly timeframe string to MT5 enum."""
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
    def broker_ts_to_utc(server_timestamp: int) -> datetime:
        """
        Convert a raw MT5 rate["time"] value to a true UTC datetime.

        MT5 returns timestamps in broker server time (e.g. UTC+3 for FBS).
        We subtract the broker's UTC offset — resolved lazily via
        terminal_info().trade_server_timezone — to get true UTC.
        """
        offset = get_broker_utc_offset()
        true_utc = server_timestamp - (offset * 3600)
        return datetime.utcfromtimestamp(true_utc).replace(tzinfo=pytz.UTC)

    @staticmethod
    def convert_to_timezone(server_timestamp: int, tz: str = Config.TIMEZONE()) -> str:
        """
        Convert a raw MT5 broker timestamp to a formatted string in the
        requested timezone.

        Args:
            server_timestamp: raw MT5 rate["time"] (broker server time)
            tz: target timezone string e.g. "Africa/Lagos"

        Returns:
            Formatted datetime string: "YYYY-MM-DD HH:MM:SS AM/PM"
        """
        try:
            tz_obj = pytz.timezone(tz)
        except pytz.exceptions.UnknownTimeZoneError as e:
            raise ValueError(
                f"Invalid timezone: '{tz}'. Use format like 'Africa/Lagos'"
            ) from e

        dt_utc = CandleDataService.broker_ts_to_utc(server_timestamp)
        local_dt = dt_utc.astimezone(tz_obj)
        return local_dt.strftime("%Y-%m-%d %I:%M:%S %p")

    @staticmethod
    def parse_date_string(date_str: str, timezone: str = Config.TIMEZONE()) -> datetime:
        """Parse a date string in various formats to a timezone-aware datetime."""
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
        """Fetch candle data from MT5 with support for date ranges or limit."""
        try:
            timeframe_enum = TimeframeConverter.to_mt5(timeframe)

            dt_from: Optional[datetime] = None
            dt_to: Optional[datetime] = None

            if dt_from:
                dt_from = CandleDataService.parse_date_string(from_date, timezone)
                dt_from = dt_from.astimezone(pytz.UTC)
                # Shift to broker time — MT5 copy_rates_range expects broker server time
                dt_from = dt_from + timedelta(hours=get_broker_utc_offset())

            if to_date:
                dt_to = CandleDataService.parse_date_string(to_date, timezone)
                dt_to = dt_to.astimezone(pytz.UTC)
                dt_to = dt_to + timedelta(hours=get_broker_utc_offset())
            else:
                # No to_date — use current broker time
                dt_to = datetime.now(pytz.UTC) + timedelta(hours=get_broker_utc_offset())

            if dt_from and dt_to:
                rates = mt5.copy_rates_range(symbol, timeframe_enum, dt_from, dt_to)
            elif limit:
                rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, limit)
            else:
                rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, 100)

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
                timestamp = CandleDataService.convert_to_timezone(
                    rate["time"], timezone
                )
                candles.append(
                    Candle(
                        timestamp=timestamp,
                        open=float(rate["open"]),
                        high=float(rate["high"]),
                        low=float(rate["low"]),
                        close=float(rate["close"]),
                        volume=float(volume),
                    )
                )

            # Sort ascending (oldest → newest)
            candles.sort(
                key=lambda x: datetime.strptime(x.timestamp, "%Y-%m-%d %I:%M:%S %p")
            )

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
        """Fetch candle data for multiple symbols and timeframes."""
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
