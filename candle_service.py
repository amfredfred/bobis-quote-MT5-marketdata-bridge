import MetaTrader5 as mt5
import pytz
from datetime import datetime, timezone as dtz, timedelta
from typing import List, Optional
from pydantic import BaseModel, validator
from fastapi import HTTPException
from configs import Config
from utils import clean_symbol


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
        # If both limit and date range are provided, prioritize date range
        if "limit" in values and values["limit"] is not None and v is not None:
            print("Warning: Both limit and date range provided. Using date range.")

        # If no date range or limit is provided, default to last 100 candles
        if v is None and ("limit" not in values or values["limit"] is None):
            values["limit"] = 100

        return v


# ========== UTILITIES ==========
class TimeframeConverter:
    @staticmethod
    def to_mt5(timeframe: str) -> int:
        """Convert trader-friendly timeframe to MT5 enum"""
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
            return {1: mt5.TIMEFRAME_H1, 4: mt5.TIMEFRAME_H4}.get(
                hours, mt5.TIMEFRAME_H1
            )
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
    def convert_to_timezone(server_timestamp: int, tz: str = Config.TIMEZONE()) -> str:
        """Convert FBS broker timestamp (UTC+3) to specified local timezone.

        Args:
            server_timestamp: FBS server timestamp (UTC+3)
            tz: Target timezone (e.g., 'Africa/Lagos')

        Returns:
            Formatted datetime string in target timezone (12-hour with AM/PM)

        Raises:
            ValueError: If invalid timezone provided
        """
        try:
            tz_obj = pytz.timezone(tz)
        except pytz.exceptions.UnknownTimeZoneError as e:
            raise ValueError(
                f"Invalid timezone: '{tz}'. Use format like 'Africa/Lagos'"
            ) from e

        # Step 1: Convert FBS UTC+3 timestamp to true UTC
        true_utc_timestamp = server_timestamp - (3 * 3600)  # Subtract 3 hours

        # Step 2: Create timezone-aware UTC datetime
        dt_utc = datetime.utcfromtimestamp(true_utc_timestamp).replace(tzinfo=pytz.UTC)

        # Step 3: Convert to target timezone
        local_dt = dt_utc.astimezone(tz_obj)

        # Step 4: Format as 12-hour with AM/PM
        return local_dt.strftime("%Y-%m-%d %I:%M:%S %p")

    @staticmethod
    def parse_date_string(date_str: str, timezone: str = Config.TIMEZONE()) -> datetime:
        """Parse date string in various formats to datetime object"""
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
                # Localize the datetime to the specified timezone
                tz_obj = pytz.timezone(timezone)
                return tz_obj.localize(dt)
            except ValueError:
                continue

        raise ValueError(f"Unable to parse date string: {date_str}")

    @staticmethod
    def get_candles(
        symbol: str,
        timeframe: str,
        limit: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timezone: str = Config.TIMEZONE(),
    ) -> List[Candle]:
        """Get candle data from MT5 with support for date ranges"""
        try:
            timeframe_enum = TimeframeConverter.to_mt5(timeframe)

            # Convert date strings to datetime objects if provided
            dt_from = None
            dt_to = None

            if from_date:
                dt_from = CandleDataService.parse_date_string(from_date, timezone)
                # Convert to UTC for MT5
                dt_from = dt_from.astimezone(pytz.UTC)

            if to_date:
                dt_to = CandleDataService.parse_date_string(to_date, timezone)
                # Convert to UTC for MT5
                dt_to = dt_to.astimezone(pytz.UTC)
            else:
                # Default to current time if to_date not provided
                dt_to = datetime.now(pytz.UTC)

            # Fetch data based on parameters
            if dt_from and dt_to:
                # Get data between two dates
                rates = mt5.copy_rates_range(symbol, timeframe_enum, dt_from, dt_to)
            elif limit:
                # Get data by limit (number of candles)
                rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, limit)
            else:
                # Default to 100 candles if no parameters provided
                rates = mt5.copy_rates_from_pos(symbol, timeframe_enum, 0, 100)

            if rates is None or len(rates) == 0:
                raise HTTPException(
                    status_code=404, detail=f"No data for {symbol} {timeframe}"
                )

            candles = []
            for rate in rates:
                # Check which volume field exists
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
                        open=rate["open"],
                        high=rate["high"],
                        low=rate["low"],
                        close=rate["close"],
                        volume=volume,
                    )
                )

            # Sort candles by timestamp in descending order (newest first)
            candles.sort(
                key=lambda x: datetime.strptime(x.timestamp, "%Y-%m-%d %I:%M:%S %p"),
                reverse=True,
            )

            return candles
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching data for {symbol} {timeframe}: {str(e)}",
            )

    @staticmethod
    def get_multiple_timeframes(request: CandleRequest) -> dict:
        """Get candle data for multiple symbols and timeframes"""
        result = {}
        for symbol in request.symbols:
            symbol_data = {}
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
                    print(mt5.last_error())
                    symbol_data[tf] = {"error": str(e.detail)}
            result[symbol] = symbol_data
        return result
