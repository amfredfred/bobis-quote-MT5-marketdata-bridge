"""Data models and exceptions for MT5 market data provider."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
    model_validator,
)

from .constants import _TIMEFRAME_MAP


# =============================================================================
# EXCEPTIONS
# =============================================================================


class MarketDataError(Exception):
    """Base for all market data errors."""


class MT5ConnectionError(MarketDataError):
    """Terminal is not connected or a reconnect attempt failed."""


class SymbolNotFoundError(MarketDataError):
    def __init__(self, symbol: str) -> None:
        super().__init__(f"Symbol not found or unavailable: {symbol!r}")
        self.symbol = symbol


class SymbolResolutionError(MarketDataError):
    def __init__(self, symbol: str, candidates: list[str]) -> None:
        super().__init__(
            f"Ambiguous symbol {symbol!r} — multiple matches: {candidates}. "
            "Use the broker's exact name."
        )
        self.symbol = symbol
        self.candidates = candidates


class NoDataError(MarketDataError):
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> None:
        super().__init__(f"No data for {symbol}/{timeframe} [{from_date} → {to_date}]")
        self.symbol = symbol
        self.timeframe = timeframe


class StaleDataError(MarketDataError):
    """The most recent bar is too far behind wall-clock time — feed is frozen."""

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        last_bar: datetime,
        expected_by: datetime,
    ) -> None:
        super().__init__(
            f"Stale feed for {symbol}/{timeframe}: "
            f"last_bar={last_bar.isoformat()}, expected_by={expected_by.isoformat()}"
        )
        self.symbol = symbol
        self.timeframe = timeframe
        self.last_bar = last_bar
        self.expected_by = expected_by


class GapDetectedError(MarketDataError):
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        gaps: list[tuple[datetime, datetime]],
    ) -> None:
        super().__init__(f"Gaps in {symbol}/{timeframe}: {gaps}")
        self.symbol = symbol
        self.timeframe = timeframe
        self.gaps = gaps


class DataIntegrityError(MarketDataError):
    def __init__(self, symbol: str, timeframe: str, issues: list[str]) -> None:
        super().__init__(
            f"Integrity violations in {symbol}/{timeframe} ({len(issues)} bars): {issues[:5]}"
        )
        self.symbol = symbol
        self.timeframe = timeframe
        self.issues = issues


# =============================================================================
# RESULT TYPES  (typed union — no mixed dict/list ambiguity)
# =============================================================================


@dataclass(frozen=True)
class FetchSuccess:
    symbol: str
    timeframe: str
    candles: list  # list[Candle]


@dataclass(frozen=True)
class FetchFailure:
    symbol: str
    timeframe: str
    error: str
    error_type: str


FetchResult = FetchSuccess | FetchFailure


# =============================================================================
# MODELS
# =============================================================================


class Candle(BaseModel):
    model_config = ConfigDict(frozen=True)

    # timestamp is always UTC Unix milliseconds.
    # The broker offset is subtracted from r["time"] inside _build() so
    # that broker-local bar times are normalised to UTC before storage.
    # It is also added when converting request boundaries to broker-local
    # time for MT5 API calls (copy_rates_range).
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    # callers must know whether volume is meaningful.
    is_tick_volume: bool

    @model_validator(mode="after")
    def _validate_ohlcv(self) -> "Candle":
        # Reject every class of corrupt bar MT5 can emit.
        issues: list[str] = []
        if self.open <= 0:
            issues.append(f"open={self.open} <= 0")
        if self.high <= 0:
            issues.append(f"high={self.high} <= 0")
        if self.low <= 0:
            issues.append(f"low={self.low} <= 0")
        if self.close <= 0:
            issues.append(f"close={self.close} <= 0")
        if self.high < self.low:
            issues.append(f"high({self.high}) < low({self.low})")
        if self.high < self.open:
            issues.append(f"high({self.high}) < open({self.open})")
        if self.high < self.close:
            issues.append(f"high({self.high}) < close({self.close})")
        if self.low > self.open:
            issues.append(f"low({self.low}) > open({self.open})")
        if self.low > self.close:
            issues.append(f"low({self.low}) > close({self.close})")
        if self.volume < 0:
            issues.append(f"volume={self.volume} < 0")
        if issues:
            raise ValueError(f"OHLCV integrity failures: {issues}")
        return self


class CandleRequest(BaseModel):
    symbols: list[str]
    timeframes: list[str]
    limit: Optional[int] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    allow_gaps: bool = False
    check_staleness: bool = True

    @field_validator("timeframes")
    @classmethod
    def _validate_timeframes(cls, v: list[str]) -> list[str]:
        out = []
        for tf in v:
            k = tf.lower()
            if k not in _TIMEFRAME_MAP:
                raise ValueError(f"Invalid timeframe: {tf!r}. Valid: {sorted(_TIMEFRAME_MAP)}")
            out.append(k)
        return out

    @model_validator(mode="after")
    def _validate_date_limit(self) -> "CandleRequest":
        if self.from_date and self.limit:
            raise ValueError("Provide from_date OR limit, not both")
        if not self.from_date and not self.limit:
            raise ValueError("Provide from_date or limit")
        return self


CandleResult = dict[str, dict[str, FetchResult]]
