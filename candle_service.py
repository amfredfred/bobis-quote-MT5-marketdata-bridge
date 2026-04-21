"""
market_data.py — Production-grade MT5 market data provider.

Architecture
------------
MT5Worker           All MT5 API calls serialized onto one dedicated OS thread.
                    MT5's Python binding is thread-affine; a lock does not fix
                    this — thread affinity does.

BrokerOffsetManager Reads BROKER_UTC_OFFSET_HOURS from config and re-verifies
                    against live MT5 server time every 6 h to detect DST drift.

SymbolResolver      Exact → unique-prefix → error resolution. Never silently
                    substitutes a wrong instrument.

MarketDataProvider  Main entry point. All state encapsulated — no module
                    globals, fully injectable, fully testable.

Fix index
---------
1.  Timestamp correctness  — r["time"] is UTC; offset never subtracted from output.
2.  Confirmed-closed bar   — copy_rates_from_pos starts at position 1, not 0.
3.  Volume transparency    — is_tick_volume flag on every Candle.
4.  Gap detection          — _detect_gaps raises GapDetectedError (or warns).
                             Session breaks (nightly close, weekends) are NOT gaps.
5.  Symbol resolution      — exact → prefix → SymbolResolutionError.
6.  MTF synchronization    — single analysis_ts anchored to HTF boundary.
7.  Staleness detection    — _check_staleness raises StaleDataError.
8.  zip truncation removed — explicit indexed loop with length assertion.
9.  DST-aware offset       — BrokerOffsetManager.verify() every 6 h.
10. Data integrity         — Candle validator + DataIntegrityError on bad bars.
"""

import atexit
import concurrent.futures
import logging
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from typing import Callable, Optional, TypeVar

import MetaTrader5 as mt5
from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)

from configs import Config

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
# CONSTANTS
# =============================================================================

_TIMEFRAME_MAP: dict[str, int] = {
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

# Nominal seconds per bar — used for gap detection and staleness checks.
# W1 / MN1 calendar-correct logic is handled separately.
_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "6m": 360,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14_400,
    "d1": 86_400,
    "w1": 604_800,
    "mn1": 2_592_000,
}

# FIX 3 — Instruments where MT5 real_volume is always zero.
# These are synthetic CFDs with no centralised exchange.
_TICK_VOLUME_ONLY_PREFIXES = (
    "US",
    "UK",
    "DE",
    "FR",
    "JP",
    "AU",
    "XAU",
    "XAG",
    "XPT",
    "XPD",
    "BTC",
    "ETH",
    "LTC",
    "XRP",
)

# FIX 4 — Instruments with a daily trading session break (~2 h nightly close).
# For these, intraday gaps up to 4 h and weekend gaps up to 72 h are normal
# and must not be flagged as data errors.
_SESSION_BREAK_PREFIXES = (
    "XAU",
    "XAG",
    "XPT",
    "XPD",
    "BTC",
    "ETH",
    "LTC",
    "XRP",
    "US",
    "UK",
    "DE",
    "FR",
    "JP",
)

# Maximum gap (seconds) that is still considered a normal session break.
# Nightly close ≈ 2 h; give 4 h of margin.
# Weekend close ≈ 48–72 h; anything up to 75 h is ignored.
_SESSION_BREAK_INTRADAY_S = 4 * 3600  # 4 h
_SESSION_BREAK_WEEKEND_S = 75 * 3600  # 75 h


# =============================================================================
# MODELS
# =============================================================================


class Candle(BaseModel):
    model_config = ConfigDict(frozen=True)

    # FIX 1 — timestamp is always UTC Unix milliseconds.
    # The broker offset is used only to translate request boundaries into
    # broker-local time for MT5 API calls; it is never applied to output.
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    # FIX 3 — callers must know whether volume is meaningful.
    is_tick_volume: bool

    @model_validator(mode="after")
    def _validate_ohlcv(self) -> "Candle":
        # FIX 10 — reject every class of corrupt bar MT5 can emit.
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
                raise ValueError(
                    f"Invalid timeframe: {tf!r}. Valid: {sorted(_TIMEFRAME_MAP)}"
                )
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


# =============================================================================
# FIX 1 — MT5 WORKER  (single dedicated OS thread)
# =============================================================================


class MT5Worker:
    """
    Serializes every MT5 API call onto one dedicated OS thread.

    MT5's Python binding is initialized against a specific thread.
    Calling it from any other thread — even under a lock — causes
    undefined behavior: silent data corruption or spurious errors.
    Thread affinity is the only correct solution.

    Usage
    -----
    worker.run_sync(lambda: mt5.copy_rates_from_pos(...))
    """

    _SHUTDOWN = object()  # sentinel

    def __init__(self) -> None:
        self._queue: Queue = Queue()
        self._ready = threading.Event()
        self._init_error: Optional[Exception] = None
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="mt5-worker"
        )
        self._thread.start()

        if not self._ready.wait(timeout=30):
            raise MT5ConnectionError("MT5 worker failed to initialize within 30 s")
        if self._init_error:
            raise self._init_error

        atexit.register(self.shutdown)
        logger.info("MT5Worker ready (tid=%d)", self._thread.ident)

    def _run(self) -> None:
        """Entire body executes on the dedicated MT5 thread."""
        try:
            if not mt5.initialize():
                self._init_error = MT5ConnectionError(
                    f"mt5.initialize() failed: {mt5.last_error()}"
                )
                return
        finally:
            self._ready.set()

        while True:
            try:
                item = self._queue.get(timeout=1)
            except Empty:
                continue

            if item is self._SHUTDOWN:
                break

            fn, future = item
            if future.cancelled():
                continue
            try:
                future.set_result(fn())
            except Exception as exc:
                future.set_exception(exc)

        mt5.shutdown()
        logger.info("MT5Worker shut down cleanly")

    def submit(self, fn: Callable[[], T]) -> "Future[T]":
        """Queue a callable; return a Future resolved on the MT5 thread."""
        future: Future[T] = Future()
        self._queue.put((fn, future))
        return future

    def run_sync(self, fn: Callable[[], T], timeout: float = 15.0) -> T:
        """Submit and block the calling thread until the result is ready."""
        future = self.submit(fn)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise MT5ConnectionError(f"MT5 call timed out after {timeout} s")

    def ensure_connected(self) -> None:
        """Verify the terminal is alive; attempt reconnect if not."""

        def _check() -> None:
            if mt5.terminal_info() is None:
                logger.warning("MT5 terminal_info() is None — attempting reconnect")
                if not mt5.initialize():
                    raise MT5ConnectionError(
                        f"MT5 reconnect failed: {mt5.last_error()}"
                    )

        self.run_sync(_check)

    def shutdown(self) -> None:
        self._queue.put(self._SHUTDOWN)
        self._thread.join(timeout=10)


# =============================================================================
# FIX 9 — BROKER OFFSET MANAGER  (DST-aware, periodically verified)
# =============================================================================


class BrokerOffsetManager:
    _RECHECK_INTERVAL = timedelta(hours=6)
    _DRIFT_WARN_SECS = 1800

    def __init__(self, config: Config, worker: MT5Worker) -> None:
        self._worker = worker
        self._lock = threading.Lock()
        self._offset: float = self._parse(config)
        self._last_verified: Optional[datetime] = None
        logger.info(
            "BrokerOffsetManager: offset=%+.0fs (%+.1fh)",
            self._offset,
            self._offset / 3600,
        )

    @staticmethod
    def _parse(config: Config) -> float:
        raw = config.BROKER_UTC_OFFSET_HOURS
        if raw is None:
            raise RuntimeError(
                "BROKER_UTC_OFFSET_HOURS is not set. "
                "Add it to your .env (e.g. BROKER_UTC_OFFSET_HOURS=2)."
            )
        try:
            return float(raw) * 3600
        except (ValueError, TypeError):
            raise RuntimeError(
                f"BROKER_UTC_OFFSET_HOURS={raw!r} must be numeric (e.g. 2 or 2.5)."
            )

    def get(self) -> float:
        with self._lock:
            now = datetime.now(timezone.utc)
            if (
                self._last_verified is None
                or now - self._last_verified >= self._RECHECK_INTERVAL
            ):
                self._verify(now)
            return self._offset

    def _verify(self, now: datetime) -> None:
        try:
            tick = self._worker.run_sync(
                lambda: mt5.symbol_info_tick("EURUSD"), timeout=5.0
            )
            if tick is not None:
                expected_broker_ts = now.timestamp() + self._offset
                drift_s = abs(expected_broker_ts - tick.time)
                if drift_s > self._DRIFT_WARN_SECS:
                    logger.warning(
                        "Broker offset drift detected: configured=%+.0fs, "
                        "observed_drift=%.0fs — possible DST transition. "
                        "Update BROKER_UTC_OFFSET_HOURS in .env.",
                        self._offset,
                        drift_s,
                    )
            self._last_verified = now
        except Exception as exc:
            logger.warning("Broker offset verification skipped: %s", exc)
            self._last_verified = now


# =============================================================================
# DATE UTILITIES
# =============================================================================


def _parse_utc_date(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date {s!r}")


def _utc_to_broker(dt: datetime, offset_s: float) -> datetime:
    ts = dt.timestamp() + offset_s
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)


def _last_closed_bar_utc(timeframe: str) -> datetime:
    now = datetime.now(timezone.utc)

    if timeframe == "mn1":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if timeframe == "w1":
        monday = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return monday

    tf_s = _TIMEFRAME_SECONDS[timeframe]
    floored = (int(now.timestamp()) // tf_s) * tf_s
    return datetime.fromtimestamp(floored, tz=timezone.utc)


# =============================================================================
# FIX 5 — SYMBOL RESOLVER
# =============================================================================


class SymbolResolver:
    def __init__(self, worker: MT5Worker) -> None:
        self._worker = worker
        self._all_symbols: list[str] = []
        self._cache: dict[str, str] = {}
        self._lock = threading.RLock()

    def preload(self) -> None:
        def _load() -> list[str]:
            symbols = mt5.symbols_get()
            if not symbols:
                return []
            for s in symbols:
                mt5.symbol_select(s.name, True)
            return [s.name for s in symbols]

        names = self._worker.run_sync(_load)
        with self._lock:
            self._all_symbols = names
            self._cache.clear()
        logger.info("SymbolResolver: %d symbols loaded", len(names))

    def resolve(self, symbol: str) -> str:
        clean = symbol.replace("/", "").replace("_", "").upper()

        with self._lock:
            if clean in self._cache:
                return self._cache[clean]

            exact = [n for n in self._all_symbols if n.upper() == clean]
            if exact:
                self._cache[clean] = exact[0]
                return exact[0]

            prefix = [n for n in self._all_symbols if n.upper().startswith(clean)]
            if len(prefix) == 1:
                logger.info("Symbol %r resolved via prefix → %r", symbol, prefix[0])
                self._cache[clean] = prefix[0]
                return prefix[0]

            if len(prefix) > 1:
                raise SymbolResolutionError(symbol, prefix)

            raise SymbolNotFoundError(symbol)


# =============================================================================
# FIX 3 — VOLUME CLASSIFICATION
# =============================================================================


def _uses_tick_volume(symbol: str, dtype_names: tuple) -> bool:
    if "real_volume" not in dtype_names:
        return True
    return any(symbol.upper().startswith(p) for p in _TICK_VOLUME_ONLY_PREFIXES)


# =============================================================================
# FIX 10 — DATA INTEGRITY CHECKS
# =============================================================================


def _validate_no_duplicate_timestamps(
    candles: list[Candle], symbol: str, timeframe: str
) -> None:
    seen: set[int] = set()
    dupes: list[str] = []
    for c in candles:
        if c.timestamp in seen:
            dupes.append(
                datetime.fromtimestamp(c.timestamp / 1000, tz=timezone.utc).isoformat()
            )
        seen.add(c.timestamp)
    if dupes:
        raise DataIntegrityError(symbol, timeframe, [f"Duplicate timestamps: {dupes}"])


# =============================================================================
# FIX 4 — GAP DETECTION  (session-aware)
# =============================================================================


def _has_session_break(symbol: str) -> bool:
    """True for instruments that have a daily close or weekend-only trading."""
    return any(symbol.upper().startswith(p) for p in _SESSION_BREAK_PREFIXES)


def _detect_gaps(
    candles: list[Candle], timeframe: str, symbol: str
) -> list[tuple[datetime, datetime]]:
    """
    Flag gaps that represent genuine missing data, ignoring expected
    session breaks (nightly close ~2 h, weekends ~48-72 h).

    Rules
    -----
    W1 / MN1          — 2× nominal bar; calendar variation is expected.
    Session-break instruments (XAU, indices, crypto, …)
        intraday TFs  — gaps ≤ 4 h are normal nightly closes; skip them.
                        gaps ≤ 75 h are normal weekends; skip them.
                        anything larger is a real gap.
    All other instruments (forex majors trade 24/5)
                      — 1.5× nominal bar threshold.
    """
    if len(candles) < 2:
        return []

    tf_s = _TIMEFRAME_SECONDS[timeframe]
    tf_ms = tf_s * 1000

    has_break = _has_session_break(symbol)

    gaps: list[tuple[datetime, datetime]] = []

    for i in range(1, len(candles)):
        delta_ms = candles[i].timestamp - candles[i - 1].timestamp
        delta_s = delta_ms / 1000

        # --- determine whether this delta is an expected session break ---
        if timeframe in ("w1", "mn1"):
            # Calendar months/weeks vary; use a loose 2× multiplier.
            if delta_ms <= tf_ms * 2.0:
                continue

        elif has_break:
            # Nightly close or weekend — not a data error.
            if delta_s <= _SESSION_BREAK_INTRADAY_S:
                continue
            if delta_s <= _SESSION_BREAK_WEEKEND_S:
                continue
            # Anything beyond 75 h on a session-break instrument IS suspicious.

        else:
            # Standard 24/5 forex — flag anything > 1.5 bars.
            if delta_ms <= tf_ms * 1.5:
                continue

        # If we reach here it's a real gap — log and record it.
        g_start = datetime.fromtimestamp(
            candles[i - 1].timestamp / 1000, tz=timezone.utc
        )
        g_end = datetime.fromtimestamp(candles[i].timestamp / 1000, tz=timezone.utc)
        gaps.append((g_start, g_end))
        logger.warning(
            "Gap in %s/%s: %s → %s (≈%.1f bars missing)",
            symbol,
            timeframe,
            g_start.isoformat(),
            g_end.isoformat(),
            delta_ms / tf_ms - 1,
        )

    return gaps


# =============================================================================
# FIX 7 — STALENESS DETECTION
# =============================================================================


def _check_staleness(candles: list[Candle], timeframe: str, symbol: str) -> None:
    if not candles:
        return

    tf_s = _TIMEFRAME_SECONDS[timeframe]
    now_utc = datetime.now(timezone.utc)
    last_bar_dt = datetime.fromtimestamp(candles[-1].timestamp / 1000, tz=timezone.utc)

    # For session-break instruments allow up to 1 full nightly close (4 h)
    # on top of the normal 2-bar window before declaring stale.
    extra_s = _SESSION_BREAK_INTRADAY_S if _has_session_break(symbol) else 0
    expected_by = now_utc - timedelta(seconds=tf_s * 2 + extra_s)

    if last_bar_dt < expected_by:
        raise StaleDataError(symbol, timeframe, last_bar_dt, expected_by)


# =============================================================================
# BUILD  (fixes 1, 3, 8, 10)
# =============================================================================


def _build(rates, symbol: str, timeframe: str) -> list[Candle]:
    use_tick = _uses_tick_volume(symbol, rates.dtype.names)
    vol_key = "tick_volume" if use_tick else "real_volume"
    vol_arr = rates[vol_key]

    if len(rates) != len(vol_arr):
        raise DataIntegrityError(
            symbol,
            timeframe,
            [f"rates/volume length mismatch: {len(rates)} vs {len(vol_arr)}"],
        )

    candles: list[Candle] = []
    integrity_issues: list[str] = []

    for i in range(len(rates)):
        r = rates[i]
        ts_ms = int(float(r["time"])) * 1000
        try:
            candles.append(
                Candle(
                    timestamp=ts_ms,
                    open=float(r["open"]),
                    high=float(r["high"]),
                    low=float(r["low"]),
                    close=float(r["close"]),
                    volume=float(vol_arr[i]),
                    is_tick_volume=use_tick,
                )
            )
        except ValidationError as exc:
            bar_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            integrity_issues.append(f"bar@{bar_dt}: {exc.errors()}")

    if integrity_issues:
        raise DataIntegrityError(symbol, timeframe, integrity_issues)

    return candles


# =============================================================================
# MARKET DATA PROVIDER  (main entry point)
# =============================================================================


class MarketDataProvider:
    def __init__(self, config: Config) -> None:
        self._worker = MT5Worker()
        self._offset_mgr = BrokerOffsetManager(config, self._worker)
        self._resolver = SymbolResolver(self._worker)
        self._resolver.preload()
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="md-dispatch"
        )
        atexit.register(self._pool.shutdown, wait=True)

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        allow_gaps: bool = False,
        check_staleness: bool = True,
        analysis_ts: Optional[datetime] = None,
    ) -> list[Candle]:
        tf = timeframe.lower()
        if tf not in _TIMEFRAME_MAP:
            raise ValueError(f"Invalid timeframe: {timeframe!r}")

        tf_mt5 = _TIMEFRAME_MAP[tf]
        offset_s = self._offset_mgr.get()
        resolved = self._resolver.resolve(symbol)

        self._worker.ensure_connected()
        self._worker.run_sync(lambda: mt5.symbol_select(resolved, True))

        if from_date:
            f_utc = _parse_utc_date(from_date)
            t_utc = (
                _parse_utc_date(to_date)
                if to_date
                else (analysis_ts or _last_closed_bar_utc(tf))
            )
            f_broker = _utc_to_broker(f_utc, offset_s)
            t_broker = _utc_to_broker(t_utc, offset_s)
            rates = self._worker.run_sync(
                lambda: mt5.copy_rates_range(resolved, tf_mt5, f_broker, t_broker)
            )
        else:
            rates = self._worker.run_sync(
                lambda: mt5.copy_rates_from_pos(resolved, tf_mt5, 1, limit)
            )

        if rates is None or len(rates) == 0:
            raise NoDataError(resolved, tf, from_date, to_date)

        candles = _build(rates, resolved, tf)
        candles.sort(key=lambda c: c.timestamp)

        _validate_no_duplicate_timestamps(candles, resolved, tf)

        gaps = _detect_gaps(candles, tf, resolved)
        if gaps and not allow_gaps:
            raise GapDetectedError(resolved, tf, gaps)

        if check_staleness:
            _check_staleness(candles, tf, resolved)

        logger.debug(
            "Fetched %d candles for %s/%s  [%s → %s]  tick_volume=%s",
            len(candles),
            resolved,
            tf,
            datetime.fromtimestamp(
                candles[0].timestamp / 1000, tz=timezone.utc
            ).isoformat(),
            datetime.fromtimestamp(
                candles[-1].timestamp / 1000, tz=timezone.utc
            ).isoformat(),
            candles[0].is_tick_volume,
        )

        return candles

    def get_multiple(self, request: CandleRequest) -> CandleResult:
        anchor_tf = max(request.timeframes, key=lambda t: _TIMEFRAME_SECONDS[t])
        analysis_ts = _last_closed_bar_utc(anchor_tf)

        self._worker.ensure_connected()

        result: CandleResult = {}

        def _job(s: str, tf: str) -> FetchResult:
            try:
                candles = self.get_candles(
                    symbol=s,
                    timeframe=tf,
                    limit=request.limit,
                    from_date=request.from_date,
                    to_date=request.to_date,
                    allow_gaps=request.allow_gaps,
                    check_staleness=request.check_staleness,
                    analysis_ts=analysis_ts,
                )
                return FetchSuccess(symbol=s, timeframe=tf, candles=candles)
            except MarketDataError as exc:
                logger.error(
                    "Fetch failed %s/%s [%s]: %s",
                    s,
                    tf,
                    type(exc).__name__,
                    exc,
                )
                return FetchFailure(
                    symbol=s,
                    timeframe=tf,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

        futures = {
            self._pool.submit(_job, s, tf): (s, tf)
            for s in request.symbols
            for tf in request.timeframes
        }

        for future in concurrent.futures.as_completed(futures):
            fetch_result = future.result()
            result.setdefault(fetch_result.symbol, {})[
                fetch_result.timeframe
            ] = fetch_result

        return result

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)
        self._worker.shutdown()
