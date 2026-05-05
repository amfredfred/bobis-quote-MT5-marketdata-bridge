"""
performance.py — TTL cache + SQLite persistence layer for MT5 candle data.

Flow: Request → TTL Cache → SQLite → MT5 → Store → Response

Rules:
- Always refresh latest candle from MT5
- SQLite is NOT source of truth (MT5 wins on conflicts)
- No cron, no background sync
"""

import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import concurrent.futures

from .configs import Config
from .constants import _TIMEFRAME_SECONDS
from .market_data import MarketDataProvider
from .models import (
    Candle,
    CandleRequest,
    CandleResult,
    FetchFailure,
    FetchResult,
    FetchSuccess,
    MarketDataError,
)

logger = logging.getLogger(__name__)

_TTL_SECONDS = 30.0   # was 1.5 s — align with the shortest meaningful bar cadence
_CACHE_MAXSIZE = 512  # max live entries; LRU eviction kicks in beyond this


# =============================================================================
# TTL CACHE
# =============================================================================


class _TTLEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: list[Candle], ttl: float) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl


class TTLCache:
    """Thread-safe TTL cache with per-key in-flight deduplication.

    When multiple threads request the same key simultaneously and the cache
    is cold, only the first thread fetches; the rest wait and share the result.
    This prevents duplicate MT5 round-trips for the same (symbol, timeframe).

    Size is bounded by maxsize: expired entries are swept on every set(); if
    still over capacity, the soonest-to-expire entry is evicted (LRU-ish).
    Without this bound the store grows without limit on services polling many
    symbol/timeframe combinations.
    """

    def __init__(self, ttl: float = _TTL_SECONDS, maxsize: int = _CACHE_MAXSIZE) -> None:
        self._ttl = ttl
        self._maxsize = maxsize
        self._store: dict[str, _TTLEntry] = {}
        self._inflight: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers  (must be called with self._lock held)
    # ------------------------------------------------------------------

    def _sweep_expired(self) -> None:
        """Remove all expired entries. Call with lock held."""
        now = time.monotonic()
        expired = [k for k, v in self._store.items() if now > v.expires_at]
        for k in expired:
            del self._store[k]

    def _evict_one(self) -> None:
        """Evict the entry nearest to expiry to make room. Call with lock held."""
        if self._store:
            victim = min(self._store, key=lambda k: self._store[k].expires_at)
            del self._store[victim]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[list[Candle]]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if time.monotonic() > entry.expires_at:
                del self._store[key]
                return None
            return entry.value

    def set(self, key: str, value: list[Candle]) -> None:
        with self._lock:
            # Always sweep first — removes expired entries for free.
            self._sweep_expired()
            # Enforce hard size cap: evict until we have room for the new entry.
            while len(self._store) >= self._maxsize and key not in self._store:
                self._evict_one()
            self._store[key] = _TTLEntry(value, self._ttl)
            # Wake any threads that were waiting on this key.
            event = self._inflight.pop(key, None)
        if event:
            event.set()

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def acquire_inflight(self, key: str) -> Optional[threading.Event]:
        """Register as the fetching thread for *key*.

        Returns None  → caller is the designated fetcher; it must call set().
        Returns Event → another thread is already fetching; wait on the event,
                        then call get() again to retrieve the shared result.
        """
        with self._lock:
            if key in self._inflight:
                return self._inflight[key]      # already in-flight — wait
            self._inflight[key] = threading.Event()
            return None                         # we are the fetcher

    def release_inflight(self, key: str) -> None:
        """Called by the fetching thread on error (set() already handles success)."""
        with self._lock:
            event = self._inflight.pop(key, None)
        if event:
            event.set()

    @staticmethod
    def make_key(
        symbol: str,
        timeframe: str,
        limit: Optional[int],
        from_date: Optional[str],
        to_date: Optional[str],
    ) -> str:
        if from_date:
            return f"{symbol}:{timeframe}:range:{from_date}:{to_date or ''}"
        return f"{symbol}:{timeframe}:{limit}"


# =============================================================================
# SQLITE STORE
# =============================================================================


class CandleStore:
    _DDL = """
    CREATE TABLE IF NOT EXISTS candles (
        symbol        TEXT,
        timeframe     TEXT,
        timestamp     INTEGER,
        open          REAL,
        high          REAL,
        low           REAL,
        close         REAL,
        volume        REAL,
        is_tick_volume INTEGER,
        PRIMARY KEY (symbol, timeframe, timestamp)
    ) WITHOUT ROWID;

    CREATE INDEX IF NOT EXISTS idx_candles_lookup
        ON candles (symbol, timeframe, timestamp);
    """

    def __init__(self, db_path: str = "candles.db") -> None:
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        if not getattr(self._local, "conn", None):
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-4000")   # 4 MB — was 32 MB per thread
            self._local.conn = conn
        return self._local.conn

    def close(self) -> None:
        """Close the connection on the calling thread (call from pool shutdown hook)."""
        conn = getattr(self._local, "conn", None)
        if conn:
            conn.close()
            self._local.conn = None

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(self._DDL)
        conn.commit()
        logger.info("CandleStore initialized: %s", self._db_path)

    # ------------------------------------------------------------------
    # Reads  (no lock needed — WAL mode handles concurrent readers)
    # ------------------------------------------------------------------

    def query_limit(self, symbol: str, timeframe: str, limit: int) -> list[Candle]:
        rows = (
            self._conn()
            .execute(
                """
            SELECT timestamp, open, high, low, close, volume, is_tick_volume
            FROM candles
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
                (symbol, timeframe, limit),
            )
            .fetchall()
        )
        return [self._row_to_candle(r) for r in reversed(rows)]

    def query_range(self, symbol: str, timeframe: str, from_ts: int, to_ts: int) -> list[Candle]:
        rows = (
            self._conn()
            .execute(
                """
            SELECT timestamp, open, high, low, close, volume, is_tick_volume
            FROM candles
            WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
            """,
                (symbol, timeframe, from_ts, to_ts),
            )
            .fetchall()
        )
        return [self._row_to_candle(r) for r in rows]

    def count(self, symbol: str, timeframe: str) -> int:
        row = (
            self._conn()
            .execute(
                "SELECT COUNT(*) FROM candles WHERE symbol = ? AND timeframe = ?",
                (symbol, timeframe),
            )
            .fetchone()
        )
        return row[0] if row else 0

    def newest_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        row = (
            self._conn()
            .execute(
                "SELECT MAX(timestamp) FROM candles WHERE symbol = ? AND timeframe = ?",
                (symbol, timeframe),
            )
            .fetchone()
        )
        return row[0] if row and row[0] is not None else None

    def oldest_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        row = (
            self._conn()
            .execute(
                "SELECT MIN(timestamp) FROM candles WHERE symbol = ? AND timeframe = ?",
                (symbol, timeframe),
            )
            .fetchone()
        )
        return row[0] if row and row[0] is not None else None

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert(self, symbol: str, timeframe: str, candles: list[Candle]) -> None:
        if not candles:
            return
        with self._write_lock:
            conn = self._conn()
            conn.executemany(
                """
                INSERT OR REPLACE INTO candles
                    (symbol, timeframe, timestamp, open, high, low, close, volume, is_tick_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        symbol,
                        timeframe,
                        c.timestamp,
                        c.open,
                        c.high,
                        c.low,
                        c.close,
                        c.volume,
                        int(c.is_tick_volume),
                    )
                    for c in candles
                ],
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_candle(row: tuple) -> Candle:
        return Candle(
            timestamp=row[0],
            open=row[1],
            high=row[2],
            low=row[3],
            close=row[4],
            volume=row[5],
            is_tick_volume=bool(row[6]),
        )


# =============================================================================
# MERGE HELPERS
# =============================================================================


def _merge(base: list[Candle], override: list[Candle]) -> list[Candle]:
    """Merge two candle lists. override wins on duplicate timestamps."""
    merged: dict[int, Candle] = {c.timestamp: c for c in base}
    merged.update({c.timestamp: c for c in override})
    return sorted(merged.values(), key=lambda c: c.timestamp)


# =============================================================================
# CACHED MARKET DATA PROVIDER
# =============================================================================


class CachedMarketDataProvider:
    """
    Drop-in replacement for MarketDataProvider with TTL cache + SQLite persistence.

    get_candles() and get_multiple() have the same signature and semantics.
    """

    _POOL_WORKERS = 8

    def __init__(self, config: Config, db_path: str = "candles.db") -> None:
        # One shared pool — injected into MarketDataProvider so there are no
        # orphaned thread pools sitting idle (Fix 1).
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._POOL_WORKERS, thread_name_prefix="cached-md"
        )
        self._provider = MarketDataProvider(config, pool=self._pool)
        self._store = CandleStore(db_path)
        self._cache = TTLCache(ttl=_TTL_SECONDS)

    # ------------------------------------------------------------------
    # Public API  (same as MarketDataProvider)
    # ------------------------------------------------------------------

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
        cache_key = TTLCache.make_key(symbol, tf, limit, from_date, to_date)

        # 1. TTL cache — fast path, no locking needed.
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug("TTL cache hit: %s", cache_key)
            return cached

        # 2. In-flight dedup — if another thread is already fetching this key,
        #    wait for it and return its result rather than firing a second MT5 call.
        wait_event = self._cache.acquire_inflight(cache_key)
        if wait_event is not None:
            logger.debug("In-flight wait: %s", cache_key)
            wait_event.wait(timeout=30.0)
            result = self._cache.get(cache_key)
            if result is not None:
                return result
            # Fetching thread errored — fall through and try ourselves.

        # 3. We are the designated fetcher for this key.
        try:
            if from_date:
                candles = self._fetch_range(
                    symbol, tf, from_date, to_date, allow_gaps, check_staleness, analysis_ts
                )
            else:
                candles = self._fetch_limit(
                    symbol, tf, limit, allow_gaps, check_staleness, analysis_ts
                )
            self._cache.set(cache_key, candles)   # also wakes any waiters
            return candles
        except Exception:
            self._cache.release_inflight(cache_key)  # wake waiters so they don't hang
            raise

    def get_multiple(self, request: CandleRequest) -> CandleResult:
        from .constants import _TIMEFRAME_SECONDS
        from .market_data import _last_closed_bar_utc

        anchor_tf = max(request.timeframes, key=lambda t: _TIMEFRAME_SECONDS[t])
        analysis_ts = _last_closed_bar_utc(anchor_tf)

        self._provider._worker.ensure_connected()

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
                logger.error("Fetch failed %s/%s [%s]: %s", s, tf, type(exc).__name__, exc)
                return FetchFailure(
                    symbol=s, timeframe=tf, error=str(exc), error_type=type(exc).__name__
                )

        futures = {
            self._pool.submit(_job, s, tf): (s, tf)
            for s in request.symbols
            for tf in request.timeframes
        }

        for future in concurrent.futures.as_completed(futures):
            fetch_result = future.result()
            result.setdefault(fetch_result.symbol, {})[fetch_result.timeframe] = fetch_result

        return result

    def shutdown(self) -> None:
        # Close each thread's SQLite connection from within the thread itself.
        # Submit one close() job per worker so every thread runs the cleanup
        # before the pool joins them.
        close_futures = [
            self._pool.submit(self._store.close)
            for _ in range(self._POOL_WORKERS)
        ]
        concurrent.futures.wait(close_futures)

        self._pool.shutdown(wait=True)
        self._provider.shutdown()   # shuts down MT5Worker; won't double-close the pool

    # ------------------------------------------------------------------
    # Internal: limit-based fetch
    # ------------------------------------------------------------------

    def _fetch_limit(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        allow_gaps: bool,
        check_staleness: bool,
        analysis_ts: Optional[datetime],
    ) -> list[Candle]:
        db_count = self._store.count(symbol, timeframe)

        if db_count >= limit:
            # SQLite has enough history — only refresh the latest candle from MT5.
            logger.debug(
                "SQLite hit (partial refresh): %s/%s count=%d limit=%d",
                symbol,
                timeframe,
                db_count,
                limit,
            )
            fresh = self._mt5_fetch(
                symbol, timeframe, 2, None, None, allow_gaps, check_staleness, analysis_ts
            )

            db_candles = self._store.query_limit(symbol, timeframe, limit)
            merged = _merge(db_candles, fresh)[-limit:]

            self._store.upsert(symbol, timeframe, fresh)
            logger.debug("Stored %d fresh candles for %s/%s", len(fresh), symbol, timeframe)
            return merged

        # SQLite doesn't have enough — full MT5 fetch, fill the store.
        logger.debug(
            "SQLite miss (full fetch): %s/%s count=%d limit=%d", symbol, timeframe, db_count, limit
        )
        mt5_candles = self._mt5_fetch(
            symbol, timeframe, limit, None, None, allow_gaps, check_staleness, analysis_ts
        )

        # Merge only what SQLite already has (no double-fetch).
        db_candles = self._store.query_limit(symbol, timeframe, limit)
        merged = _merge(db_candles, mt5_candles)

        self._store.upsert(symbol, timeframe, merged)
        return merged[-limit:]

    # ------------------------------------------------------------------
    # Internal: date-range fetch
    # ------------------------------------------------------------------

    def _fetch_range(
        self,
        symbol: str,
        timeframe: str,
        from_date: str,
        to_date: Optional[str],
        allow_gaps: bool,
        check_staleness: bool,
        analysis_ts: Optional[datetime],
    ) -> list[Candle]:
        from .market_data import _parse_utc_date, _last_closed_bar_utc

        from_dt = _parse_utc_date(from_date)
        to_dt = (
            _parse_utc_date(to_date)
            if to_date
            else (analysis_ts or _last_closed_bar_utc(timeframe))
        )

        from_ts = int(from_dt.timestamp()) * 1000
        to_ts = int(to_dt.timestamp()) * 1000

        db_candles = self._store.query_range(symbol, timeframe, from_ts, to_ts)

        if db_candles:
            db_oldest = db_candles[0].timestamp
            db_newest = db_candles[-1].timestamp
            tf_ms = _TIMEFRAME_SECONDS[timeframe] * 1000

            # Always refresh the tail (latest bar rule)
            fresh_tail = self._mt5_fetch(
                symbol,
                timeframe,
                2,
                None,
                None,
                allow_gaps,
                False,
                analysis_ts,
            )

            # Check if there's a gap at the start we need to fill
            needs_head = db_oldest > (from_ts + tf_ms)

            if needs_head:
                # Fetch the missing head portion from MT5 via date range
                logger.debug("Filling head gap for %s/%s from %s", symbol, timeframe, from_date)
                head_to = datetime.fromtimestamp(db_oldest / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                head_candles = self._mt5_fetch(
                    symbol,
                    timeframe,
                    None,
                    from_date,
                    head_to,
                    allow_gaps,
                    False,
                    analysis_ts,
                )
                merged = _merge(db_candles, _merge(head_candles, fresh_tail))
            else:
                merged = _merge(db_candles, fresh_tail)

            # Filter to requested range
            merged = [c for c in merged if from_ts <= c.timestamp <= to_ts]
            self._store.upsert(symbol, timeframe, merged)
            return merged

        # Nothing in SQLite — full MT5 fetch
        logger.debug("SQLite range miss: %s/%s [%s → %s]", symbol, timeframe, from_date, to_date)
        mt5_candles = self._mt5_fetch(
            symbol,
            timeframe,
            None,
            from_date,
            to_date,
            allow_gaps,
            check_staleness,
            analysis_ts,
        )
        self._store.upsert(symbol, timeframe, mt5_candles)
        return mt5_candles

    # ------------------------------------------------------------------
    # Internal: delegate to raw MT5 provider
    # ------------------------------------------------------------------

    def _mt5_fetch(
        self,
        symbol: str,
        timeframe: str,
        limit: Optional[int],
        from_date: Optional[str],
        to_date: Optional[str],
        allow_gaps: bool,
        check_staleness: bool,
        analysis_ts: Optional[datetime],
    ) -> list[Candle]:
        return self._provider.get_candles(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            from_date=from_date,
            to_date=to_date,
            allow_gaps=allow_gaps,
            check_staleness=check_staleness,
            analysis_ts=analysis_ts,
        )