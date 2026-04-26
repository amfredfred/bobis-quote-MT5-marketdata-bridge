# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-26

### Added

- **Production-grade MT5 integration**
  - Thread-affine MT5Worker for serialized API calls
  - BrokerOffsetManager with DST-aware automatic offset verification
  - SymbolResolver with exact → prefix matching

- **Comprehensive data validation**
  - OHLCV integrity checks (negative prices, inverted HLOC)
  - Duplicate timestamp detection
  - Session-aware gap detection (distinguishes real gaps from nightly/weekend breaks)
  - Staleness monitoring (detects frozen feeds)

- **FastAPI REST service**
  - POST `/api/v1/time-series` — batch fetch with JSON body
  - GET `/api/v1/time-series` — batch fetch with query parameters
  - GET `/api/v1/candles/{symbol}` — single symbol convenience endpoint
  - GET `/api/v1/health` — health check
  - GET `/` — service info
  - Swagger UI at `/docs` and ReDoc at `/redoc`

- **Data models**
  - `Candle`: frozen dataclass with full OHLCV + metadata
  - `CandleRequest`: request validation with Pydantic
  - `FetchSuccess` / `FetchFailure`: typed result union
  - Exception hierarchy: `MarketDataError`, `MT5ConnectionError`, `SymbolNotFoundError`, etc.

- **Project structure**
  - Modular architecture: `src/core/` for reusable library
  - API layer: `src/api/` separate from business logic
  - Modern Python packaging with `pyproject.toml`
  - Comprehensive README and documentation

- **Configuration**
  - Environment-based setup (`.env` file)
  - Support for 11 timeframes (1m to MN1)
  - Configurable session breaks and gap thresholds
  - Dynamic offset verification every 6 hours

### Features

- **Symbol resolution**: Exact → unique-prefix → error (never silently substitutes)
- **Multi-symbol, multi-timeframe**: Fetch up to 8 concurrent combinations
- **MTF synchronization**: Single analysis timestamp anchored to highest timeframe
- **Volume transparency**: `is_tick_volume` flag on every candle
- **Type safety**: Full type hints and Pydantic validation
- **Async-ready**: FastAPI with asyncio thread pool for blocking MT5 calls

---

## Future Roadmap

### [1.1.0] - Planned

- [ ] WebSocket support for real-time candle updates
- [ ] Caching layer for recent candles
- [ ] Candle aggregation (e.g., 5-minute from 1-minute data)
- [ ] Database storage backend
- [ ] Enhanced logging and observability (OpenTelemetry)

### [1.2.0] - Planned

- [ ] Multi-account support
- [ ] Advanced filtering (volume, price range, etc.)
- [ ] Candle replay from history
- [ ] Timezone conversion utilities
