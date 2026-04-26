# MT5 Candle Service

Production-grade market data API for MetaTrader 5. Fetch OHLCV data with comprehensive gap detection, staleness monitoring, and DST-aware timezone handling.

## Features

- **Thread-safe MT5 integration**: All MT5 calls serialized on a dedicated OS thread (thread-affine requirement)
- **Multi-symbol, multi-timeframe requests**: Batch fetch candles for multiple symbols and timeframes simultaneously
- **Comprehensive data validation**: OHLCV integrity checks, duplicate detection, gap analysis
- **Session-aware gap detection**: Distinguishes real gaps from normal trading session breaks (nightly close, weekends)
- **Staleness monitoring**: Detects frozen feeds and alerts on stale data
- **DST-aware offset management**: Automatic detection and correction of broker UTC offset drift
- **Symbol resolution**: Exact → prefix matching for flexible symbol input
- **FastAPI web service**: REST API with async request handling
- **Type-safe**: Full Pydantic validation and type hints

## Project Structure

```
src/
├── 
│   ├── __init__.py              # Public API exports
│   ├── core/
│   │   ├── __init__.py
│   │   ├── market_data.py       # Production-grade MT5 provider
│   │   ├── models.py            # Data models and exceptions
│   │   ├── constants.py         # Timeframe maps, prefixes
│   │   └── configs.py           # Configuration from environment
│   └── api/
│       ├── __init__.py
│       ├── main.py              # FastAPI app with lifespan
│       └── routes.py            # API endpoints
tests/
├── __init__.py
└── unit/
    ├── __init__.py
    └── test_market_data.py      # Unit tests (mock MT5Worker)
main.py                           # Root entry point: uvicorn main:app
pyproject.toml                    # Modern Python project configuration
requirements.txt                  # Fallback dependency pinning
.env.example                      # Environment variables template
```

## Installation

### Prerequisites

- Python 3.10+
- MetaTrader 5 terminal running on the same machine
- Virtual environment (recommended)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd bobis-quote-mt5-marketdata-bridge
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# or: source .venv/bin/activate  # Linux/macOS
```

3. Install dependencies:
```bash
pip install -e .
# or for development with test tools:
pip install -e ".[dev]"
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your MT5 credentials
```

### Environment Variables

Required in `.env`:

```env
MT5_ACCOUNT_NUMBER=12345678
MT5_ACCOUNT_PASSWORD=your_password
MT5_ACCOUNT_SERVER=broker-server.com
PATH_MT5_EXEC=C:\Program Files\MetaTrader 5\terminal64.exe
BROKER_UTC_OFFSET_HOURS=2
LOG_LEVEL=INFO
```

## Usage

### As a Web Service

Start the API server:

```bash
# Using the root entry point
python main.py

# Or with uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000

# With auto-reload (development only)
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

View API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### API Endpoints

#### Health Check
```bash
GET /api/v1/health
```

#### Fetch Multiple Symbols/Timeframes (POST)
```bash
POST /api/v1/time-series
Content-Type: application/json

{
    "symbols": ["EURUSD", "GBPUSD"],
    "timeframes": ["1h", "4h"],
    "limit": 100,
    "allow_gaps": false,
    "check_staleness": true
}
```

#### Fetch Multiple Symbols/Timeframes (GET)
```bash
GET /api/v1/time-series?symbols=EURUSD,GBPUSD&timeframes=1h,4h&limit=100
```

#### Fetch Single Symbol
```bash
GET /api/v1/candles/EURUSD?timeframe=1h&limit=200
```

### Programmatic Usage

```python
from src.core import MarketDataProvider
from src.core.configs import Config

# Initialize provider
provider = MarketDataProvider(Config)

try:
    # Fetch candles for a single symbol
    candles = provider.get_candles(
        symbol="EURUSD",
        timeframe="1h",
        limit=100,
        check_staleness=True
    )
    
    for candle in candles:
        print(f"{candle.timestamp}: O={candle.open} H={candle.high} "
              f"L={candle.low} C={candle.close} V={candle.volume}")
    
    # Fetch multiple symbols/timeframes
    from src.core import CandleRequest
    
    request = CandleRequest(
        symbols=["EURUSD", "GBPUSD"],
        timeframes=["1h", "4h"],
        limit=100,
        allow_gaps=False
    )
    
    result = provider.get_multiple(request)
    
finally:
    provider.shutdown()
```

## Data Model

### Candle

```python
class Candle(BaseModel):
    timestamp: int          # UTC Unix milliseconds
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_tick_volume: bool    # True if volume is tick volume (CFDs)
```

All timestamps are normalized to UTC milliseconds, regardless of broker offset.

### Exceptions

- `MarketDataError`: Base exception for all market data errors
- `MT5ConnectionError`: Terminal not connected or reconnect failed
- `SymbolNotFoundError`: Symbol not available
- `SymbolResolutionError`: Ambiguous symbol (multiple prefix matches)
- `NoDataError`: No data available for the requested period
- `GapDetectedError`: Gaps detected in the data (when `allow_gaps=False`)
- `StaleDataError`: Feed is frozen / too old
- `DataIntegrityError`: OHLCV validation failures (corrupt bar)

## Configuration

### Timeframes

Supported timeframes: `1m`, `5m`, `6m`, `10m`, `15m`, `30m`, `1h`, `4h`, `d1`, `w1`, `mn1`

### Gap Detection

Gaps are flagged for:
- **Standard instruments** (forex majors): > 1.5× nominal bar
- **Session-break instruments** (commodities, crypto, indices): > 4h intraday or > 75h total
- **Weekly/Monthly**: > 2× nominal bar (calendar variation expected)

Session breaks (nightly close ~2h, weekends ~48-72h) are never flagged as gaps.

### Staleness Checks

A feed is considered stale if the most recent bar is > (2 × timeframe + session margin) behind wall-clock time.

For instruments with session breaks, an additional 4-hour margin is allowed for nightly closes.

## Testing

Run unit tests:

```bash
pytest tests/
pytest tests/ -v --cov=src
```

Tests use mocked MT5Worker to avoid requiring a live terminal.

## Development

### Code Style

Format code:
```bash
black src/ tests/
isort src/ tests/
```

Lint:
```bash
flake8 src/ tests/
mypy src/
```

### Adding Features

1. Update data models in `core/models.py`
2. Implement logic in `core/market_data.py`
3. Add API routes in `api/routes.py`
4. Write tests in `tests/unit/`
5. Update this README

## Windows Service Installation

Install as Windows service (see [install_candle_service.ps1](install_candle_service.ps1)):

```powershell
.\install_candle_service.ps1
```

## Troubleshooting

### "MT5 worker failed to initialize"

- Ensure MetaTrader 5 terminal is running and logged in
- Check `PATH_MT5_EXEC` points to the correct terminal executable
- Verify account credentials in `.env`

### "Broker offset drift detected"

- Your broker's UTC offset may have changed (DST transition)
- Update `BROKER_UTC_OFFSET_HOURS` in `.env`
- The system will auto-correct if drift is within tolerance

### "Symbol not found"

- Use the exact symbol name from your broker (e.g., `EURUSD`, not `EUR/USD`)
- The system supports prefix matching (e.g., `EUR` → `EURUSD`)

### "Gaps detected" when there shouldn't be

- For session-break instruments, the system accounts for nightly closes and weekends
- For forex majors (24/5), gaps > 1.5× bar size trigger the error
- Use `allow_gaps=true` to suppress gap checking

## Performance

- **Concurrent requests**: Up to 8 parallel candle fetches (configurable in `MarketDataProvider`)
- **Response time**: Typically < 100ms per symbol/timeframe
- **Memory**: Minimal (~50MB base + candle data in memory)

## License

MIT License — see [LICENSE](LICENSE) file for details.

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for new functionality
4. Format code with black/isort
5. Submit a pull request

## Support

For issues, questions, or feature requests, open an issue on GitHub.
