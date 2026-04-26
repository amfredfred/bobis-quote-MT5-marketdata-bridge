# MT5 Candle Service - Usage Guide

## Quick Start

### 1. Installation

```bash
# Clone and setup
git clone <repo>
cd bobis-quote-mt5-marketdata-bridge
python -m venv .venv
.venv\Scripts\activate

# Install
pip install -e .

# Configure
cp .env.example .env
# Edit .env with your MT5 credentials
```

### 2. Start the API

```bash
python main.py
# Server runs on http://localhost:8000
```

### 3. Fetch Data

**Using cURL:**
```bash
# Single symbol
curl "http://localhost:8000/api/v1/candles/EURUSD?timeframe=1h&limit=100"

# Multiple symbols (GET)
curl "http://localhost:8000/api/v1/time-series?symbols=EURUSD,GBPUSD&timeframes=1h,4h&limit=100"

# Multiple symbols (POST)
curl -X POST http://localhost:8000/api/v1/time-series \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["EURUSD", "GBPUSD"],
    "timeframes": ["1h", "4h"],
    "limit": 100
  }'
```

**Using Python:**
```python
from src.core import MarketDataProvider, CandleRequest
from src.core.configs import Config

provider = MarketDataProvider(Config)

# Single symbol
candles = provider.get_candles("EURUSD", "1h", limit=100)
print(f"Fetched {len(candles)} candles")

# Multiple
request = CandleRequest(
    symbols=["EURUSD", "GBPUSD"],
    timeframes=["1h", "4h"],
    limit=100
)
result = provider.get_multiple(request)
print(result)

provider.shutdown()
```

## Common Scenarios

### Scenario 1: Backtest Data Collection

```python
request = CandleRequest(
    symbols=["EURUSD", "GBPUSD", "USDJPY"],
    timeframes=["d1"],
    from_date="2024-01-01",
    to_date="2026-04-26",
    allow_gaps=False,
    check_staleness=False  # Historical data isn't "stale"
)

result = provider.get_multiple(request)

for symbol, timeframes in result.items():
    for tf, fetch_result in timeframes.items():
        if fetch_result.status == "success":
            print(f"{symbol}/{tf}: {len(fetch_result.candles)} candles")
        else:
            print(f"{symbol}/{tf}: {fetch_result.error}")
```

### Scenario 2: Real-time Feed Monitoring

```python
# Check that your live feed is current (not stale)
request = CandleRequest(
    symbols=["EURUSD"],
    timeframes=["5m"],
    limit=1,
    check_staleness=True  # Raise error if feed is frozen
)

try:
    result = provider.get_multiple(request)
    print("Feed is live and current ✓")
except StaleDataError as e:
    print(f"Feed is frozen: {e}")
except GapDetectedError as e:
    print(f"Data gaps detected: {e}")
```

### Scenario 3: Multi-timeframe Analysis

```python
# Fetch HTF and LTF for analysis
request = CandleRequest(
    symbols=["EURUSD"],
    timeframes=["1h", "4h", "d1"],  # Lower → Higher
    limit=100,
    allow_gaps=False
)

result = provider.get_multiple(request)
candles_1h = result["EURUSD"]["1h"].candles
candles_4h = result["EURUSD"]["4h"].candles
candles_d1 = result["EURUSD"]["d1"].candles

# All anchored to the same analysis timestamp ✓
```

## Troubleshooting

### "Symbol not found"

```python
# Try without special characters
provider.get_candles("EURUSD", ...)      # ✓
provider.get_candles("EUR/USD", ...)     # ✗ (normalized to same)
provider.get_candles("eur", ...)         # ✓ (prefix match to EURUSD)
provider.get_candles("XAUUSD", ...)      # ✓
```

### "Gaps detected"

```python
# Option 1: Accept gaps (if they're normal for this instrument)
request = CandleRequest(..., allow_gaps=True)

# Option 2: Debug the gaps
try:
    candles = provider.get_candles(...)
except GapDetectedError as e:
    print(f"Gaps at: {e.gaps}")  # [(start, end), ...]
    # Normal session breaks for instruments like XAU, BTC, indices
```

### "Stale feed detected"

```python
# Feed hasn't updated in too long (frozen feed)
# Check if:
# 1. MT5 terminal is running
# 2. Market is open for this timeframe/symbol
# 3. Broker connection is active

provider._worker.ensure_connected()  # Reconnect attempt
```

## API Response Examples

### Single Candle Success

```json
{
  "symbol": "EURUSD",
  "timeframe": "1h",
  "count": 100,
  "candles": [
    {
      "timestamp": 1704067200000,
      "datetime": 1704067200.0,
      "open": 1.0950,
      "high": 1.0960,
      "low": 1.0940,
      "close": 1.0955,
      "volume": 1250.0,
      "is_tick_volume": false
    },
    ...
  ]
}
```

### Batch Response (Mixed Success/Failure)

```json
{
  "EURUSD": {
    "1h": {
      "status": "success",
      "count": 100,
      "candles": [...]
    },
    "4h": {
      "status": "error",
      "error": "No data for EURUSD/4h [2026-04-20 → 2026-04-26]",
      "error_type": "NoDataError"
    }
  },
  "GBPUSD": {
    "1h": {
      "status": "success",
      "count": 95,
      "candles": [...]
    }
  }
}
```

## Performance Tips

1. **Batch requests**: Fetch multiple symbols at once (8 parallel max)
2. **Reasonable limits**: Limit=1000 is safer than 10000
3. **Recent data**: Use `limit=` for recent candles; `from_date=` for historical (slower)
4. **Disable staleness checks for historical data**: `check_staleness=false`

## Documentation

- **Swagger UI**: http://localhost:8000/docs (interactive API explorer)
- **ReDoc**: http://localhost:8000/redoc (static documentation)
- **README**: [README.md](../README.md)
- **API Spec**: Generated automatically by FastAPI
