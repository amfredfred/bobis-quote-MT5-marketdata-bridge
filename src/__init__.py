"""MT5 Candle Service - Production-grade market data provider for MetaTrader 5."""

__version__ = "1.0.0"

from .core.market_data import MarketDataProvider
from .core.models import (
    Candle,
    CandleRequest,
    FetchFailure,
    FetchResult,
    FetchSuccess,
    MarketDataError,
    MT5ConnectionError,
    SymbolNotFoundError,
    SymbolResolutionError,
    NoDataError,
    StaleDataError,
    GapDetectedError,
    DataIntegrityError,
)

__all__ = [
    "MarketDataProvider",
    "Candle",
    "CandleRequest",
    "FetchSuccess",
    "FetchFailure",
    "FetchResult",
    "MarketDataError",
    "MT5ConnectionError",
    "SymbolNotFoundError",
    "SymbolResolutionError",
    "NoDataError",
    "StaleDataError",
    "GapDetectedError",
    "DataIntegrityError",
]
