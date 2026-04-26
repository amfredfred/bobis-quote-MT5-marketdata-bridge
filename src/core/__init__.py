"""Core business logic for MT5 market data processing."""

from .market_data import MarketDataProvider
from .models import (
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
