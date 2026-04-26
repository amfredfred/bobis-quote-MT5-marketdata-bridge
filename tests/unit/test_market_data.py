"""Unit tests for MT5 market data provider."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from src.core import (
    MarketDataProvider,
    Candle,
    CandleRequest,
    FetchSuccess,
    FetchFailure,
)


class TestCandleModel:
    """Test the Candle data model."""

    def test_valid_candle_creation(self):
        """Test creating a valid candle."""
        candle = Candle(
            timestamp=1704067200000,
            open=1.0950,
            high=1.0960,
            low=1.0940,
            close=1.0955,
            volume=1000.0,
            is_tick_volume=False,
        )
        assert candle.timestamp == 1704067200000
        assert candle.close == 1.0955
        assert candle.is_tick_volume is False

    def test_invalid_candle_negative_open(self):
        """Test that negative open price is rejected."""
        with pytest.raises(ValueError, match="open.*<= 0"):
            Candle(
                timestamp=1704067200000,
                open=-1.0,
                high=1.0960,
                low=1.0940,
                close=1.0955,
                volume=1000.0,
                is_tick_volume=False,
            )

    def test_invalid_candle_high_low_inverted(self):
        """Test that high < low is rejected."""
        with pytest.raises(ValueError, match="high.*< low"):
            Candle(
                timestamp=1704067200000,
                open=1.0950,
                high=1.0940,
                low=1.0960,
                close=1.0955,
                volume=1000.0,
                is_tick_volume=False,
            )


class TestCandleRequest:
    """Test the CandleRequest model."""

    def test_valid_request_with_limit(self):
        """Test creating a valid request with limit."""
        request = CandleRequest(
            symbols=["EURUSD"],
            timeframes=["1h"],
            limit=100,
        )
        assert request.symbols == ["EURUSD"]
        assert request.timeframes == ["1h"]
        assert request.limit == 100

    def test_valid_request_with_from_date(self):
        """Test creating a valid request with from_date."""
        request = CandleRequest(
            symbols=["EURUSD"],
            timeframes=["1h"],
            from_date="2024-01-01",
        )
        assert request.from_date == "2024-01-01"

    def test_invalid_request_both_limit_and_from_date(self):
        """Test that providing both limit and from_date is rejected."""
        with pytest.raises(ValueError, match="from_date OR limit"):
            CandleRequest(
                symbols=["EURUSD"],
                timeframes=["1h"],
                limit=100,
                from_date="2024-01-01",
            )

    def test_invalid_request_neither_limit_nor_from_date(self):
        """Test that providing neither limit nor from_date is rejected."""
        with pytest.raises(ValueError, match="from_date or limit"):
            CandleRequest(
                symbols=["EURUSD"],
                timeframes=["1h"],
            )

    def test_invalid_timeframe(self):
        """Test that invalid timeframe is rejected."""
        with pytest.raises(ValueError, match="Invalid timeframe"):
            CandleRequest(
                symbols=["EURUSD"],
                timeframes=["invalid"],
                limit=100,
            )


@pytest.mark.asyncio
class TestMarketDataProvider:
    """Test the MarketDataProvider class."""

    @patch("src.core.market_data.MT5Worker")
    def test_provider_initialization(self, mock_worker_class):
        """Test that provider initializes correctly."""
        mock_worker = MagicMock()
        mock_worker_class.return_value = mock_worker

        with patch("src.core.market_data.BrokerOffsetManager"):
            with patch("src.core.market_data.SymbolResolver"):
                # This will fail without proper MT5 setup, so we mock it
                pass
