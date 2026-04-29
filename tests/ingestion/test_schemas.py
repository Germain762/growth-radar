"""Unit tests for Pydantic schemas validating data at ingestion boundary."""

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from ingestion.schemas import TickerInfo, YahooPriceBar


class TestYahooPriceBar:
    """Tests for YahooPriceBar : OHLCV validation from yfinance."""

    def test_valid_bar_with_yfinance_aliases(self):
        """Standard yfinance-style dict should validate and normalize."""
        raw = {
            "ticker": "NVDA",
            "price_date": date(2025, 4, 23),
            "Open": 145.23,
            "High": 147.10,
            "Low": 144.50,
            "Close": 146.80,
            "Adj Close": 146.80,
            "Volume": 52_000_000,
        }
        bar = YahooPriceBar.model_validate(raw)
        assert bar.ticker == "NVDA"
        assert bar.open_price == 145.23
        assert bar.adj_close == 146.80
        assert bar.volume == 52_000_000

    def test_type_coercion_strings_to_numbers(self):
        """Pydantic should coerce string-typed numbers (common in CSVs / APIs)."""
        raw = {
            "ticker": "NVDA",
            "price_date": "2025-04-23",  # string date
            "Open": "145.23",  # string float
            "High": "147.10",
            "Low": "144.50",
            "Close": "146.80",
            "Volume": "52000000",  # string int
        }
        bar = YahooPriceBar.model_validate(raw)
        assert isinstance(bar.open_price, float)
        assert isinstance(bar.price_date, date)
        assert isinstance(bar.volume, int)

    def test_adj_close_optional_for_indices(self):
        """Indices like ^GSPC don't always return Adj Close."""
        raw = {
            "ticker": "^GSPC",
            "price_date": date(2025, 4, 23),
            "Open": 5000.0,
            "High": 5050.0,
            "Low": 4980.0,
            "Close": 5020.0,
            # no Adj Close
            "Volume": 0,
        }
        bar = YahooPriceBar.model_validate(raw)
        assert bar.adj_close is None

    def test_reject_high_lower_than_low(self):
        """Sanity check : high must be >= low (catches yfinance data bugs)."""
        raw = {
            "ticker": "NVDA",
            "price_date": date(2025, 4, 23),
            "Open": 145.0,
            "High": 100.0,  # anomaly !
            "Low": 144.0,
            "Close": 146.0,
            "Volume": 1000,
        }
        with pytest.raises(ValidationError, match="high_price"):
            YahooPriceBar.model_validate(raw)

    def test_reject_negative_volume(self):
        """Volume must be non-negative."""
        raw = {
            "ticker": "NVDA",
            "price_date": date(2025, 4, 23),
            "Open": 145.0,
            "High": 147.0,
            "Low": 144.0,
            "Close": 146.0,
            "Volume": -100,
        }
        with pytest.raises(ValidationError, match="volume"):
            YahooPriceBar.model_validate(raw)

    def test_missing_required_field_raises(self):
        """Required fields like 'ticker' must be present."""
        raw = {
            "price_date": date(2025, 4, 23),
            "Open": 145.0,
            "High": 147.0,
            "Low": 144.0,
            "Close": 146.0,
            "Volume": 1000,
        }
        with pytest.raises(ValidationError, match="ticker"):
            YahooPriceBar.model_validate(raw)

    def test_model_dump_produces_snake_case_keys(self):
        """After validation, model_dump() returns snake_case (our convention)."""
        raw = {
            "ticker": "NVDA",
            "price_date": date(2025, 4, 23),
            "Open": 145.23,
            "High": 147.10,
            "Low": 144.50,
            "Close": 146.80,
            "Adj Close": 146.80,
            "Volume": 52_000_000,
        }
        bar = YahooPriceBar.model_validate(raw)
        dumped = bar.model_dump()
        assert "open_price" in dumped
        assert "adj_close" in dumped
        # Aliases are NOT in the dump
        assert "Open" not in dumped
        assert "Adj Close" not in dumped


class TestTickerInfo:
    """Tests for TickerInfo : ticker static metadata validation."""

    def _base_payload(self) -> dict:
        """A minimal valid payload for an equity ticker."""
        return {
            "ticker": "NVDA",
            "instrument_type": "equity",
            "long_name": "NVIDIA Corporation",
            "short_name": "NVIDIA",
            "exchange": "NMS",
            "currency": "USD",
            "country_hq": "United States",
            "gics_sector": "Technology",
            "gics_industry": "Semiconductors",
            "fetched_at": datetime.now(UTC),
        }

    def test_valid_equity_payload(self):
        """A standard equity payload should validate without surprises."""
        info = TickerInfo.model_validate(self._base_payload())
        assert info.ticker == "NVDA"
        assert info.instrument_type == "equity"
        assert info.gics_sector == "Technology"

    def test_etf_with_no_gics(self):
        """ETFs don't have GICS classification — fields should be None."""
        payload = {
            "ticker": "SOXX",
            "instrument_type": "etf",
            "long_name": "iShares Semiconductor ETF",
            "exchange": "NGM",
            "currency": "USD",
            "etf_category": "Technology",
            "fetched_at": datetime.now(UTC),
            # No gics_*, no country_hq
        }
        info = TickerInfo.model_validate(payload)
        assert info.instrument_type == "etf"
        assert info.gics_sector is None
        assert info.gics_industry is None
        assert info.etf_category == "Technology"

    def test_index_with_minimal_fields(self):
        """Indices like ^GSPC have very little metadata."""
        payload = {
            "ticker": "^GSPC",
            "instrument_type": "index",
            "long_name": "S&P 500",
            "fetched_at": datetime.now(UTC),
        }
        info = TickerInfo.model_validate(payload)
        assert info.instrument_type == "index"
        assert info.currency is None

    def test_extra_fields_are_ignored(self):
        """yfinance throws hundreds of fields at us — extras must be ignored."""
        payload = self._base_payload()
        payload.update(
            {
                "marketCap": 3_000_000_000_000,
                "trailingPE": 65.42,
                "weirdYfinanceField": "lots of noise",
            }
        )
        # Should not raise — extras are silently dropped
        info = TickerInfo.model_validate(payload)
        assert info.ticker == "NVDA"

    def test_required_fields_missing_raises(self):
        """ticker, instrument_type and fetched_at are required."""
        from pydantic import ValidationError

        payload = self._base_payload()
        del payload["ticker"]
        with pytest.raises(ValidationError, match="ticker"):
            TickerInfo.model_validate(payload)

    def test_model_dump_excludes_aliases(self):
        """model_dump() returns clean snake_case dict ready for Parquet."""
        info = TickerInfo.model_validate(self._base_payload())
        dumped = info.model_dump()
        assert "ticker" in dumped
        assert "instrument_type" in dumped
        assert "gics_sector" in dumped
        # fetched_at should still be a datetime, not a string
        assert isinstance(dumped["fetched_at"], datetime)
