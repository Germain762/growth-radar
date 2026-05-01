"""Unit tests for ArkHoldingsFetcher._parse_decimal."""

import math

import pytest

from ingestion.sources.etf_holdings.ark import ArkHoldingsFetcher


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Standard cases
        ("8.80%", 8.80),
        ("9.73%", 9.73),
        ("0.5%", 0.5),
        # Currency symbol
        ("$146,880,804.56", 146880804.56),
        ("$1.50", 1.50),
        # Thousands separator only
        ("414,344", 414344.0),
        ("1,000,000", 1000000.0),
        # Plain numbers
        ("123.45", 123.45),
        (123.45, 123.45),
        (123, 123.0),
        # Edge cases
        ("", None),
        ("   ", None),
        (None, None),
        ("not a number", None),
    ],
)
def test_parse_decimal(raw, expected):
    """_parse_decimal handles ARK's various number formats."""
    result = ArkHoldingsFetcher._parse_decimal(raw)
    if expected is None:
        assert result is None
    else:
        assert math.isclose(result, expected, rel_tol=1e-9)


def test_parse_decimal_handles_nan():
    """pandas NaN should return None (not crash, not raise)."""
    import numpy as np

    assert ArkHoldingsFetcher._parse_decimal(np.nan) is None
