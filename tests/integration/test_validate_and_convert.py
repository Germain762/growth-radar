# tests/integration/test_validate_and_convert.py
"""
Tests for validate_and_convert : converts pandas DataFrame from yfinance
into a list of validated dicts.
"""

import pandas as pd

from ingestion.sources.yahoo_finance import validate_and_convert


def _make_df(dates, opens, highs, lows, closes, adj_closes, volumes):
    """Helper : build a yfinance-like DataFrame."""
    return pd.DataFrame(
        {
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Adj Close": adj_closes,
            "Volume": volumes,
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )


def test_valid_dataframe_yields_all_rows():
    """Standard case : 3 valid rows, all 3 should be converted."""
    df = _make_df(
        dates=["2025-04-21", "2025-04-22", "2025-04-23"],
        opens=[145.0, 146.0, 147.0],
        highs=[148.0, 149.0, 150.0],
        lows=[144.0, 145.0, 146.0],
        closes=[147.0, 148.0, 149.0],
        adj_closes=[147.0, 148.0, 149.0],
        volumes=[1_000_000, 1_100_000, 1_200_000],
    )

    rows = validate_and_convert(df, ticker="NVDA")

    assert len(rows) == 3
    assert all(r["ticker"] == "NVDA" for r in rows)
    assert rows[0]["open_price"] == 145.0


def test_bad_row_skipped_not_crash():
    """A row with high < low is skipped, but other rows are returned."""
    df = _make_df(
        dates=["2025-04-21", "2025-04-22"],
        opens=[145.0, 146.0],
        highs=[148.0, 100.0],  # second row : high (100) < low (145), invalid
        lows=[144.0, 145.0],
        closes=[147.0, 148.0],
        adj_closes=[147.0, 148.0],
        volumes=[1_000_000, 1_100_000],
    )

    rows = validate_and_convert(df, ticker="NVDA")

    assert len(rows) == 1  # only the valid row passes
    assert rows[0]["price_date"].isoformat() == "2025-04-21"


def test_empty_dataframe_returns_empty_list():
    """Empty DataFrame should not crash, just return []."""
    df = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"])
    rows = validate_and_convert(df, ticker="NVDA")
    assert rows == []
