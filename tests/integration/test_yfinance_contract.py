"""
Contract test : verify yfinance still returns the structure we expect.

This test really hits the yfinance API. It's a "canary" : if yfinance
changes its response format, this test fails and warns us before
the rest of the pipeline breaks silently.

Run it manually or in a weekly scheduled CI, not on every push.
"""

from datetime import date, timedelta

import pytest
import yfinance as yf


@pytest.mark.slow
def test_yfinance_returns_expected_columns():
    """yf.download must still return Open, High, Low, Close, Adj Close, Volume."""
    end = date.today()
    start = end - timedelta(days=7)

    df = yf.download(
        "AAPL",
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        multi_level_index=False,
    )

    assert not df.empty, "yfinance returned no data for AAPL — API may be down"

    expected_columns = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
    actual_columns = set(df.columns)

    missing = expected_columns - actual_columns
    assert not missing, f"yfinance contract broken : missing columns {missing}"


@pytest.mark.slow
def test_yfinance_multi_ticker_returns_multilevel():
    """yf.download with multiple tickers returns a multi-level DataFrame."""
    df = yf.download(
        ["AAPL", "MSFT"],
        period="5d",
        progress=False,
        auto_adjust=False,
        group_by="ticker",
        multi_level_index=True,
    )

    assert not df.empty
    # Multi-level columns
    tickers_in_df = set(df.columns.get_level_values(0))
    assert {"AAPL", "MSFT"}.issubset(tickers_in_df)
