"""
Unit tests for the ETF orchestrator (find_fetcher dispatcher).

These tests run without any I/O — they verify the dispatch logic only.
"""

from ingestion.sources.etf_holdings.orchestrator import find_fetcher


class TestFindFetcher:
    """Tests for the find_fetcher dispatcher."""

    def test_returns_ark_fetcher_for_arkk(self):
        """ARK tickers should be dispatched to ArkHoldingsFetcher."""
        fetcher = find_fetcher("ARKK")
        assert fetcher is not None
        assert fetcher.issuer_name == "ARK Invest"

    def test_returns_ark_fetcher_for_all_ark_tickers(self):
        """All registered ARK tickers should resolve to ARK fetcher."""
        for ticker in ["ARKK", "ARKQ", "ARKG"]:
            fetcher = find_fetcher(ticker)
            assert fetcher is not None
            assert fetcher.issuer_name == "ARK Invest", f"Expected ARK fetcher for {ticker}"

    def test_returns_ishares_fetcher_for_soxx(self):
        """iShares tickers should be dispatched to IsharesHoldingsFetcher."""
        fetcher = find_fetcher("SOXX")
        assert fetcher is not None
        assert fetcher.issuer_name == "iShares"

    def test_returns_ishares_fetcher_for_all_ishares_tickers(self):
        """All registered iShares tickers should resolve to iShares fetcher."""
        for ticker in ["SOXX", "ICLN", "ITA"]:
            fetcher = find_fetcher(ticker)
            assert fetcher is not None
            assert fetcher.issuer_name == "iShares"

    def test_returns_none_for_unknown_ticker(self):
        """Unregistered ticker should return None (not raise)."""
        assert find_fetcher("UNKNOWN_ETF_XYZ") is None

    def test_returns_none_for_not_yet_implemented_etf(self):
        """
        Tickers planned but not yet implemented should return None.
        This is the real defensive case : we add a ticker to the watchlist
        before writing its issuer fetcher. The pipeline should skip it
        gracefully instead of crashing.
        """
        # BOTZ is in our 10-ETF watchlist but Global X fetcher isn't built yet
        assert find_fetcher("BOTZ") is None

    def test_case_insensitive(self):
        """Ticker matching should be case-insensitive."""
        assert find_fetcher("arkk") is not None
        assert find_fetcher("Arkk") is not None
        assert find_fetcher("ARKK") is not None
