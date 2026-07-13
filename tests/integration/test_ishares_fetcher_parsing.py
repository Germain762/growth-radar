from dbt.utils import date

from ingestion.sources.etf_holdings.ishares import IsharesHoldingsFetcher


class TestExtractAsOfDate:
    def test_standard_preamble(self):
        csv = 'iShares Semiconductor ETF\nFund Holdings as of,"Jul 09, 2026"\nInception Date,"Jul 10, 2001"\n'
        assert IsharesHoldingsFetcher._extract_as_of_date(csv) == date(2026, 7, 9)

    def test_missing_line_returns_none(self):
        csv = 'iShares Semiconductor ETF\nInception Date,"Jul 10, 2001"\n'
        assert IsharesHoldingsFetcher._extract_as_of_date(csv) is None

    def test_empty_dash_returns_none(self):
        csv = 'iShares Semiconductor ETF\nFund Holdings as of,"-"\n'
        assert IsharesHoldingsFetcher._extract_as_of_date(csv) is None
