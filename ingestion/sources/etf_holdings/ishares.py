"""
iShares (BlackRock) holdings fetcher.

iShares CSVs are tricky :
  - First ~9 rows are metadata (fund name, AUM, inception date, etc.)
  - Then a blank line
  - Then the actual headers and data rows
  - Numbers use the same human-readable format as ARK ('1,234.56%')

URL DISCOVERY (when these break) :
  1. Visit https://www.ishares.com/us/products/etf-investments
  2. Search for the ETF ticker (e.g., 'SOXX')
  3. On the fund page, find "Portfolio Holdings"
  4. Right-click on "Detailed Holdings and Analytics" → Copy link address
"""

from datetime import UTC, date, datetime
from io import StringIO

import httpx
import pandas as pd
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.sources.etf_holdings.base import EtfHoldingsFetcher

log = structlog.get_logger()


ISHARES_URLS: dict[str, str] = {
    "SOXX": "https://www.ishares.com/us/products/239705/ishares-phlx-semiconductor-etf/1467271812596.ajax?fileType=csv",
    "ICLN": "https://www.ishares.com/us/products/239738/ishares-global-clean-energy-etf/1467271812596.ajax?fileType=csv",
    "ITA": "https://www.ishares.com/us/products/239502/ishares-us-aerospace-defense-etf/1467271812596.ajax?fileType=csv",
}


class IsharesHoldingsFetcher(EtfHoldingsFetcher):
    """Fetcher for iShares (BlackRock) ETFs."""

    issuer_name = "iShares"
    supported_tickers = list(ISHARES_URLS.keys())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _download_csv(self, url: str) -> str:
        """Download the raw CSV from iShares with retry."""
        log.info("ishares_download_start", url=url)
        with httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "GrowthRadar/1.0 contact@example.com"},
            follow_redirects=True,
        ) as client:
            response = client.get(url)

            if response.status_code == 404:
                raise RuntimeError(
                    f"iShares CSV not found (404) at {url}. "
                    f"See URL DISCOVERY in ishares.py docstring."
                )

            response.raise_for_status()
            log.info("ishares_download_done", url=url, size=len(response.content))
            return response.text

    @staticmethod
    def _parse_decimal(value) -> float | None:
        """Same as ARK's : strip $, %, comma, whitespace."""
        if value is None or pd.isna(value):
            return None
        s = str(value).strip()
        if not s or s == "-":
            return None
        s = s.replace("$", "").replace(",", "").replace("%", "").replace(" ", "")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _find_header_row(csv_text: str) -> int:
        """
        Find the row index of the actual CSV header.

        iShares CSVs have ~9 lines of metadata before the data.
        We detect the header by looking for a line containing 'Ticker'.
        """
        for i, line in enumerate(csv_text.split("\n")):
            if "Ticker" in line and ("Weight" in line or "Name" in line):
                return i
        raise ValueError("Could not find iShares CSV header row (no 'Ticker' column)")

    def fetch(
        self,
        etf_ticker: str,
        composition_date: date | None = None,
    ) -> list[dict]:
        """Fetch current holdings for an iShares ETF."""
        if etf_ticker.upper() not in ISHARES_URLS:
            raise ValueError(f"iShares ticker {etf_ticker} not supported")

        url = ISHARES_URLS[etf_ticker.upper()]
        csv_text = self._download_csv(url)

        # Skip the metadata preamble — find the real header row
        header_row = self._find_header_row(csv_text)
        log.info("ishares_header_detected", etf=etf_ticker, line=header_row)

        df = pd.read_csv(
            StringIO(csv_text),
            skiprows=header_row,
            on_bad_lines="skip",
        )

        # Normalize column names : lowercase + strip
        df.columns = [c.strip().lower() for c in df.columns]

        # Filter : keep only equity holdings (drop cash, footers, NaN)
        # iShares uses 'Asset Class' = 'Equity' for stocks
        if "asset class" in df.columns:
            df = df[df["asset class"].str.strip().str.lower() == "equity"]

        df = df.dropna(subset=["ticker"])
        df = df[df["ticker"].astype(str).str.strip() != "-"]
        df = df[df["ticker"].astype(str).str.strip() != ""]

        snapshot_date = composition_date or datetime.now(UTC).date()
        fetched_at = datetime.now(UTC)

        rows: list[dict] = []
        for _, row in df.iterrows():
            try:
                weight_pct = self._parse_decimal(row.get("weight (%)"))
                if weight_pct is None:
                    continue

                rows.append(
                    {
                        "etf_ticker": etf_ticker.upper(),
                        "company_ticker": str(row["ticker"]).strip().upper(),
                        "company_name": str(row.get("name", "")).strip() or None,
                        "composition_date": snapshot_date,
                        "weight_pct": weight_pct,
                        "shares_held": self._parse_decimal(row.get("shares")),
                        "market_value_usd": self._parse_decimal(row.get("market value")),
                        "issuer": self.issuer_name,
                        "fetched_at": fetched_at,
                    }
                )
            except (ValueError, KeyError) as e:
                log.warning(
                    "ishares_row_skip",
                    etf=etf_ticker,
                    ticker=row.get("ticker"),
                    error=str(e),
                )

        log.info(
            "ishares_fetch_done",
            etf=etf_ticker,
            holdings_count=len(rows),
            snapshot_date=str(snapshot_date),
        )
        return rows
