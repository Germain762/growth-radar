"""
ARK Invest holdings fetcher.

ARK publishes daily holdings as CSVs at predictable URLs.
The CSV starts with a title row, then a clean data table.

URL DISCOVERY (when these break) :
  1. Visit https://www.ark-funds.com/funds
  2. Click on a fund (e.g., "ARK Innovation ETF")
  3. Scroll to the "Holdings" section
  4. Right-click on "Download Holdings (CSV)" → Copy link address

These URLs change occasionally (ARK migrated CDN in early 2025
from ark-funds.com to assets.ark-funds.com). The slow contract
test is the canary that detects this.

CSV format note :
  ARK presents numbers in human-readable form ('8.80%', '$146,880,804.56',
  '414,344' with thousands separator). We strip these decorations at parse
  time via _parse_decimal().
"""

from datetime import UTC, date, datetime
from io import StringIO

import httpx
import pandas as pd
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.sources.etf_holdings.base import EtfHoldingsFetcher

log = structlog.get_logger()


ARK_URLS: dict[str, str] = {
    "ARKK": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    "ARKQ": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
    "ARKG": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv",
}


class ArkHoldingsFetcher(EtfHoldingsFetcher):
    """Fetcher for ARK Invest ETFs."""

    issuer_name = "ARK Invest"
    supported_tickers = list(ARK_URLS.keys())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _download_csv(self, url: str) -> str:
        """Download the raw CSV text from ARK with retry on transient errors."""
        log.info("ark_download_start", url=url)
        with httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "GrowthRadar/1.0 contact@example.com"},
            follow_redirects=True,
        ) as client:
            response = client.get(url)

            if response.status_code == 404:
                raise RuntimeError(
                    f"ARK CSV not found (404) at {url}. "
                    f"This usually means ARK changed the URL pattern. "
                    f"See URL DISCOVERY in ark.py docstring."
                )

            response.raise_for_status()
            log.info("ark_download_done", url=url, size=len(response.content))
            return response.text

    @staticmethod
    def _parse_decimal(value) -> float | None:
        """
        Parse a 'human-readable' number from ARK's CSV.

        Handles common decorations :
          - Percent symbol : '8.80%' → 8.80
          - Currency symbol : '$146,880,804.56' → 146880804.56
          - Thousands separator : '414,344' → 414344.0
          - NaN / empty / None → None
        """
        if value is None or pd.isna(value):
            return None

        s = str(value).strip()
        if not s:
            return None

        # Strip common decorations
        s = s.replace("$", "").replace(",", "").replace("%", "").replace(" ", "")

        if not s:
            return None

        try:
            return float(s)
        except ValueError:
            return None

    def _extract_composition_date(self, df: pd.DataFrame) -> date | None:
        """
        Read the composition date from ARK's 'date' column (format MM/DD/YYYY).
        Returns None if absent or unparseable (caller falls back to today).
        """
        if "date" not in df.columns:
            return None
        raw = df["date"].dropna()
        if raw.empty:
            return None
        try:
            return datetime.strptime(str(raw.iloc[0]).strip(), "%m/%d/%Y").date()
        except ValueError:
            return None

    def fetch(
        self,
        etf_ticker: str,
        composition_date: date | None = None,
    ) -> list[dict]:
        """Fetch current holdings for an ARK ETF."""
        if etf_ticker.upper() not in ARK_URLS:
            raise ValueError(f"ARK ticker {etf_ticker} not supported")

        url = ARK_URLS[etf_ticker.upper()]
        csv_text = self._download_csv(url)

        df = pd.read_csv(
            StringIO(csv_text),
            on_bad_lines="skip",
        )

        df.columns = [c.strip().lower() for c in df.columns]

        df = df.dropna(subset=["ticker", "weight (%)"])
        df = df[df["ticker"].astype(str).str.strip() != ""]

        # Prefer the issuer's declared date over today's date.
        # Priority : explicit arg > date parsed from file > today (last resort).
        file_date = self._extract_composition_date(df)
        snapshot_date = composition_date or file_date or datetime.now(UTC).date()

        if file_date is None and composition_date is None:
            log.warning("ark_no_date_in_file", etf=etf_ticker, fallback="today")

        fetched_at = datetime.now(UTC)

        rows: list[dict] = []
        for _, row in df.iterrows():
            try:
                weight_pct = self._parse_decimal(row.get("weight (%)"))
                if weight_pct is None:
                    continue  # skip row with no usable weight

                rows.append(
                    {
                        "etf_ticker": etf_ticker.upper(),
                        "company_ticker": str(row["ticker"]).strip().upper(),
                        "company_name": str(row.get("company", "")).strip() or None,
                        "composition_date": snapshot_date,
                        "weight_pct": weight_pct,
                        "shares_held": self._parse_decimal(row.get("shares")),
                        "market_value_usd": self._parse_decimal(row.get("market value ($)")),
                        "issuer": self.issuer_name,
                        "fetched_at": fetched_at,
                    }
                )
            except (ValueError, KeyError) as e:
                log.warning(
                    "ark_row_skip",
                    etf=etf_ticker,
                    ticker=row.get("ticker"),
                    error=str(e),
                )

        log.info(
            "ark_fetch_done",
            etf=etf_ticker,
            holdings_count=len(rows),
            snapshot_date=str(snapshot_date),
        )
        return rows
