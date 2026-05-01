"""
Abstract base for ETF holdings fetchers.

Each issuer (iShares, ARK, Global X, etc.) has its own CSV format.
Concrete fetchers implement the issuer-specific logic, and all expose
a common interface defined here.

This is the Strategy pattern : the orchestrator picks the right fetcher
based on the ETF ticker, without knowing the implementation details.
"""

from abc import ABC, abstractmethod
from datetime import date

import structlog

log = structlog.get_logger()


class EtfHoldingsFetcher(ABC):
    """
    Abstract base class for ETF holdings fetchers.

    Subclasses must implement :
        - fetch(etf_ticker)         : download and return list of EtfHolding dicts
        - issuer_name (class attr)  : 'iShares', 'ARK Invest', etc.
        - supported_tickers (class attr) : list of tickers this fetcher handles
    """

    issuer_name: str = "Unknown"
    supported_tickers: list[str] = []

    @abstractmethod
    def fetch(
        self,
        etf_ticker: str,
        composition_date: date | None = None,
    ) -> list[dict]:
        """
        Download holdings for the given ETF ticker.

        Returns a list of dicts ready for EtfHolding.model_validate().
        Empty list if no data available.
        Raises Exception on fatal errors (network, parsing).
        """
        ...

    def supports(self, etf_ticker: str) -> bool:
        """Return True if this fetcher handles the given ticker."""
        return etf_ticker.upper() in {t.upper() for t in self.supported_tickers}
