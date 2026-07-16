"""
Pydantic schemas for validating ingested data at the source boundary.

Why validate at ingestion ? Because APIs (yfinance, SEC, etc.) change their
schemas silently. Without validation, bad data propagates downstream and is
discovered 3 layers later, much harder to debug.

Pattern : every raw row from an external source goes through a Pydantic
model before being written to bronze. If validation fails, we log and skip
(fail-soft) rather than crash the whole batch (fail-hard).
"""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class YahooPriceBar(BaseModel):
    """
    A single daily OHLCV bar from yfinance.

    Note : yfinance returns columns with Capitalized names (Open, High, etc.).
    We map them to snake_case at this boundary.
    """

    model_config = ConfigDict(
        populate_by_name=True,  # accept both 'Open' and 'open_price'
        str_strip_whitespace=True,
    )

    ticker: str
    price_date: date
    open_price: float = Field(alias="Open")
    high_price: float = Field(alias="High")
    low_price: float = Field(alias="Low")
    close_price: float = Field(alias="Close")
    adj_close: float | None = Field(default=None, alias="Adj Close")
    volume: int = Field(alias="Volume")

    @field_validator("high_price")
    @classmethod
    def high_must_be_highest(cls, v: float, info) -> float:
        """Sanity check : high >= open, close, low."""
        values = info.data
        for field_name in ("open_price", "low_price", "close_price"):
            other = values.get(field_name)
            if other is not None and v < other:
                raise ValueError(f"high_price ({v}) < {field_name} ({other})")
        return v

    @field_validator("volume")
    @classmethod
    def volume_must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"volume must be >= 0, got {v}")
        return v


class TickerInfo(BaseModel):
    """
    Static metadata for a ticker (company or ETF) from yfinance.

    Note : yfinance .info dict is huge and unstable.
    We extract only the fields we need and trust those that work.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",  # ignore any extra keys yfinance throws at us
        str_strip_whitespace=True,
    )

    ticker: str
    instrument_type: str  # 'equity' | 'etf' | 'index'
    long_name: str | None = None
    short_name: str | None = None
    exchange: str | None = None
    currency: str | None = None
    country_hq: str | None = None

    # Yahoo Finance classification (only for equities)
    yahoo_sector: str | None = None
    industry_group: str | None = None
    yahoo_industry: str | None = None
    sub_industry: str | None = None

    # ETF-specific fields
    etf_category: str | None = None

    # Timestamp of when we fetched this info
    fetched_at: datetime


class EtfHolding(BaseModel):
    """
    A single ETF holding line (one row = one company in one ETF on one date).

    All issuer-specific formats are normalized to this schema by the fetchers.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        str_strip_whitespace=True,
    )

    # Identification
    etf_ticker: str  # the ETF holding the company (e.g., 'SOXX')
    company_ticker: str  # the held company (e.g., 'NVDA')
    company_name: str | None = None  # full name as published by issuer

    # Snapshot reference
    composition_date: date  # date the composition was published

    # Position
    weight_pct: float  # % of the ETF (0-100, NOT 0-1)
    shares_held: float | None = None  # number of shares
    market_value_usd: float | None = None

    # Metadata
    issuer: str  # e.g., 'iShares', 'ARK Invest'
    fetched_at: datetime
