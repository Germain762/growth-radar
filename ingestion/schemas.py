"""
Pydantic schemas for validating ingested data at the source boundary.

Why validate at ingestion ? Because APIs (yfinance, SEC, etc.) change their
schemas silently. Without validation, bad data propagates downstream and is
discovered 3 layers later, much harder to debug.

Pattern : every raw row from an external source goes through a Pydantic
model before being written to bronze. If validation fails, we log and skip
(fail-soft) rather than crash the whole batch (fail-hard).
"""

from datetime import date

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
