"""
Yahoo Finance ticker info ingestion : static metadata per ticker.

Unlike prices, this data changes rarely (sector reclassification once a year max).
We materialize it weekly, not daily.

Target : s3://bronze/yahoo_finance/ticker_info/snapshot_date=YYYY-MM-DD/part.parquet
"""

from __future__ import annotations

import io
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import yfinance as yf
from pydantic import ValidationError

from ingestion.common.s3_client import get_s3_client
from ingestion.schemas import TickerInfo

log = structlog.get_logger()

BRONZE_BUCKET = "bronze"
SOURCE_PREFIX = "yahoo_finance/ticker_info"


def fetch_ticker_info(ticker: str) -> dict:
    """
    Fetch static metadata for one ticker via yfinance.

    Returns a dict with only the fields we care about.
    The full yfinance .info dict has 100+ keys, most we don't need.
    """
    log.info("fetching_ticker_info", ticker=ticker)

    info = yf.Ticker(ticker).info
    if not info:
        raise ValueError(f"yfinance returned empty info for {ticker}")

    # Determine instrument type from yfinance hints
    quote_type = info.get("quoteType", "").upper()
    match quote_type:
        case "ETF":
            instrument_type = "etf"
        case "INDEX":
            instrument_type = "index"
        case "EQUITY":
            instrument_type = "equity"
        case _:
            instrument_type = "unknown"

    return {
        "ticker": ticker,
        "instrument_type": instrument_type,
        "long_name": info.get("longName"),
        "short_name": info.get("shortName"),
        "exchange": info.get("exchange"),
        "currency": info.get("currency"),
        "country_hq": info.get("country"),
        # GICS-style fields (only present on equities)
        "gics_sector": info.get("sector"),
        "gics_industry_group": None,  # yfinance doesn't expose this level
        "gics_industry": info.get("industry"),
        "gics_sub_industry": None,  # not exposed either
        # ETF-specific
        "etf_category": info.get("category") if instrument_type == "etf" else None,
        "fetched_at": datetime.now(UTC),
    }


def validate_and_convert_info(raw: dict) -> dict | None:
    """Validate one raw info dict via Pydantic. Returns None on failure."""
    try:
        validated = TickerInfo.model_validate(raw)
        return validated.model_dump()
    except ValidationError as e:
        log.warning("validation_failed", ticker=raw.get("ticker"), error=str(e))
        return None


def write_ticker_info_to_minio(rows: list[dict], snapshot_date: date, s3_client=None) -> str:
    """Write ticker info batch to MinIO, partitioned by snapshot date."""
    if not rows:
        log.warning("no_rows_to_write")
        return ""

    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = f"{SOURCE_PREFIX}/snapshot_date={snapshot_date.isoformat()}/part.parquet"

    s3 = s3_client if s3_client is not None else get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )

    log.info(
        "ticker_info_written",
        bucket=BRONZE_BUCKET,
        key=key,
        rows=len(rows),
    )

    return key
