"""
Yahoo Finance ingestion : daily OHLCV prices.

Target : s3://bronze/yahoo_finance/prices/date=YYYY-MM-DD/part.parquet

Design decisions :
    - Partitioned by date (Hive-style) : enables partition pruning in queries
      and makes backfills/reruns idempotent (one file per day, overwritable).
    - One ticker per row (long format), not wide : more flexible for downstream.
    - Pydantic validation per row : bad rows are logged and skipped, not crashing.
    - structlog for structured logging : JSON output is easier to parse later.

CLI usage :
    python -m ingestion.sources.yahoo_finance --tickers NVDA,AAPL --days 30
"""
from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import yfinance as yf
from dotenv import load_dotenv
from pydantic import ValidationError

from ingestion.common.s3_client import get_s3_client
from ingestion.schemas import YahooPriceBar

# Load .env if present (for local dev)
load_dotenv()

# Configure structlog to output readable logs in dev
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

BRONZE_BUCKET = "bronze"
SOURCE_PREFIX = "yahoo_finance/prices"


def fetch_ticker_prices(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Download OHLCV data for a single ticker via yfinance.

    Returns a DataFrame with columns : Open, High, Low, Close, Adj Close, Volume
    Index is a DatetimeIndex.

    Raises if the download returns empty (ticker delisted, typo, etc.).
    """
    log.info("fetching_ticker", ticker=ticker, start=str(start_date), end=str(end_date))

    df = yf.download(
        ticker,
        start=start_date,
        end=end_date + timedelta(days=1),   # yfinance end is exclusive
        progress=False,
        auto_adjust=False,   # we want both 'Close' and 'Adj Close'
        multi_level_index=False,
    )

    if df.empty:
        raise ValueError(f"No data returned for ticker {ticker}")

    return df


def validate_and_convert(df: pd.DataFrame, ticker: str) -> list[dict]:
    """
    Apply Pydantic validation row by row.

    Returns a list of valid rows as dicts (ready for PyArrow).
    Bad rows are logged and skipped.
    """
    valid_rows: list[dict] = []
    bad_rows = 0

    for idx, row in df.iterrows():
        # idx is a Timestamp (DatetimeIndex)
        price_date = idx.date() if hasattr(idx, "date") else idx

        raw = {
            "ticker": ticker,
            "price_date": price_date,
            "Open": row.get("Open"),
            "High": row.get("High"),
            "Low": row.get("Low"),
            "Close": row.get("Close"),
            "Adj Close": row.get("Adj Close"),
            "Volume": int(row.get("Volume") or 0),
        }

        try:
            validated = YahooPriceBar.model_validate(raw)
            valid_rows.append(validated.model_dump())
        except ValidationError as e:
            log.warning(
                "validation_failed",
                ticker=ticker,
                price_date=str(price_date),
                error=str(e),
            )
            bad_rows += 1

    if bad_rows > 0:
        log.warning("bad_rows_count", ticker=ticker, count=bad_rows)

    return valid_rows


def write_parquet_to_minio(rows: list[dict], target_date: date) -> str:
    """
    Write a list of rows as a Parquet file to MinIO at :
        s3://bronze/yahoo_finance/prices/date=YYYY-MM-DD/part.parquet

    Returns the S3 key where the file was written.
    """
    if not rows:
        log.warning("no_rows_to_write", target_date=str(target_date))
        return ""

    # Convert to PyArrow table (preserves types better than pandas)
    table = pa.Table.from_pylist(rows)

    # Serialize to in-memory bytes
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    # Build Hive-style partition key
    key = f"{SOURCE_PREFIX}/date={target_date.isoformat()}/part.parquet"

    s3 = get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )

    log.info(
        "parquet_written",
        bucket=BRONZE_BUCKET,
        key=key,
        rows=len(rows),
        size_bytes=len(buf.getvalue()),
    )

    return key


def load_watchlist(path: Path) -> list[str]:
    """Read tickers from a CSV watchlist file."""
    df = pd.read_csv(path)
    tickers = df["ticker"].tolist()
    log.info("watchlist_loaded", count=len(tickers), tickers=tickers)
    return tickers


@click.command()
@click.option(
    "--tickers",
    default=None,
    help="Comma-separated list of tickers (overrides watchlist file).",
)
@click.option(
    "--watchlist",
    default="ingestion/config/watchlist_week1.csv",
    help="Path to watchlist CSV.",
)
@click.option(
    "--days",
    default=730,
    type=int,
    help="Lookback window in days (default : 2 years).",
)
def main(tickers: str | None, watchlist: str, days: int):
    """Ingest yfinance daily prices into bronze bucket."""
    # Load tickers
    if tickers:
        ticker_list = [t.strip() for t in tickers.split(",")]
    else:
        ticker_list = load_watchlist(Path(watchlist))

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    log.info(
        "ingestion_started",
        tickers_count=len(ticker_list),
        start=str(start_date),
        end=str(end_date),
    )

    # Group all rows by price_date to partition properly
    rows_by_date: dict[date, list[dict]] = {}

    for ticker in ticker_list:
        try:
            df = fetch_ticker_prices(ticker, start_date, end_date)
            rows = validate_and_convert(df, ticker)

            for row in rows:
                d = row["price_date"]
                rows_by_date.setdefault(d, []).append(row)

        except Exception as e:
            log.error("ticker_failed", ticker=ticker, error=str(e))

    # Write one Parquet file per date partition
    for target_date, rows in sorted(rows_by_date.items()):
        write_parquet_to_minio(rows, target_date)

    log.info(
        "ingestion_completed",
        dates_written=len(rows_by_date),
        total_rows=sum(len(r) for r in rows_by_date.values()),
    )


if __name__ == "__main__":
    main()
