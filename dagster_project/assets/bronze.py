"""
Bronze layer assets : raw data ingested from external APIs.

Naming convention : <source>_<dataset>_bronze
"""

from datetime import date, timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.resources.s3 import S3Resource
from ingestion.sources.yahoo_finance import (
    fetch_ticker_prices,
    load_watchlist,
    validate_and_convert,
    write_parquet_to_minio,
)

# A daily partition starting 2 years ago.
# Each partition = one trading day.
# This enables backfills per date, and only-current-day in scheduled runs.
daily_partitions = DailyPartitionsDefinition(start_date="2023-04-01")


@asset(
    name="yahoo_prices_bronze",
    description="Raw OHLCV daily prices from Yahoo Finance, partitioned by date.",
    group_name="bronze",
    partitions_def=daily_partitions,
    compute_kind="python",
)
def yahoo_prices_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    """
    Materialize one day-partition of yfinance prices.

    For each partition (a date), fetches all watchlist tickers' prices
    for that day and writes a Parquet file to s3://bronze/yahoo_finance/...
    """
    target_date = date.fromisoformat(context.partition_key)

    context.log.info(f"Materializing yahoo_prices for partition {target_date}")

    # Load watchlist
    watchlist_path = Path("ingestion/config/watchlist_week1.csv")
    tickers = load_watchlist(watchlist_path)

    rows: list[dict] = []
    failed_tickers: list[str] = []

    # For a daily partition, we fetch only that day's data.
    # yfinance needs start/end with end exclusive, so we ask for a 2-day window
    # and filter to keep only the target date.
    fetch_start = target_date
    fetch_end = target_date + timedelta(days=1)

    for ticker in tickers:
        try:
            df = fetch_ticker_prices(ticker, fetch_start, fetch_end)
            valid_rows = validate_and_convert(df, ticker)

            # Keep only rows for the target date
            day_rows = [r for r in valid_rows if r["price_date"] == target_date]
            rows.extend(day_rows)

        except Exception as e:
            context.log.warning(f"Ticker {ticker} failed: {e}")
            failed_tickers.append(ticker)

    if not rows:
        context.log.warning(f"No data for partition {target_date} (probably non-trading day)")
        return MaterializeResult(
            metadata={
                "rows_written": 0,
                "tickers_failed": failed_tickers,
                "is_trading_day": False,
            }
        )

    # Write to MinIO using our existing function
    s3_key = write_parquet_to_minio(rows, target_date)

    return MaterializeResult(
        metadata={
            "rows_written": len(rows),
            "tickers_succeeded": len({r["ticker"] for r in rows}),
            "tickers_failed": failed_tickers,
            "s3_key": MetadataValue.text(f"s3://bronze/{s3_key}"),
            "partition_date": str(target_date),
        }
    )
