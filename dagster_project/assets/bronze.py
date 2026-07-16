"""
Bronze layer assets : raw data ingested from external APIs.

Two assets for yfinance prices :
  - yahoo_prices_history_bronze : partitioned by ticker, used for initial
    historical backfill. Each ticker run = 1 API call covering full history.
  - yahoo_prices_daily_bronze : partitioned by day, used for daily updates.
    Each day run = 1 API call covering all watchlist tickers.

This split optimizes for yfinance's rate limits : we minimize the number of
API calls by grouping appropriately.
"""

import io
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from dagster_project.resources.s3 import S3Resource
from ingestion.sources.yahoo_finance import (
    BRONZE_BUCKET,
    SOURCE_PREFIX,
    fetch_ticker_prices,
    load_watchlist,
    validate_and_convert,
)

# ---- Watchlist loaded once at module import ----
WATCHLIST_PATH = Path("ingestion/config/watchlist_week1.csv")
WATCHLIST_TICKERS = load_watchlist(WATCHLIST_PATH)

# ---- Partitions definitions ----
ticker_partitions = StaticPartitionsDefinition(WATCHLIST_TICKERS)
daily_partitions = DailyPartitionsDefinition(start_date="2023-04-01")


# =========================================================
# Asset 1 : historical backfill, partitioned by ticker
# =========================================================
@asset(
    name="yahoo_prices_history_bronze",
    description=(
        "Historical OHLCV from Yahoo Finance, one partition per ticker. "
        "Use this for the initial backfill : each materialization "
        "downloads the full history of one ticker in a single API call."
    ),
    group_name="bronze",
    partitions_def=ticker_partitions,
    compute_kind="python",
    pool="bronze",
)
def yahoo_prices_history_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    """Materialize one ticker's full history."""
    ticker = context.partition_key
    end_date = date.today()
    start_date = date(2023, 1, 1)  # 2+ years of history

    context.log.info(f"Fetching {ticker} from {start_date} to {end_date}")

    df = yf.download(
        ticker,
        start=start_date,
        end=end_date + timedelta(days=1),
        progress=False,
        auto_adjust=False,
        multi_level_index=False,
    )

    if df.empty:
        context.log.warning(f"No data for {ticker}")
        return MaterializeResult(metadata={"rows_written": 0})

    # Validate row by row
    valid_rows = validate_and_convert(df, ticker)

    if not valid_rows:
        return MaterializeResult(metadata={"rows_written": 0})

    # Group rows by date and write one file per (date, ticker partition)
    rows_by_date: dict[date, list[dict]] = {}
    for row in valid_rows:
        rows_by_date.setdefault(row["price_date"], []).append(row)

    s3_client = s3.get_client()
    files_written = 0

    for target_date, rows in rows_by_date.items():
        # Note : we write per-ticker per-date. This is intentional :
        # downstream, dbt/DuckDB will read all part-*.parquet files in
        # date=YYYY-MM-DD/ folders and union them naturally.
        key = f"{SOURCE_PREFIX}/date={target_date.isoformat()}/ticker={ticker}.parquet"

        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=buf.getvalue(),
        )
        files_written += 1

    return MaterializeResult(
        metadata={
            "ticker": ticker,
            "rows_written": len(valid_rows),
            "files_written": files_written,
            "date_range_start": str(min(rows_by_date)),
            "date_range_end": str(max(rows_by_date)),
        }
    )


# =========================================================
# Asset 2 : daily updates, partitioned by day
# =========================================================
@asset(
    name="yahoo_prices_daily_bronze",
    description=(
        "Daily OHLCV from Yahoo Finance, one partition per trading day. "
        "Use this for incremental refreshes : each materialization "
        "fetches all watchlist tickers in a single API call."
    ),
    group_name="bronze",
    partitions_def=daily_partitions,
    compute_kind="python",
    pool="bronze",
)
def yahoo_prices_daily_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    """Materialize one day's prices for all watchlist tickers."""
    target_date = date.fromisoformat(context.partition_key)

    context.log.info(f"Fetching {len(WATCHLIST_TICKERS)} tickers for {target_date}")

    # Single bulk download for all tickers
    df = yf.download(
        WATCHLIST_TICKERS,
        start=target_date,
        end=target_date + timedelta(days=1),
        progress=False,
        auto_adjust=False,
        group_by="ticker",
        multi_level_index=True,
    )

    if df.empty:
        context.log.warning(f"No data for {target_date} (non-trading day?)")
        return MaterializeResult(metadata={"rows_written": 0, "is_trading_day": False})

    # Reshape : yfinance returns a multi-level df (ticker × OHLCV)
    # We flatten it to one row per ticker
    all_rows: list[dict] = []
    for ticker in WATCHLIST_TICKERS:
        if ticker not in df.columns.get_level_values(0):
            continue
        ticker_df = df[ticker].dropna(how="all")
        if ticker_df.empty:
            continue
        rows = validate_and_convert(ticker_df, ticker)
        all_rows.extend(rows)

    if not all_rows:
        # Distinguish "market closed" from "source broken" — both produce
        # zero rows, but only one is normal. We probe the reference index :
        # if it has no data either, the market was closed. If it does have
        # data but our tickers don't, something is wrong.
        reference_bars = fetch_ticker_prices(
            "^GSPC",
            start_date=target_date,
            end_date=target_date + timedelta(days=1),
        )
        market_was_open = bool(reference_bars)

        reason = (
            "source_returned_nothing_but_market_was_open" if market_was_open else "market_closed"
        )
        if market_was_open:
            context.log.warning(
                f"No data for {target_date} but ^GSPC has data — "
                f"this is NOT a market closure, investigate."
            )

        return MaterializeResult(
            metadata={
                "rows_written": 0,
                "target_date": str(target_date),
                "empty_reason": reason,
                "market_was_open": market_was_open,
            }
        )

    # Write one Parquet for this day, all tickers concatenated
    s3_client = s3.get_client()
    key = f"{SOURCE_PREFIX}/date={target_date.isoformat()}/part_daily.parquet"

    table = pa.Table.from_pylist(all_rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )

    return MaterializeResult(
        metadata={
            "partition_date": str(target_date),
            "tickers_count": len({r["ticker"] for r in all_rows}),
            "rows_written": len(all_rows),
            "s3_key": MetadataValue.text(f"s3://{BRONZE_BUCKET}/{key}"),
        }
    )


# =========================================================
# Asset 3 : ticker static info (sector, industry, country)
# =========================================================
@asset(
    name="yahoo_ticker_info_bronze",
    description=(
        "Static metadata per ticker : sector, industry, country, etc. "
        "Refreshed weekly because this data changes rarely."
    ),
    group_name="bronze",
    compute_kind="python",
    pool="bronze",
)
def yahoo_ticker_info_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    """Fetch ticker info for all watchlist tickers in one go."""
    from ingestion.sources.yahoo_ticker_info import (
        fetch_ticker_info,
        validate_and_convert_info,
        write_ticker_info_to_minio,
    )

    snapshot_date = datetime.now(UTC).date()

    valid_rows: list[dict] = []
    failed: list[str] = []

    for ticker in WATCHLIST_TICKERS:
        try:
            raw = fetch_ticker_info(ticker)
            validated = validate_and_convert_info(raw)
            if validated:
                valid_rows.append(validated)
            else:
                failed.append(ticker)
        except Exception as e:
            context.log.warning(f"Failed to fetch info for {ticker}: {e}")
            failed.append(ticker)

    if not valid_rows:
        return MaterializeResult(metadata={"rows_written": 0, "failed_tickers": failed})

    s3_key = write_ticker_info_to_minio(valid_rows, snapshot_date, s3_client=s3.get_client())

    return MaterializeResult(
        metadata={
            "rows_written": len(valid_rows),
            "tickers_succeeded": len(valid_rows),
            "tickers_failed": failed,
            "snapshot_date": str(snapshot_date),
            "s3_key": MetadataValue.text(f"s3://{BRONZE_BUCKET}/{s3_key}"),
        }
    )
