"""
Bronze layer assets for ETF holdings.

One asset per ETF (StaticPartitionsDefinition over the watchlist),
each partition fetches the latest holdings from the appropriate issuer
and writes a Parquet file to MinIO.

Path : s3://bronze/etf_holdings/etf_ticker=XXX/composition_date=YYYY-MM-DD/part.parquet
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from dagster_project.resources.s3 import S3Resource

# ---- Watchlist of ETFs loaded once at module import ----
ETFS_PATH = Path("ingestion/config/etfs_week3.csv")
ETFS_DF = pd.read_csv(ETFS_PATH)
ETF_TICKERS: list[str] = ETFS_DF["ticker"].tolist()

# ---- Partition by ETF ticker ----
etf_partitions = StaticPartitionsDefinition(ETF_TICKERS)


@asset(
    name="etf_holdings_bronze",
    description=(
        "Daily snapshot of holdings for each thematic ETF. "
        "One partition per ETF; each materialization fetches the latest "
        "composition from the issuer (iShares, ARK, Global X, etc.)."
    ),
    group_name="bronze",
    partitions_def=etf_partitions,
    compute_kind="python",
    pool="bronze",
)
def etf_holdings_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    """Fetch and store holdings for one ETF (one partition)."""
    from ingestion.sources.etf_holdings.orchestrator import (
        BRONZE_BUCKET,
        fetch_etf_holdings,
        find_fetcher,
        write_holdings_to_minio,
    )

    etf_ticker = context.partition_key
    composition_date = datetime.now(UTC).date()

    # Verify we have a fetcher registered for this ticker
    fetcher = find_fetcher(etf_ticker)
    if fetcher is None:
        context.log.warning(f"No fetcher registered for {etf_ticker}, skipping")
        return MaterializeResult(
            metadata={
                "rows_written": 0,
                "skipped_reason": "no_fetcher_registered",
            }
        )

    context.log.info(f"Fetching {etf_ticker} via {fetcher.issuer_name}")

    try:
        valid_rows = fetch_etf_holdings(etf_ticker, composition_date)
    except Exception as e:
        context.log.error(f"Failed to fetch {etf_ticker}: {e}")
        raise

    if not valid_rows:
        return MaterializeResult(
            metadata={
                "rows_written": 0,
                "etf_ticker": etf_ticker,
                "issuer": fetcher.issuer_name,
            }
        )

    s3_key = write_holdings_to_minio(
        valid_rows,
        etf_ticker,
        composition_date,
        s3_client=s3.get_client(),
    )

    # Sanity metric : sum of weights should be ~100%
    total_weight = sum(r["weight_pct"] for r in valid_rows)

    return MaterializeResult(
        metadata={
            "etf_ticker": etf_ticker,
            "issuer": fetcher.issuer_name,
            "composition_date": str(composition_date),
            "holdings_count": len(valid_rows),
            "total_weight_pct": round(total_weight, 2),
            "s3_key": MetadataValue.text(f"s3://{BRONZE_BUCKET}/{s3_key}"),
            "weight_sanity": (
                "✅ within range"
                if 80 <= total_weight <= 105
                else f"⚠️ unusual : {total_weight:.1f}%"
            ),
        }
    )
