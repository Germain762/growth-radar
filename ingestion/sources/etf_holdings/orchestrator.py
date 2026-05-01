"""
Orchestrator for ETF holdings fetching.

Dispatches each ETF ticker to the appropriate issuer-specific fetcher,
and writes the result as a Parquet file to MinIO bronze.

This is the entry point used by the Dagster asset.
"""

from datetime import date
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pydantic import ValidationError

from ingestion.common.s3_client import get_s3_client
from ingestion.schemas import EtfHolding
from ingestion.sources.etf_holdings.ark import ArkHoldingsFetcher
from ingestion.sources.etf_holdings.base import EtfHoldingsFetcher
from ingestion.sources.etf_holdings.ishares import IsharesHoldingsFetcher

log = structlog.get_logger()

BRONZE_BUCKET = "bronze"
SOURCE_PREFIX = "etf_holdings"


# Registered fetchers (extend as we add more issuers)
def _build_registry() -> list[EtfHoldingsFetcher]:
    return [
        ArkHoldingsFetcher(),
        IsharesHoldingsFetcher(),
        # Future : VanEck, GlobalX, FirstTrust, KraneShares
    ]


def find_fetcher(etf_ticker: str) -> EtfHoldingsFetcher | None:
    """Return the fetcher that supports this ticker, or None if unsupported."""
    for fetcher in _build_registry():
        if fetcher.supports(etf_ticker):
            return fetcher
    return None


def fetch_etf_holdings(
    etf_ticker: str,
    composition_date: date | None = None,
) -> list[dict]:
    """
    Fetch holdings for a given ETF, dispatching to the right issuer fetcher.

    Returns a list of validated dict rows (ready for Parquet writing).
    """
    fetcher = find_fetcher(etf_ticker)
    if fetcher is None:
        raise ValueError(f"No fetcher registered for ETF ticker {etf_ticker}")

    log.info(
        "etf_dispatch",
        etf=etf_ticker,
        issuer=fetcher.issuer_name,
    )

    raw_rows = fetcher.fetch(etf_ticker, composition_date=composition_date)

    # Validate everything via Pydantic at the boundary
    valid_rows: list[dict] = []
    failed = 0
    for row in raw_rows:
        try:
            valid_rows.append(EtfHolding.model_validate(row).model_dump())
        except ValidationError as e:
            log.warning(
                "etf_validation_failed",
                etf=etf_ticker,
                company=row.get("company_ticker"),
                error=str(e)[:200],
            )
            failed += 1

    log.info(
        "etf_fetch_complete",
        etf=etf_ticker,
        valid=len(valid_rows),
        failed=failed,
    )
    return valid_rows


def write_holdings_to_minio(
    rows: list[dict],
    etf_ticker: str,
    composition_date: date,
    s3_client=None,
) -> str:
    """
    Write a list of holdings as Parquet to MinIO.

    Path : s3://bronze/etf_holdings/etf_ticker=XXX/composition_date=YYYY-MM-DD/part.parquet
    """
    if not rows:
        log.warning("etf_no_rows_to_write", etf=etf_ticker)
        return ""

    table = pa.Table.from_pylist(rows)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = (
        f"{SOURCE_PREFIX}"
        f"/etf_ticker={etf_ticker.upper()}"
        f"/composition_date={composition_date.isoformat()}"
        f"/part.parquet"
    )

    s3 = s3_client if s3_client is not None else get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )

    log.info(
        "etf_holdings_written",
        bucket=BRONZE_BUCKET,
        key=key,
        rows=len(rows),
    )
    return key
