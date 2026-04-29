# tests/integration/test_dagster_ticker_info_mocked.py
"""
Test for yahoo_ticker_info_bronze asset with mocked yfinance.
"""

from contextlib import suppress
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from dagster import materialize

from dagster_project.assets.bronze import yahoo_ticker_info_bronze
from dagster_project.resources.s3 import S3Resource


def _test_s3_resource(minio_container) -> S3Resource:
    """Build an S3Resource pointed at the test MinIO container."""
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    return S3Resource(
        endpoint_url=f"http://{host}:{port}",
        access_key_id=minio_container.access_key,
        secret_access_key=minio_container.secret_key,
    )


@pytest.fixture
def bronze_bucket(s3_client):
    """Ensure the 'bronze' bucket exists for these tests."""
    with suppress(s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket="bronze")
    yield "bronze"


@pytest.fixture
def mock_ticker_info():
    """Mock fetch_ticker_info to return controlled data without hitting yfinance."""

    def fake_fetch(ticker: str) -> dict:
        return {
            "ticker": ticker,
            "instrument_type": "equity" if not ticker.startswith("^") else "index",
            "long_name": f"{ticker} Corp" if not ticker.startswith("^") else ticker,
            "short_name": ticker,
            "exchange": "NMS",
            "currency": "USD",
            "country_hq": "United States" if not ticker.startswith("^") else None,
            "gics_sector": "Technology" if not ticker.startswith("^") else None,
            "gics_industry_group": None,
            "gics_industry": "Semiconductors" if not ticker.startswith("^") else None,
            "gics_sub_industry": None,
            "etf_category": None,
            "fetched_at": datetime.now(UTC),
        }

    with patch(
        "ingestion.sources.yahoo_ticker_info.fetch_ticker_info",
        side_effect=fake_fetch,
    ) as mock:
        yield mock


def test_ticker_info_asset_writes_parquet(
    minio_container, bronze_bucket, mock_ticker_info, s3_client
):
    """
    Materialize the asset with mocked yfinance, verify Parquet is written
    to the test MinIO via the injected S3Resource.
    """
    s3_resource = _test_s3_resource(minio_container)

    result = materialize(
        [yahoo_ticker_info_bronze],
        resources={"s3": s3_resource},
    )

    assert result.success

    # Verify metadata reflects what was written
    materialization = result.asset_materializations_for_node("yahoo_ticker_info_bronze")[0]
    assert materialization.metadata["rows_written"].value > 0

    # Verify the Parquet file exists in the TEST MinIO (not dev)
    objects = s3_client.list_objects_v2(
        Bucket="bronze",
        Prefix="yahoo_finance/ticker_info/",
    )
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert len(keys) >= 1, (
        "No Parquet file found in test MinIO. "
        "This usually means the asset wrote to dev MinIO instead, "
        "check that S3Resource is properly injected."
    )
    assert any("snapshot_date=" in k for k in keys)
    assert any(k.endswith(".parquet") for k in keys)
