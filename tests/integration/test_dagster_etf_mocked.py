"""
Integration test for etf_holdings_bronze asset with mocked fetchers.

We mock fetch_etf_holdings (the orchestrator entry point), so the test
runs without hitting any real ETF issuer API.
"""

from contextlib import suppress
from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest
from dagster import materialize

from dagster_project.assets.etf import etf_holdings_bronze
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
def mock_fetch_etf_holdings():
    """
    Mock fetch_etf_holdings to return controlled data.

    Returns 3 fake holdings for any ETF ticker, summing to 100%.
    """

    def fake_fetch(etf_ticker: str, composition_date=None) -> list[dict]:
        snap = composition_date or date.today()
        ts = datetime.now(UTC)
        return [
            {
                "etf_ticker": etf_ticker,
                "company_ticker": "TEST1",
                "company_name": "Test Company 1",
                "composition_date": snap,
                "weight_pct": 50.0,
                "shares_held": 1000.0,
                "market_value_usd": 100000.0,
                "issuer": "Mock Issuer",
                "fetched_at": ts,
            },
            {
                "etf_ticker": etf_ticker,
                "company_ticker": "TEST2",
                "company_name": "Test Company 2",
                "composition_date": snap,
                "weight_pct": 30.0,
                "shares_held": 500.0,
                "market_value_usd": 60000.0,
                "issuer": "Mock Issuer",
                "fetched_at": ts,
            },
            {
                "etf_ticker": etf_ticker,
                "company_ticker": "TEST3",
                "company_name": "Test Company 3",
                "composition_date": snap,
                "weight_pct": 20.0,
                "shares_held": 200.0,
                "market_value_usd": 40000.0,
                "issuer": "Mock Issuer",
                "fetched_at": ts,
            },
        ]

    with patch(
        "ingestion.sources.etf_holdings.orchestrator.fetch_etf_holdings",
        side_effect=fake_fetch,
    ) as mock:
        yield mock


def test_etf_holdings_asset_writes_parquet(
    minio_container, bronze_bucket, mock_fetch_etf_holdings, s3_client
):
    """
    Materialize etf_holdings_bronze for one partition (ARKK).
    Verify Parquet is written and metadata reflects the data.
    """
    s3_resource = _test_s3_resource(minio_container)

    result = materialize(
        [etf_holdings_bronze],
        partition_key="ARKK",
        resources={"s3": s3_resource},
    )

    assert result.success

    # Verify metadata
    materialization = result.asset_materializations_for_node("etf_holdings_bronze")[0]
    metadata = materialization.metadata
    assert metadata["holdings_count"].value == 3
    assert metadata["total_weight_pct"].value == 100.0
    assert metadata["etf_ticker"].value == "ARKK"

    # Verify the Parquet file exists in test MinIO
    objects = s3_client.list_objects_v2(
        Bucket="bronze",
        Prefix="etf_holdings/etf_ticker=ARKK/",
    )
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert len(keys) >= 1, "No Parquet file found in test MinIO"
    assert any("composition_date=" in k for k in keys)
    assert any(k.endswith(".parquet") for k in keys)
