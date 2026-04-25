# tests/integration/test_dagster_assets.py
"""
Tests for Dagster assets : invoke them in isolation with mocked partitions.

Dagster provides materialize() helpers that simulate a real run.
"""

from contextlib import suppress

import pytest
from dagster import materialize

from dagster_project.assets.bronze import (
    yahoo_prices_history_bronze,
)
from dagster_project.resources.s3 import S3Resource


def _test_s3_resource(minio_container) -> S3Resource:
    """Build an S3Resource pointed at the test MinIO."""
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


@pytest.mark.slow
def test_history_asset_for_real_ticker(minio_container, bronze_bucket):
    """
    Materialize yahoo_prices_history_bronze for one ticker.

    This is a slow test : it really hits yfinance API.
    Marked @pytest.mark.slow so it can be skipped in fast CI runs.
    """
    s3_resource = _test_s3_resource(minio_container)

    result = materialize(
        [yahoo_prices_history_bronze],
        partition_key="AAPL",
        resources={"s3": s3_resource},
    )

    assert result.success
    # Inspect the materialization metadata
    materialization = result.asset_materializations_for_node("yahoo_prices_history_bronze")[0]
    rows = materialization.metadata["rows_written"].value
    assert rows > 0, "should have written some rows"
