"""
Tests for Dagster assets with mocked yfinance.

These tests are FAST (< 1s) because they don't hit the real API.
They complement the slow tests in test_dagster_assets.py.
"""

from contextlib import suppress
from unittest.mock import patch

import pandas as pd
import pytest
from dagster import materialize

from dagster_project.assets.bronze import yahoo_prices_history_bronze
from dagster_project.resources.s3 import S3Resource


@pytest.fixture
def mock_yfinance_history():
    """
    Mock yf.download to return a controlled, small DataFrame.

    This isolates the test from the real yfinance API : faster,
    deterministic, works offline.
    """
    fake_df = pd.DataFrame(
        {
            "Open": [145.0, 146.0],
            "High": [148.0, 149.0],
            "Low": [144.0, 145.0],
            "Close": [147.0, 148.0],
            "Adj Close": [147.0, 148.0],
            "Volume": [1_000_000, 1_100_000],
        },
        index=pd.DatetimeIndex(["2025-04-21", "2025-04-22"], name="Date"),
    )

    with patch("dagster_project.assets.bronze.yf.download", return_value=fake_df) as mock:
        yield mock


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
    """Ensure the 'bronze' bucket exists."""
    with suppress(s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket="bronze")
    yield "bronze"


def test_history_asset_with_mocked_yfinance(minio_container, bronze_bucket, mock_yfinance_history):
    """Materialize the asset without hitting the real yfinance API."""
    s3_resource = _test_s3_resource(minio_container)

    result = materialize(
        [yahoo_prices_history_bronze],
        partition_key="NVDA",
        resources={"s3": s3_resource},
    )

    assert result.success
    # Verify yfinance was called exactly once
    assert mock_yfinance_history.call_count == 1

    # Verify metadata reflects the mocked data (2 rows)
    materialization = result.asset_materializations_for_node("yahoo_prices_history_bronze")[0]
    assert materialization.metadata["rows_written"].value == 2
