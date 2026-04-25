"""
Pytest fixtures shared across all tests.

Why testcontainers ? It spins up a real MinIO Docker container for the
duration of the test session. Tests run against a fresh, isolated bucket,
not your dev MinIO. No risk of polluting your dev data.
"""

import boto3
import pytest
from botocore.client import Config
from testcontainers.minio import MinioContainer


@pytest.fixture(scope="session")
def minio_container():
    """Start a MinIO container for the test session."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="session")
def s3_client(minio_container):
    """A boto3 client pointed at the test MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{minio_container.get_container_host_ip()}:"
        f"{minio_container.get_exposed_port(9000)}",
        aws_access_key_id=minio_container.access_key,
        aws_secret_access_key=minio_container.secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@pytest.fixture(scope="function")
def fresh_bucket(s3_client):
    """
    Create a unique bucket per test, clean it up after.

    Each test gets a clean state — no test pollution.
    """
    import uuid

    bucket_name = f"test-{uuid.uuid4().hex[:8]}"
    s3_client.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup : delete all objects then the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get("Contents", []):
        s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3_client.delete_bucket(Bucket=bucket_name)
