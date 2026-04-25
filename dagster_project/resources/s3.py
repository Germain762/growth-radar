"""
S3-compatible client resource for Dagster.

Why a Dagster resource ? So all assets share the same configured client,
and we can swap implementations (real S3, mock for tests, MinIO for dev)
via configuration without touching asset code.
"""

import os

import boto3
from botocore.client import Config
from dagster import ConfigurableResource


class S3Resource(ConfigurableResource):
    """
    Configurable S3 client, defaults to local MinIO.

    Override via env vars or Dagster config :
        endpoint_url       (default : http://localhost:9000)
        access_key_id      (default : minioadmin)
        secret_access_key  (default : minioadmin)
    """

    endpoint_url: str = "http://localhost:9000"
    access_key_id: str = "minioadmin"
    secret_access_key: str = "minioadmin"
    region_name: str = "us-east-1"

    def get_client(self):
        """Return a boto3 S3 client configured for this resource."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=Config(signature_version="s3v4"),
            region_name=self.region_name,
        )


def s3_resource_from_env() -> S3Resource:
    """Factory : build an S3Resource from environment variables."""
    return S3Resource(
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )
