"""
Factory for S3-compatible clients (MinIO in dev, real S3 in prod).

Usage :
    from ingestion.common.s3_client import get_s3_client
    s3 = get_s3_client()
    s3.list_buckets()
"""

import os

import boto3
from botocore.client import Config


def get_s3_client():
    """
    Return a boto3 S3 client configured from env variables.

    Env vars read :
        MINIO_ENDPOINT       (default : http://localhost:9000)
        MINIO_ACCESS_KEY     (default : minioadmin)
        MINIO_SECRET_KEY     (default : minioadmin)

    Why env vars ? So we can point to a real S3 bucket in prod without
    changing the code.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
