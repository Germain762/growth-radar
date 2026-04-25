"""
Integration tests : write Parquet to MinIO, read it back, verify data.

These tests catch :
  - Type coercion bugs (e.g., date stored as str)
  - Partition path bugs
  - Compression / serialization issues
"""

import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq


def test_write_and_read_parquet(s3_client, fresh_bucket):
    """Round-trip : write a small Parquet, read it back, verify content."""
    # Arrange
    rows = [
        {
            "ticker": "NVDA",
            "price_date": date(2025, 4, 23),
            "open_price": 145.23,
            "volume": 52_000_000,
        },
        {
            "ticker": "AAPL",
            "price_date": date(2025, 4, 23),
            "open_price": 168.50,
            "volume": 35_000_000,
        },
    ]
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = "yahoo_finance/prices/date=2025-04-23/part.parquet"

    # Act
    s3_client.put_object(Bucket=fresh_bucket, Key=key, Body=buf.getvalue())

    # Assert : read back via S3
    obj = s3_client.get_object(Bucket=fresh_bucket, Key=key)
    read_buf = io.BytesIO(obj["Body"].read())
    read_table = pq.read_table(read_buf)
    read_rows = read_table.to_pylist()

    assert len(read_rows) == 2
    assert read_rows[0]["ticker"] == "NVDA"
    assert read_rows[0]["price_date"] == date(2025, 4, 23)
    assert read_rows[0]["open_price"] == 145.23
    assert isinstance(read_rows[0]["volume"], int)


def test_partition_pruning_via_listing(s3_client, fresh_bucket):
    """
    Verify that Hive-style partitioning produces the expected key structure.
    This matters because dbt/DuckDB use this convention for partition pruning.
    """
    # Write 3 partitions
    for d in [date(2025, 4, 21), date(2025, 4, 22), date(2025, 4, 23)]:
        key = f"yahoo_finance/prices/date={d.isoformat()}/part.parquet"
        s3_client.put_object(Bucket=fresh_bucket, Key=key, Body=b"fake parquet")

    # List with a prefix that targets one specific partition
    response = s3_client.list_objects_v2(
        Bucket=fresh_bucket,
        Prefix="yahoo_finance/prices/date=2025-04-22/",
    )
    keys = [o["Key"] for o in response.get("Contents", [])]

    assert len(keys) == 1
    assert "date=2025-04-22" in keys[0]
