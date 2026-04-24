"""Quick check : read Parquet from MinIO via DuckDB."""
import duckdb

con = duckdb.connect()

# Configurer DuckDB pour lire MinIO (S3-compatible)
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")

# Compter les lignes par ticker (scan toutes les partitions d'un coup !)
result = con.execute("""
    SELECT 
        ticker,
        COUNT(*) AS rows,
        MIN(price_date) AS first_date,
        MAX(price_date) AS last_date,
        ROUND(AVG(close_price), 2) AS avg_close
    FROM read_parquet('s3://bronze/yahoo_finance/prices/date=*/*.parquet', hive_partitioning=1)
    GROUP BY ticker
    ORDER BY ticker
""").fetchdf()

print(result.to_string(index=False))
