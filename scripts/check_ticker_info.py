import duckdb

con = duckdb.connect()
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")

result = con.execute("""
    SELECT
        ticker,
        instrument_type,
        long_name,
        yahoo_sector,
        yahoo_industry,
        country_hq
    FROM read_parquet('s3://bronze/yahoo_finance/ticker_info/snapshot_date=*/*.parquet', hive_partitioning=1)
    ORDER BY instrument_type, ticker
""").fetchdf()

print(result.to_string(index=False))
