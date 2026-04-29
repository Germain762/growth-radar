-- Init script for the growth-radar DuckDB shell.
-- Loaded automatically with : duckdb -init .duckdb_init.sql growth_radar.duckdb
-- (Or via 'make duckdb-shell' once we have a Makefile.)

INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_url_style='path';
SET s3_use_ssl=false;
