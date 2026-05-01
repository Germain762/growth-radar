-- Lister tous les schémas
SELECT DISTINCT table_schema
FROM information_schema.tables
ORDER BY table_schema;

-- Lister toutes les tables/vues, par schéma
SELECT
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'duckdb_temp_')
ORDER BY table_schema, table_name;

DESCRIBE fct_price_daily;

-- Vue d'ensemble de tout
SELECT 'dim_instrument' AS table_name, count(*) AS rows FROM dim_instrument
UNION ALL
SELECT 'fct_price_daily', count(*) FROM fct_price_daily
UNION ALL
SELECT 'stg_yahoo_prices', count(*) FROM stg_yahoo_prices
UNION ALL
SELECT 'snapshot_instrument', count(*) FROM snapshots.snapshot_instrument
UNION ALL
SELECT 'stg_yahoo_ticker_info', count(*) FROM stg_yahoo_ticker_info;


-- 5 lignes random pour voir à quoi ça ressemble
SELECT * FROM dim_instrument LIMIT 5;

-- Top performers du dernier jour
SELECT
    f.ticker_nk,
    d.long_name,
    d.gics_sector,
    f.price_date,
    round(f.close_price, 2) AS close,
    round(f.return_21d * 100, 2) AS perf_21d_pct,
    round(f.return_252d * 100, 2) AS perf_1y_pct
FROM fct_price_daily f
LEFT JOIN dim_instrument d ON f.instrument_sk = d.instrument_sk
WHERE f.price_date = (SELECT max(price_date) FROM fct_price_daily)
ORDER BY f.return_21d DESC NULLS LAST;
