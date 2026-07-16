{{ config(materialized='view') }}

-- Union the two bronze sources for prices :
--  - yahoo_prices_history : per-ticker partitioned, used for backfill
--  - yahoo_prices_daily   : per-day partitioned, used for incremental
-- Both should produce the same data shape.
-- Deduplication on (ticker, price_date) — keeping the most recent file
-- via the file path (not perfect but sufficient for now).

-- Reference dbt sources via  source('schema', 'table') so dbt builds the lineage graph
-- The actual data is read via read_parquet() since we don't use external_location plugin
-- The  source('schema', 'table') call is essentially a no-op SELECT but it tells dbt :
-- "this model depends on these sources" → enables lineage in dbt + Dagster

with history_source_check as (
    -- This SELECT exists only to declare the dependency for dbt's lineage.
    -- It's never read because the LIMIT 0 returns nothing.
    select 1 from {{ source('bronze', 'yahoo_prices_history_bronze') }} limit 0
),

daily_source_check as (
    select 1 from {{ source('bronze', 'yahoo_prices_daily_bronze') }} limit 0
),

history as (
    select * from read_parquet(
        's3://bronze/yahoo_finance/prices/date=*/ticker=*.parquet',
        hive_partitioning = 1
    )
),

daily as (
    select * from read_parquet(
        's3://bronze/yahoo_finance/prices/date=*/part_daily.parquet',
        hive_partitioning = 1
    )
),

unioned as (
    select * from history
    union all by name
    select * from daily
)

select distinct on (ticker, price_date)
    cast(ticker            as varchar) as ticker_nk,
    cast(price_date        as date)    as price_date,
    cast(open_price        as double)  as open_price,
    cast(high_price        as double)  as high_price,
    cast(low_price         as double)  as low_price,
    cast(close_price       as double)  as close_price,
    cast(adj_close         as double)  as adj_close,
    cast(volume            as bigint)  as volume,

    current_timestamp                  as dbt_loaded_at

from unioned
where price_date < (current_date at time zone 'America/New_York')
order by ticker, price_date, volume desc
