{{ config(materialized='view') }}

-- Reference declaration for dbt lineage (the actual data is read via read_parquet)
with source_check as (
    select 1 from {{ source('bronze', 'yahoo_ticker_info_bronze') }} limit 0
),

raw as (
    select * from read_parquet(
        's3://bronze/yahoo_finance/ticker_info/snapshot_date=*/part.parquet',
        hive_partitioning = 1
    )
),

-- For each ticker, keep only the most recent snapshot
-- (we materialize ticker_info weekly, so we want the latest)
deduplicated as (
    select * from raw
    qualify row_number() over (
        partition by ticker
        order by fetched_at desc
    ) = 1
)

select
    cast(ticker              as varchar)  as ticker_nk,
    cast(instrument_type     as varchar)  as instrument_type,
    cast(long_name           as varchar)  as long_name,
    cast(short_name          as varchar)  as short_name,
    cast(exchange            as varchar)  as exchange,
    cast(currency            as varchar)  as currency,
    cast(country_hq          as varchar)  as country_hq,

    -- GICS classification (NULL for ETFs and indices)
    cast(gics_sector         as varchar)  as gics_sector,
    cast(gics_industry_group as varchar)  as gics_industry_group,
    cast(gics_industry       as varchar)  as gics_industry,
    cast(gics_sub_industry   as varchar)  as gics_sub_industry,

    -- ETF-specific fields
    cast(etf_category        as varchar)  as etf_category,

    -- Lineage / freshness fields
    cast(fetched_at          as timestamp) as source_fetched_at,
    cast(snapshot_date       as date)     as snapshot_date,

    current_timestamp                      as dbt_loaded_at

from deduplicated
