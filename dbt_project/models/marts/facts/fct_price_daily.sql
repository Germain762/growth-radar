{{ config(
    materialized='incremental',
    unique_key='price_sk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- =====================================================================
-- fct_price_daily — daily OHLCV facts with calculated returns
-- =====================================================================
-- Grain : one row per (instrument, trading day)
--
-- This is materialized as 'incremental' :
--   - First run     : full history loaded (~thousands of rows)
--   - Subsequent    : only new dates are appended
--   - Idempotent    : same (ticker, date) overwrites cleanly
--
-- The 'delete+insert' strategy : if a partition is rerun, dbt deletes
-- existing rows for that range first, then inserts new ones. This is
-- what makes the model truly idempotent.
-- =====================================================================

with enriched as (
    select * from {{ ref('int_prices_enriched') }}
)

select
    -- Surrogate key for the fact row : hash of (ticker, date)
    {{ dbt_utils.generate_surrogate_key(['ticker_nk', 'price_date']) }}  as price_sk,

    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['ticker_nk']) }} as instrument_sk,

    -- Natural keys (kept for direct querying / debug)
    ticker_nk,
    price_date,

    -- Date attributes (denormalized for query convenience)
    extract(year  from price_date)::int  as price_year,
    extract(month from price_date)::int  as price_month,

    -- OHLCV measures
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close,
    volume,

    -- Calculated returns
    return_1d,
    return_5d,
    return_21d,
    return_63d,
    return_252d,

    -- Risk / activity metrics
    volatility_21d,
    volume_ratio_21d,

    -- Lineage
    current_timestamp                                                   as dbt_loaded_at

from enriched

{% if is_incremental() %}
    -- On incremental runs, only process new dates
    -- (we re-process the last 60 days to allow late corrections)
    where price_date >= (
        select coalesce(max(price_date), '1900-01-01') - interval '60 days'
        from {{ this }}
    )
{% endif %}
