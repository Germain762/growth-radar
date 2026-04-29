{{ config(materialized='table') }}

-- =====================================================================
-- dim_instrument — current version of each instrument (SCD2 source)
-- =====================================================================
-- Reads from snapshot_instrument and exposes only the latest version
-- of each ticker (where dbt_valid_to IS NULL).
--
-- For historical analysis, query snapshot_instrument directly or build
-- a dim_instrument_historical view exposing all versions.
-- =====================================================================

with snapshot as (
    select * from {{ ref('snapshot_instrument') }}
    where dbt_valid_to is null   -- only the current version
)

select
    -- Surrogate key including version (deterministic per version)
    -- This is dbt_scd_id from the snapshot — already a unique hash per version
    dbt_scd_id                                             as instrument_sk,

    -- Natural key
    ticker_nk,

    -- Type discrimination
    instrument_type,

    -- Common attributes
    long_name,
    short_name,
    exchange,
    currency,
    country_hq,

    -- GICS classification
    gics_sector,
    gics_industry_group,
    gics_industry,
    gics_sub_industry,

    -- Concatenated GICS path
    case
        when gics_sector is not null
        then gics_sector || ' / ' || coalesce(gics_industry, '?')
        else null
    end                                                    as gics_path,

    -- ETF-specific
    etf_category,

    -- SCD2 metadata (always current here, but exposed for downstream)
    true                                                   as is_current,
    dbt_valid_from                                         as valid_from,
    dbt_valid_to                                           as valid_to,

    -- Lineage
    source_fetched_at,
    current_timestamp                                      as dbt_loaded_at

from snapshot
