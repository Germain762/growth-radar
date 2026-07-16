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
    -- Clé d'ENTITÉ : hash de ticker_nk, stable dans le temps.
    -- C'est la cible des FK de faits — elle ne change jamais pour un ticker.
    {{ dbt_utils.generate_surrogate_key(['ticker_nk']) }} as instrument_sk,

    -- Clé de VERSION : le dbt_scd_id du snapshot, change à chaque nouvelle
    -- version. Conservée pour l'analyse historique des attributs (SCD2).
    dbt_scd_id                                             as instrument_version_sk,

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

    -- Yahoo Finance classification
    yahoo_sector,
    industry_group,
    yahoo_industry,
    sub_industry,

    -- Concatenated Yahoo Finance path
    case
        when yahoo_sector is not null
        then yahoo_sector || ' / ' || coalesce(yahoo_industry, '?')
        else null
    end                                                    as yahoo_path,

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
