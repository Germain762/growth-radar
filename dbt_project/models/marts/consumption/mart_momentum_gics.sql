{{ config(materialized='table') }}

-- =====================================================================
-- mart_momentum_gics — sector momentum vs S&P 500 benchmark
-- =====================================================================
-- Business question :
--   "Which GICS sectors are outperforming/underperforming the market
--    over different time horizons (1W, 1M, 3M, 1Y) ?"
--
-- Grain : (gics_sector, snapshot_date)
-- Where snapshot_date is the latest trading day available in fct_price_daily.
--
-- Output :
--   - One row per GICS sector
--   - Average + median return over 5d, 21d, 63d, 252d
--   - Excess return vs S&P 500 (alpha)
--   - Number of constituents and average volatility
-- =====================================================================

with latest_date as (
    -- Reference date for the snapshot : last trading day available
    select max(price_date) as snapshot_date
    from {{ ref('fct_price_daily') }}
),

-- Step 1 : equities only, with their sector, on the latest date
equity_returns_latest as (
    select
        d.gics_sector,
        d.ticker_nk,
        f.price_date,
        f.close_price,
        f.return_5d,
        f.return_21d,
        f.return_63d,
        f.return_252d,
        f.volatility_21d,
        f.volume_ratio_21d
    from {{ ref('fct_price_daily') }} f
    inner join {{ ref('dim_instrument') }} d
        on f.instrument_sk = d.instrument_sk
    cross join latest_date l
    where f.price_date = l.snapshot_date
      and d.instrument_type = 'equity'
      and d.gics_sector is not null
),

-- Step 2 : aggregate by sector
sector_aggregates as (
    select
        gics_sector,
        count(distinct ticker_nk)                 as nb_constituents,

        -- Mean returns (simple average across constituents)
        avg(return_5d)                            as avg_return_5d,
        avg(return_21d)                           as avg_return_21d,
        avg(return_63d)                           as avg_return_63d,
        avg(return_252d)                          as avg_return_252d,

        -- Median returns (robust to outliers)
        median(return_5d)                         as median_return_5d,
        median(return_21d)                        as median_return_21d,
        median(return_63d)                        as median_return_63d,
        median(return_252d)                       as median_return_252d,

        -- Risk and activity
        avg(volatility_21d)                       as avg_volatility_21d,
        avg(volume_ratio_21d)                     as avg_volume_ratio_21d,

        -- Best / worst performers in the sector (over 21d)
        max(return_21d)                           as best_constituent_return_21d,
        min(return_21d)                           as worst_constituent_return_21d

    from equity_returns_latest
    group by gics_sector
),

-- Step 3 : the S&P 500 benchmark (as a separate row, not aggregated)
sp500_benchmark as (
    select
        f.return_5d   as sp500_return_5d,
        f.return_21d  as sp500_return_21d,
        f.return_63d  as sp500_return_63d,
        f.return_252d as sp500_return_252d
    from {{ ref('fct_price_daily') }} f
    cross join latest_date l
    where f.ticker_nk = '^GSPC'
      and f.price_date = l.snapshot_date
)

-- Final : join sector aggregates with benchmark for excess returns
select
    -- Snapshot reference
    l.snapshot_date,

    -- Sector identification
    s.gics_sector,
    s.nb_constituents,

    -- Mean returns
    s.avg_return_5d,
    s.avg_return_21d,
    s.avg_return_63d,
    s.avg_return_252d,

    -- Median returns
    s.median_return_5d,
    s.median_return_21d,
    s.median_return_63d,
    s.median_return_252d,

    -- Excess return vs S&P 500 (alpha) — the key metric
    s.avg_return_5d   - b.sp500_return_5d   as excess_return_5d,
    s.avg_return_21d  - b.sp500_return_21d  as excess_return_21d,
    s.avg_return_63d  - b.sp500_return_63d  as excess_return_63d,
    s.avg_return_252d - b.sp500_return_252d as excess_return_252d,

    -- Risk
    s.avg_volatility_21d,
    s.avg_volume_ratio_21d,

    -- Dispersion within the sector
    s.best_constituent_return_21d,
    s.worst_constituent_return_21d,
    s.best_constituent_return_21d
        - s.worst_constituent_return_21d   as dispersion_21d,

    -- Benchmark for context
    b.sp500_return_5d,
    b.sp500_return_21d,
    b.sp500_return_63d,
    b.sp500_return_252d,

    current_timestamp                       as dbt_loaded_at

from sector_aggregates s
cross join sp500_benchmark b
cross join latest_date l
order by excess_return_21d desc
