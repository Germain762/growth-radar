{{ config(materialized='ephemeral') }}

-- =====================================================================
-- int_prices_enriched
-- =====================================================================
-- Adds calculated metrics to staging prices :
--   - returns over 5 windows (1d, 5d, 21d, 63d, 252d) ≈ (1d, 1w, 1M, 3M, 1Y)
--   - rolling volatility 21d (std of daily returns)
--   - relative volume vs 21d moving average
--
-- IMPORTANT : returns are computed on adj_close (split/dividend adjusted),
-- NEVER on raw close. Otherwise a stock split looks like a -90% crash.
--
-- Materialized as 'ephemeral' : this CTE is inlined into downstream models.
-- No physical table, just SQL injection. Lighter than a view for transient logic.
-- =====================================================================

with prices as (
    select * from {{ ref('stg_yahoo_prices') }}
),

with_returns as (
    select
        ticker_nk,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close,
        volume,

        -- Returns : (today / N-days-ago) - 1
        -- LAG returns NULL when the window goes outside available data
        adj_close / nullif(lag(adj_close,   1) over w, 0) - 1   as return_1d,
        adj_close / nullif(lag(adj_close,   5) over w, 0) - 1   as return_5d,
        adj_close / nullif(lag(adj_close,  21) over w, 0) - 1   as return_21d,
        adj_close / nullif(lag(adj_close,  63) over w, 0) - 1   as return_63d,
        adj_close / nullif(lag(adj_close, 252) over w, 0) - 1   as return_252d,

        -- 21-day moving average of volume (denominator for volume_ratio)
        avg(volume) over (
            partition by ticker_nk
            order by price_date
            rows between 20 preceding and current row
        )                                                        as avg_volume_21d

    from prices
    window w as (partition by ticker_nk order by price_date)
),

with_volatility as (
    select
        *,
        -- 21-day rolling standard deviation of daily returns
        -- This is the daily volatility ; multiply by sqrt(252) for annualized
        stddev(return_1d) over (
            partition by ticker_nk
            order by price_date
            rows between 20 preceding and current row
        )                                                        as volatility_21d,

        -- Volume relative to its 21d moving average
        -- > 1 = high volume day, < 1 = low volume day
        case
            when avg_volume_21d > 0 then volume / avg_volume_21d
            else null
        end                                                      as volume_ratio_21d

    from with_returns
)

select
    ticker_nk,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close,
    volume,
    return_1d,
    return_5d,
    return_21d,
    return_63d,
    return_252d,
    volatility_21d,
    volume_ratio_21d
from with_volatility
