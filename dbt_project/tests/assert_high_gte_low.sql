-- Custom test : high_price must always be >= low_price.
-- Returns rows where this is violated. Test passes if empty.

select
    ticker_nk,
    price_date,
    high_price,
    low_price
from {{ ref('stg_yahoo_prices') }}
where high_price < low_price
