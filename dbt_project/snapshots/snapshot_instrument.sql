{% snapshot snapshot_instrument %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker_nk',
        strategy='check',
        check_cols=[
            'instrument_type',
            'long_name',
            'exchange',
            'currency',
            'country_hq',
            'yahoo_sector',
            'industry_group',
            'yahoo_industry',
            'sub_industry',
            'etf_category',
        ],
        invalidate_hard_deletes=True,
    )
}}

select
    ticker_nk,
    instrument_type,
    long_name,
    short_name,
    exchange,
    currency,
    country_hq,
    yahoo_sector,
    industry_group,
    yahoo_industry,
    sub_industry,
    etf_category,
    source_fetched_at
from {{ ref('stg_yahoo_ticker_info') }}

{% endsnapshot %}
