"""
Schedules : trigger materializations on a recurring basis.
"""

from dagster import (
    AssetSelection,
    define_asset_job,
)

# ---------------------------------------------------------------
# DAILY pipeline : ingest the latest closed session + refresh
# everything downstream (staging → snapshot → dim → fact → marts).
#
# The `.downstream()` selection means this job automatically picks up
# any new dbt model we add later — no need to edit this list.
# ---------------------------------------------------------------
# Branche prix : partitionnée par date de séance
daily_prices_job = define_asset_job(
    name="daily_prices_job",
    selection=AssetSelection.assets("yahoo_prices_daily_bronze"),
)

# Branche ETF : partitionnée par ticker
daily_etf_job = define_asset_job(
    name="daily_etf_job",
    selection=AssetSelection.assets("etf_holdings_bronze"),
)

# Transformation dbt : non partitionnée, consomme les deux branches
daily_transform_job = define_asset_job(
    name="daily_transform_job",
    selection=(
        AssetSelection.assets("stg_yahoo_prices").downstream()
        | AssetSelection.assets("stg_yahoo_ticker_info").downstream()
    ),
)

# ---------------------------------------------------------------
# WEEKLY pipeline : refresh the expensive static metadata (Yahoo Finance sector/industry/country classification via yfinance .info), then let the SCD2 snapshot
# classification via yfinance .info), then let the SCD2 snapshot
# capture any reclassification.
# ---------------------------------------------------------------
weekly_refresh_job = define_asset_job(
    name="weekly_refresh_job",
    selection=AssetSelection.assets("yahoo_ticker_info_bronze").downstream(),
)
