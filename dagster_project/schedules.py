"""
Schedules : trigger materializations on a recurring basis.
"""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

# Job that materializes the bronze yahoo_prices asset for the latest partition
yahoo_prices_daily_job = define_asset_job(
    name="yahoo_prices_daily_job",
    selection=AssetSelection.assets("yahoo_prices_daily_bronze"),
)

yahoo_prices_history_job = define_asset_job(
    name="yahoo_prices_history_job",
    selection=AssetSelection.assets("yahoo_prices_history_bronze"),
)

# Schedule : runs every day at 7:00 AM (after US market close + processing time)
yahoo_prices_daily_schedule = build_schedule_from_partitioned_job(
    job=yahoo_prices_daily_job,
    hour_of_day=7,
    minute_of_hour=0,
)

# ETF holdings : daily refresh of all 10 ETFs
etf_holdings_daily_job = define_asset_job(
    name="etf_holdings_daily_job",
    selection=AssetSelection.assets("etf_holdings_bronze"),
)

etf_holdings_daily_schedule = ScheduleDefinition(
    job=etf_holdings_daily_job,
    cron_schedule="30 7 * * 1-5",  # 7:30 AM, weekdays only
    name="etf_holdings_daily_schedule",
)
