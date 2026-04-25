"""Dagster definitions : single entry point."""

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import bronze
from dagster_project.resources.s3 import s3_resource_from_env
from dagster_project.schedules import (
    yahoo_prices_daily_job,
    yahoo_prices_daily_schedule,
    yahoo_prices_history_job,
)

# Discover all assets in the bronze module (and any future module added here)
all_assets = load_assets_from_modules([bronze])

defs = Definitions(
    assets=all_assets,
    jobs=[yahoo_prices_daily_job, yahoo_prices_history_job],
    schedules=[yahoo_prices_daily_schedule],
    resources={
        "s3": s3_resource_from_env(),
    },
)
