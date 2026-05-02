"""Dagster definitions : single entry point."""

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project.assets import bronze, etf
from dagster_project.assets import dbt as dbt_assets_module
from dagster_project.assets.dbt import dbt_project
from dagster_project.resources.s3 import s3_resource_from_env
from dagster_project.schedules import (
    etf_holdings_daily_job,
    etf_holdings_daily_schedule,
    yahoo_prices_daily_job,
    yahoo_prices_daily_schedule,
    yahoo_prices_history_job,
)

# Discover all assets in the bronze module (and any future module added here)
bronze_assets = load_assets_from_modules([bronze])
etf_assets = load_assets_from_modules([etf])
dbt_assets_collection = load_assets_from_modules([dbt_assets_module])

defs = Definitions(
    assets=[*bronze_assets, *etf_assets, *dbt_assets_collection],
    jobs=[
        yahoo_prices_daily_job,
        yahoo_prices_history_job,
        etf_holdings_daily_job,
    ],
    schedules=[
        yahoo_prices_daily_schedule,
        etf_holdings_daily_schedule,
    ],
    resources={
        "s3": s3_resource_from_env(),
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir),
    },
)
