"""Dagster definitions : single entry point."""

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project.assets import bronze, etf, spark
from dagster_project.assets import dbt as dbt_assets_module
from dagster_project.assets.dbt import dbt_project
from dagster_project.resources.s3 import s3_resource_from_env
from dagster_project.schedules import (
    daily_etf_job,
    daily_prices_job,
    daily_transform_job,
    weekly_refresh_job,
)

# Discover all assets in the bronze module (and any future module added here)
bronze_assets = load_assets_from_modules([bronze])
etf_assets = load_assets_from_modules([etf])
spark_assets = load_assets_from_modules([spark])
dbt_assets_collection = load_assets_from_modules([dbt_assets_module])

defs = Definitions(
    assets=[*bronze_assets, *etf_assets, *spark_assets, *dbt_assets_collection],
    jobs=[
        daily_prices_job,
        daily_etf_job,
        daily_transform_job,
        weekly_refresh_job,
    ],
    schedules=[],
    resources={
        "s3": s3_resource_from_env(),
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir),
    },
)
