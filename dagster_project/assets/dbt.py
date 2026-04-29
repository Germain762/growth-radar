"""
dbt models exposed as Dagster assets via dagster-dbt.

A custom translator handles two things :
  1. Map dbt sources to existing Python asset keys (so lineage links up)
  2. Use the dbt schema folder name as the Dagster group name (so we get
     'staging' instead of 'default')
"""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from dagster_project.assets.dbt_translator import GrowthRadarDbtTranslator

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR.as_posix(),
)
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=GrowthRadarDbtTranslator(),
)
def all_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models materialized via 'dbt build' (run + test)."""
    yield from dbt.cli(["build"], context=context).stream()
