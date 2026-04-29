"""
dbt models exposed as Dagster assets via dagster-dbt.

A custom translator handles two things :
  1. Map dbt sources to existing Python asset keys (so lineage links up)
  2. Use the dbt schema folder name as the Dagster group name (so we get
     'staging' instead of 'default')
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR.as_posix(),
)
dbt_project.prepare_if_dev()


class GrowthRadarDbtTranslator(DagsterDbtTranslator):
    """
    Custom translator to align dbt asset keys with our Dagster conventions.

    By default, dagster-dbt builds keys like ['source', 'bronze', 'table_name'].
    We want sources to share the SAME asset key as the upstream Python asset,
    so the lineage graph connects automatically.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """
        Map a dbt resource (model, source, seed) to a Dagster AssetKey.

        For dbt sources : use just the table name (matching our Python asset).
        For everything else : use the dbt default behavior.
        """
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            # Match the upstream Python asset by name, ignoring the source group
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """
        Group dbt models by their folder under models/ rather than 'default'.

        Convention :
            models/staging/*       → group 'staging'
            models/intermediate/*  → group 'intermediate'
            models/marts/*         → group 'marts'
        """
        # The 'fqn' (fully-qualified name) is something like
        # ['growth_radar', 'staging', 'stg_yahoo_prices']
        fqn = dbt_resource_props.get("fqn", [])
        if len(fqn) >= 2:
            return fqn[1]  # the folder right under models/
        return super().get_group_name(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=GrowthRadarDbtTranslator(),
)
def all_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models materialized via 'dbt build' (run + test)."""
    yield from dbt.cli(["build"], context=context).stream()
