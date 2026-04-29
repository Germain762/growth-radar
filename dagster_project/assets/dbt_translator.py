"""
Custom DagsterDbtTranslator for the Growth Radar project.

Kept in its own module (separate from dbt_assets definition) so it can be
imported and tested without requiring a generated dbt manifest.
"""

from collections.abc import Mapping
from typing import Any

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator


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
        fqn = dbt_resource_props.get("fqn", [])
        if len(fqn) >= 2:
            return fqn[1]
        return super().get_group_name(dbt_resource_props)
