"""
Tests for GrowthRadarDbtTranslator.

We only test the logic WE wrote :
  - sources : strip the 'source' prefix from asset keys
  - models  : group by folder under models/

We don't test the parent class's default behavior — that's dagster-dbt's
responsibility, and trying to mock it requires reconstructing the full
dbt resource dict (which is brittle across dagster-dbt versions).
"""

from dagster import AssetKey

from dagster_project.assets.dbt import GrowthRadarDbtTranslator


class TestGrowthRadarDbtTranslator:
    """Tests for the custom dbt translator."""

    def test_source_asset_key_strips_source_prefix(self):
        """
        For a dbt source, the AssetKey should be just the table name.
        This is what enables lineage matching with our Python bronze assets.
        """
        translator = GrowthRadarDbtTranslator()
        dbt_resource_props = {
            "resource_type": "source",
            "name": "yahoo_prices_daily_bronze",
            "source_name": "bronze",
        }

        asset_key = translator.get_asset_key(dbt_resource_props)

        assert asset_key == AssetKey("yahoo_prices_daily_bronze")

    def test_source_asset_key_for_each_bronze_table(self):
        """All our bronze sources should map cleanly."""
        translator = GrowthRadarDbtTranslator()
        bronze_tables = [
            "yahoo_prices_daily_bronze",
            "yahoo_prices_history_bronze",
            "yahoo_ticker_info_bronze",
        ]
        for table_name in bronze_tables:
            props = {
                "resource_type": "source",
                "name": table_name,
                "source_name": "bronze",
            }
            assert translator.get_asset_key(props) == AssetKey(table_name)

    def test_group_name_staging(self):
        """A model in models/staging/ should be in 'staging' group."""
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "staging", "stg_yahoo_prices"],
        }
        assert translator.get_group_name(props) == "staging"

    def test_group_name_marts(self):
        """A model in models/marts/* should be in 'marts' group."""
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "marts", "finance", "dim_instrument"],
        }
        assert translator.get_group_name(props) == "marts"

    def test_group_name_intermediate(self):
        """A model in models/intermediate/ should be in 'intermediate' group."""
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "intermediate", "int_prices_enriched"],
        }
        assert translator.get_group_name(props) == "intermediate"
