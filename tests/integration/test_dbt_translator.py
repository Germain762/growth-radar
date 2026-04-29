# tests/integration/test_dbt_translator.py
"""
Tests for GrowthRadarDbtTranslator.

Imports from dbt_translator (not dbt) so the test doesn't depend on a
generated dbt manifest. This separation enables fast unit-style tests
on the translator logic.
"""

from dagster import AssetKey

from dagster_project.assets.dbt_translator import GrowthRadarDbtTranslator  # ← changé


class TestGrowthRadarDbtTranslator:
    """Tests for the custom dbt translator."""

    def test_source_asset_key_strips_source_prefix(self):
        translator = GrowthRadarDbtTranslator()
        dbt_resource_props = {
            "resource_type": "source",
            "name": "yahoo_prices_daily_bronze",
            "source_name": "bronze",
        }
        asset_key = translator.get_asset_key(dbt_resource_props)
        assert asset_key == AssetKey("yahoo_prices_daily_bronze")

    def test_source_asset_key_for_each_bronze_table(self):
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
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "staging", "stg_yahoo_prices"],
        }
        assert translator.get_group_name(props) == "staging"

    def test_group_name_marts(self):
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "marts", "finance", "dim_instrument"],
        }
        assert translator.get_group_name(props) == "marts"

    def test_group_name_intermediate(self):
        translator = GrowthRadarDbtTranslator()
        props = {
            "resource_type": "model",
            "fqn": ["growth_radar", "intermediate", "int_prices_enriched"],
        }
        assert translator.get_group_name(props) == "intermediate"
