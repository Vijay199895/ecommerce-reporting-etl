"""
Pruebas unitarias para InventoryEnricher.
Validan uniones con catálogos y banderas de stock.
"""

import pytest
import pytest_check as check

from transform.cleaners.inventory_cleaner import InventoryCleaner
from transform.enrichers.inventory_enricher import InventoryEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryEnricher:
    """Tests de enriquecimiento de inventario con productos y bodegas."""

    def test_enrich_should_join_catalogs_and_add_stock_flags_when_inventory_provided(
        self, inventory_enricher_inputs
    ):
        """Debe unir catálogos, añadir banderas de stock y metadatos de ubicación."""
        enricher = InventoryEnricher(cleaner=InventoryCleaner())

        enriched = enricher.enrich(
            inventory_df=inventory_enricher_inputs["inventory"],
            products_df=inventory_enricher_inputs["products"],
            warehouses_df=inventory_enricher_inputs["warehouses"],
        )

        check.equal(len(enriched), 1)
        check.is_true("is_low_stock" in enriched.columns)
        check.is_true("product_name" in enriched.columns)
        check.is_true("location" in enriched.columns)
