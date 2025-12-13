"""
Pruebas unitarias para OrdersEnricher.
Revisan uniones y campos derivados luego de limpiar pedidos.
"""

import pytest
import pytest_check as check

from transform.cleaners.orders_cleaner import OrdersCleaner
from transform.enrichers.orders_enricher import OrdersEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersEnricher:
    """Tests del enriquecimiento de pedidos con catálogos y métricas derivadas."""

    def test_enrich_should_join_catalogs_and_derive_metrics_when_orders_provided(
        self, orders_enricher_inputs
    ):
        """Debe unir catálogos, agregar metadatos y calcular columnas agregadas."""
        enricher = OrdersEnricher(cleaner=OrdersCleaner())

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
            products_df=orders_enricher_inputs["products"],
            categories_df=orders_enricher_inputs["categories"],
            brands_df=orders_enricher_inputs["brands"],
        )

        check.equal(len(enriched), 1)
        check.is_true("order_month" in enriched.columns)
        check.is_true("avg_item_price" in enriched.columns)
        check.is_true("product_name" in enriched.columns)
        check.is_true(enriched.loc[0, "used_promotion"])
