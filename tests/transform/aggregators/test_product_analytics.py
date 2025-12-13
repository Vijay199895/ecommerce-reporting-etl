"""
Pruebas unitarias para ProductAnalyticsAggregator.
Revisan rankings por unidades vendidas y por ingresos con nombres de producto.
"""

import pytest
import pytest_check as check

from transform.aggregators.product_analytics import ProductAnalyticsAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestProductAnalyticsTopQuantity:
    """Tests de `top_products_by_quantity` con orden y enriquecimiento de nombres."""

    def test_top_products_by_quantity_should_return_ranked_with_names_when_items_and_catalog_provided(
        self, order_items_sample, products_catalog
    ):
        """Debe devolver top por unidades, ordenado y con `product_name` presente."""
        aggregator = ProductAnalyticsAggregator()

        result = aggregator.top_products_by_quantity(
            order_items_df=order_items_sample,
            products_df=products_catalog,
            top_n=2,
        )

        check.equal(len(result), 2)
        check.is_true("product_name" in result.columns)
        check.equal(result.iloc[0]["product_id"], 12)
        check.greater_equal(
            result.iloc[0]["total_units"], result.iloc[1]["total_units"]
        )


@pytest.mark.unit
@pytest.mark.transform
class TestProductAnalyticsTopRevenue:
    """Tests de `top_products_by_revenue` confirmando orden por ingresos."""

    def test_top_products_by_revenue_should_return_ranked_with_names_when_items_and_catalog_provided(
        self, order_items_sample, products_catalog
    ):
        """Debe retornar top por revenue, en orden descendente y con nombre de producto."""
        aggregator = ProductAnalyticsAggregator()

        result = aggregator.top_products_by_revenue(
            order_items_df=order_items_sample,
            products_df=products_catalog,
            top_n=3,
        )

        check.equal(len(result), 3)
        check.is_true("product_name" in result.columns)
        check.is_true(result["revenue"].is_monotonic_decreasing)
