"""
Pruebas unitarias para SalesAnalyticsAggregator.
Verifican ventas mensuales y métricas de promociones aplicadas.
"""

import pytest
import pytest_check as check

from transform.aggregators.sales_analytics import SalesAnalyticsAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestSalesAnalyticsMonthly:
    """Tests de `monthly_sales` para agregaciones por periodo y agrupaciones opcionales."""

    def test_monthly_sales_should_derive_period_and_sort_when_orders_provided(
        self, enriched_orders_sample
    ):
        """Debe derivar el mes de orden, ordenar y sumar órdenes totales."""
        aggregator = SalesAnalyticsAggregator()

        result = aggregator.monthly_sales(enriched_orders_df=enriched_orders_sample)

        check.is_false(result["order_month"].isnull().any())
        check.is_true(result["order_month"].is_monotonic_increasing)
        check.equal(result["orders"].sum(), len(enriched_orders_sample))


@pytest.mark.unit
@pytest.mark.transform
class TestSalesAnalyticsPromotions:
    """Tests de `promotion_usage_rate` para proporción de órdenes con promoción."""

    def test_promotion_usage_rate_should_compute_ratio_when_orders_provided(
        self, enriched_orders_sample
    ):
        """Debe calcular la tasa de uso de promociones sobre el dataset enriquecido."""
        aggregator = SalesAnalyticsAggregator()

        rate = aggregator.promotion_usage_rate(enriched_orders_sample)

        check.almost_equal(rate, 0.4, rel=1e-3)
