"""
Pruebas unitarias para CustomerAnalyticsAggregator.
Evalúan rankings de clientes, recurrencia y ticket promedio.
"""

import pytest
import pytest_check as check

from transform.aggregators.customer_analytics import CustomerAnalyticsAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestCustomerAnalyticsTopSpenders:
    """Tests de `top_spenders` incluyendo percentil y enriquecimiento con email."""

    def test_top_spenders_should_apply_percentile_and_include_email_when_called_with_catalog(
        self, enriched_orders_sample
    ):
        """Debe ordenar por gasto, aplicar percentil y exponer emails en el resultado."""
        aggregator = CustomerAnalyticsAggregator()

        result = aggregator.top_spenders(
            enriched_orders_df=enriched_orders_sample, top_n=3, percentile=0.5
        )

        check.is_true("email" in result.columns)
        check.equal(len(result), 3)
        check.is_true(result["total_spent"].is_monotonic_decreasing)
        check.is_in("user1@example.com", result["email"].tolist())


@pytest.mark.unit
@pytest.mark.transform
class TestCustomerAnalyticsRecurring:
    """Tests de `recurring_customers` para clientes con órdenes mínimas."""

    def test_recurring_customers_should_filter_min_orders_and_include_email_when_catalog_joined(
        self, enriched_orders_sample
    ):
        """Debe filtrar clientes recurrentes, contar órdenes y añadir correo."""
        aggregator = CustomerAnalyticsAggregator()

        result = aggregator.recurring_customers(
            enriched_orders_df=enriched_orders_sample,
            min_orders=2,
        )

        check.is_true("email" in result.columns)
        check.is_true((result["total_orders"] >= 2).all())
        check.equal(result.iloc[0]["customer_id"], 1)


@pytest.mark.unit
@pytest.mark.transform
class TestCustomerAnalyticsAverageTicket:
    """Tests de `average_ticket_overall` para validar cálculo global."""

    def test_average_ticket_should_return_overall_mean_when_orders_provided(
        self, enriched_orders_sample
    ):
        """Debe devolver el ticket promedio calculado sobre todas las órdenes."""
        aggregator = CustomerAnalyticsAggregator()

        avg_ticket = aggregator.average_ticket_overall(enriched_orders_sample)

        check.almost_equal(avg_ticket, 120.0, rel=1e-3)
