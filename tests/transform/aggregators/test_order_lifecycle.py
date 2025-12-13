"""
Pruebas unitarias para OrderLifecycleAggregator.
Evalúan embudos de estado, tasas de cancelación y backlogs en progreso.
"""

import pytest
import pytest_check as check

from transform.aggregators.order_lifecycle import OrderLifecycleAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestOrderLifecycleStatus:
    """Tests de métricas de estado: funnel, cancelaciones y entregas."""

    def test_status_funnel_should_sum_orders_and_shares_when_statuses_provided(
        self, orders_status_sample
    ):
        """Debe generar un funnel que sume las órdenes y proporciones totales."""
        agg = OrderLifecycleAggregator()

        funnel = agg.status_funnel(orders_status_sample)

        check.equal(funnel["orders"].sum(), len(orders_status_sample))
        check.almost_equal(funnel["share"].sum(), 1.0, rel=1e-3)

    def test_cancellation_rate_should_compute_ratio_when_statuses_provided(
        self, orders_status_sample
    ):
        """Debe calcular la tasa de cancelación sobre el total de órdenes."""
        agg = OrderLifecycleAggregator()

        rate = agg.cancellation_rate(orders_status_sample)

        check.almost_equal(rate, 1 / 6, rel=1e-3)

    def test_delivery_rate_should_compute_ratio_when_statuses_provided(
        self, orders_status_sample
    ):
        """Debe calcular la tasa de entrega sobre el total de órdenes."""
        agg = OrderLifecycleAggregator()

        rate = agg.delivery_rate(orders_status_sample)

        check.almost_equal(rate, 2 / 6, rel=1e-3)


@pytest.mark.unit
@pytest.mark.transform
class TestOrderLifecycleBacklog:
    """Tests de backlog en progreso por estado pendiente o en tránsito."""

    def test_in_progress_backlog_should_sum_orders_and_value_when_statuses_provided(
        self, orders_status_sample
    ):
        """Debe sumar órdenes y valor en backlog para estados en progreso."""
        agg = OrderLifecycleAggregator()

        backlog = agg.in_progress_backlog(orders_status_sample)

        check.equal(backlog["backlog_orders"].sum(), 3)
        check.equal(backlog["backlog_value"].sum(), 100 + 120 + 80)
