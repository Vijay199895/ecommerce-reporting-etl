"""
Pruebas unitarias para InventoryAnalyticsAggregator.
Verifican salud de stock, items críticos y utilización por bodega.
"""

import pytest
import pytest_check as check

from transform.aggregators.inventory_analytics import InventoryAnalyticsAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryAnalyticsHealth:
    """Tests de `stock_health_summary` para métricas agregadas de inventario."""

    def test_stock_health_summary_should_compute_totals_when_inventory_sample_provided(
        self, inventory_sample
    ):
        """Debe calcular totales, bajo stock y sobrestock según los datos de prueba."""
        agg = InventoryAnalyticsAggregator()

        summary = agg.stock_health_summary(inventory_sample)

        check.equal(
            summary.loc[summary["metric"] == "total_items", "value"].iloc[0],
            len(inventory_sample),
        )
        check.equal(summary.loc[summary["metric"] == "low_stock", "value"].iloc[0], 1)
        check.equal(summary.loc[summary["metric"] == "overstock", "value"].iloc[0], 1)


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryAnalyticsLowStock:
    """Tests de `low_stock_items` priorizando brecha de stock."""

    def test_low_stock_items_should_sort_by_gap_when_inventory_sample_provided(
        self, inventory_sample
    ):
        """Debe listar productos con menor stock ordenados por la brecha descendente."""
        agg = InventoryAnalyticsAggregator()

        low = agg.low_stock_items(inventory_sample, top_n=5)

        check.greater(len(low), 0)
        check.is_true(low["stock_gap"].is_monotonic_decreasing)
        check.equal(low.iloc[0]["product_id"], 10)


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryAnalyticsWarehouseUtilization:
    """Tests de `warehouse_utilization` para capacidad y porcentaje por bodega."""

    def test_warehouse_utilization_should_include_capacity_when_inventory_sample_provided(
        self, inventory_sample
    ):
        """Debe incluir columna de utilización y una fila por cada bodega única."""
        agg = InventoryAnalyticsAggregator()

        util = agg.warehouse_utilization(inventory_sample)

        check.is_true("utilization" in util.columns)
        check.equal(util["warehouse_id"].nunique(), 3)
