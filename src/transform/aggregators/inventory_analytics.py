"""
Métricas de salud de inventario.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing


class InventoryAnalyticsAggregator:
    """
    Agrega indicadores de stock y utilización de bodegas a partir de inventario enriquecido.
    """

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_inventory"
    )
    def stock_health_summary(self, enriched_inventory_df: pd.DataFrame) -> pd.DataFrame:
        """
        Resume la salud de stock contando total de registros, casos en low_stock y overstock,
        y calcula su proporción relativa para medir el riesgo agregado.

        Args:
            enriched_inventory_df: DataFrame de inventario enriquecido.

        Returns:
            DataFrame con resumen de salud de inventario.
        """
        total = len(enriched_inventory_df)
        low = enriched_inventory_df["is_low_stock"].sum()
        over = enriched_inventory_df["is_overstock"].sum()
        summary = pd.DataFrame(
            {
                "metric": ["total_items", "low_stock", "overstock"],
                "value": [total, low, over],
                "pct": [
                    1.0,
                    low / total if total else 0.0,
                    over / total if total else 0.0,
                ],
            }
        )
        return summary

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_inventory"
    )
    def low_stock_items(
        self, enriched_inventory_df: pd.DataFrame, top_n: int = 20
    ) -> pd.DataFrame:
        """
        Identifica ítems marcados como low_stock, calcula la brecha contra el nivel mínimo
        y devuelve el top ordenado por mayor déficit de unidades.

        Args:
            enriched_inventory_df: DataFrame de inventario enriquecido.
            top_n: Número de ítems a retornar (default: 20)

        Returns:
            DataFrame con ítems en low stock.
        """
        df = enriched_inventory_df.copy()
        df["stock_gap"] = (df["min_stock_level"] - df["quantity"]).clip(lower=0)
        filtered = df[df.get("is_low_stock", False)].copy()
        filtered = filtered.sort_values("stock_gap", ascending=False).head(top_n)
        cols = [
            "product_id",
            "product_name",
            "warehouse_id",
            "quantity",
            "min_stock_level",
            "stock_gap",
        ]
        return filtered[cols]

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_inventory"
    )
    def warehouse_utilization(
        self, enriched_inventory_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calcula unidades totales por bodega y se estima el ratio
        de utilización combinando ocupación y capacidad declarada por almacén.
        Se devuelve ordenado de menor a mayor ratio de utilización.

        Args:
            enriched_inventory_df: DataFrame de inventario enriquecido.

        Returns:
            DataFrame con utilización por bodega.
        """
        grouped = (
            enriched_inventory_df.groupby("warehouse_id")
            .agg(location=("location", "first"), total_units=("quantity", "sum"))
            .reset_index()
        )
        grouped = grouped.merge(
            enriched_inventory_df[["warehouse_id", "capacity_units"]].drop_duplicates(),
            on="warehouse_id",
            how="left",
        )
        grouped["utilization"] = grouped["total_units"] / grouped["capacity_units"]
        # Ordeno de menor a mayor utilización
        grouped = grouped.sort_values("utilization")
        return grouped
