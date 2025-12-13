"""
Métricas de salud de inventario.
"""

import pandas as pd

from utils.logger import transform_logger


class InventoryAnalyticsAggregator:
    """
    Agrega indicadores de stock y utilización de bodegas a partir de inventario enriquecido.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def stock_health_summary(self, inventory_df: pd.DataFrame) -> pd.DataFrame:
        """
        Resume la salud de stock contando total de registros, casos en low_stock y overstock,
        y calcula su proporción relativa para medir el riesgo agregado.
        """
        total = len(inventory_df)
        low = inventory_df.get("is_low_stock", pd.Series(dtype=bool)).sum()
        over = inventory_df.get("is_overstock", pd.Series(dtype=bool)).sum()
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
        self.logger.info(
            "Salud de inventario: %s low, %s overstock de %s", low, over, total
        )
        return summary

    def low_stock_items(
        self, inventory_df: pd.DataFrame, top_n: int = 20
    ) -> pd.DataFrame:
        """
        Identifica ítems marcados como low_stock, calcula la brecha contra el nivel mínimo
        y devuelve el top ordenado por mayor déficit de unidades.
        """
        df = inventory_df.copy()
        if "min_stock_level" in df.columns and "quantity" in df.columns:
            df["stock_gap"] = (df["min_stock_level"] - df["quantity"]).clip(lower=0)
        else:
            df["stock_gap"] = 0
        filtered = df[df.get("is_low_stock", False)].copy()
        filtered = filtered.sort_values("stock_gap", ascending=False).head(top_n)
        cols = [
            c
            for c in [
                "product_id",
                "product_name",
                "warehouse_id",
                "quantity",
                "min_stock_level",
                "stock_gap",
            ]
            if c in filtered.columns
        ]
        self.logger.info("Low stock listado generado: %s filas", len(filtered))
        return filtered[cols]

    def warehouse_utilization(self, inventory_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula unidades totales por bodega y, si se dispone de capacidad, estima el ratio
        de utilización combinando ocupación y capacidad declarada por almacén.
        """
        group_cols = [
            c for c in ["warehouse_id", "location"] if c in inventory_df.columns
        ]
        if not group_cols:
            group_cols = (
                ["warehouse_id"] if "warehouse_id" in inventory_df.columns else []
            )
        grouped = (
            inventory_df.groupby(group_cols)
            .agg(total_units=("quantity", "sum"))
            .reset_index()
        )
        if "capacity_units" in inventory_df.columns:
            cap = inventory_df[group_cols + ["capacity_units"]].drop_duplicates(
                subset=group_cols
            )
            grouped = grouped.merge(cap, on=group_cols, how="left")
            grouped["utilization"] = grouped["total_units"] / grouped[
                "capacity_units"
            ].replace({0: pd.NA})
        self.logger.info("Utilización por bodega calculada: %s filas", len(grouped))
        return grouped
