"""
Agregaciones de ventas y evolución temporal.
"""

import pandas as pd

from utils.logger import transform_logger


class SalesAnalyticsAggregator:
    """
    Calcula métricas temporales de ventas y uso de promociones sobre órdenes enriquecidas.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def monthly_sales(self, enriched_orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega ingresos y conteo de órdenes por mes; si falta `order_month`, lo deriva de
        `order_date`, y devuelve la serie temporal ordenada.
        """
        if (
            "order_month" not in enriched_orders_df.columns
            and "order_date" in enriched_orders_df.columns
        ):
            enriched_orders_df = enriched_orders_df.copy()
            enriched_orders_df["order_month"] = enriched_orders_df[
                "order_date"
            ].dt.to_period("M")

        grouped = (
            enriched_orders_df.groupby("order_month")
            .agg(total_revenue=("total_amount", "sum"), orders=("order_id", "count"))
            .reset_index()
            .sort_values("order_month")
        )
        self.logger.info("Ventas mensuales calculadas: %s periodos", len(grouped))
        return grouped

    def promotion_usage_rate(self, enriched_orders_df: pd.DataFrame) -> float:
        """
        Calcula la proporción de órdenes con promoción aplicada a partir de la bandera
        `used_promotion`; retorna 0.0 si el campo no está disponible.
        """
        if "used_promotion" not in enriched_orders_df.columns:
            return 0.0
        rate = enriched_orders_df["used_promotion"].mean()
        self.logger.info("Tasa de uso de promociones: %.2f", rate)
        return float(rate)
