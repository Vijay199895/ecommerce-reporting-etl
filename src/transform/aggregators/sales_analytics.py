"""
Agregaciones de ventas y evolución temporal.
"""

import pandas as pd

from utils.logger import transform_logger


class SalesAnalyticsAggregator:
    """
    Calcula métricas temporales de ventas y uso de promociones sobre órdenes enriquecidas.
    """

    def __init__(self):
        self.logger = transform_logger

    def monthly_sales(self, enriched_orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega ingresos y conteo de órdenes por mes.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            DataFrame con ventas mensuales agregadas
        """
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
        Calcula la proporción de órdenes con promoción aplicada.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            Tasa de uso de promociones como float
        """
        rate = enriched_orders_df["used_promotion"].mean()
        self.logger.info("Tasa de uso de promociones: %.2f", rate)
        return float(rate)
