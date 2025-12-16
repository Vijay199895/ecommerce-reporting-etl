"""
Agregaciones de ventas y evolución temporal.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing


class SalesAnalyticsAggregator:
    """
    Calcula métricas temporales de ventas y uso de promociones sobre órdenes enriquecidas.
    """

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_orders"
    )
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
        return grouped

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_orders"
    )
    def promotion_usage_rate(self, enriched_orders_df: pd.DataFrame) -> float:
        """
        Calcula la proporción de órdenes con promoción aplicada.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            Tasa de uso de promociones como float
        """
        rate = enriched_orders_df["used_promotion"].mean()
        return float(rate)
