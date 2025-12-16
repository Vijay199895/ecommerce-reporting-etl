"""
Agregaciones y métricas de clientes.
"""

from typing import Optional

import pandas as pd

from utils.logger import transform_logger, log_table_processing


class CustomerAnalyticsAggregator:
    """
    Responde preguntas de negocio sobre clientes a partir de órdenes enriquecidas.
    """

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_orders"
    )
    def top_spenders(
        self,
        enriched_orders_df: pd.DataFrame,
        top_n: int = 5,
        percentile: Optional[float] = 0.8,
    ) -> pd.DataFrame:
        """
        Calcula clientes con mayor gasto total, aplica filtro por percentil si se indica,
        y devuelve el top ordenado con total gastado, número de órdenes, ticket promedio
        y fecha de última compra junto con el email del cliente para posible seguimiento.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas
            top_n: Número de clientes a retornar (default: 5)
            percentile: Percentil para filtrar clientes por gasto total (default: 0.8)

        Returns:
            DataFrame con clientes top spenders.
        """
        grouped = (
            enriched_orders_df.groupby("customer_id")
            .agg(
                total_orders=("order_id", "count"),
                total_spent=("total_amount", "sum"),
                last_order_date=("order_date", "max"),
                email=("email", "first"),
            )
            .reset_index()
        )
        grouped["avg_ticket"] = grouped["total_spent"] / grouped["total_orders"]

        if percentile is not None:
            threshold = grouped["total_spent"].quantile(percentile)
            grouped = grouped[grouped["total_spent"] >= threshold]

        result = grouped.sort_values("total_spent", ascending=False).head(top_n)
        return result

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_orders"
    )
    def recurring_customers(
        self, enriched_orders_df: pd.DataFrame, min_orders: int = 2
    ) -> pd.DataFrame:
        """
        Identifica clientes con un número de órdenes mayor o igual al mínimo especificado,
        devolviendo el recuento de órdenes por cliente ordenado de mayor a menor. Se incluye
        el email del cliente para posible seguimiento.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas
            min_orders: Mínimo número de órdenes para considerar un cliente como recurrente (default: 2)

        Returns:
            DataFrame con clientes recurrentes.
        """
        grouped = (
            enriched_orders_df.groupby("customer_id")
            .agg(total_orders=("order_id", "count"), email=("email", "first"))
            .reset_index()
        )
        recurring = grouped[grouped["total_orders"] >= min_orders]
        return recurring.sort_values("total_orders", ascending=False)

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_orders"
    )
    def average_ticket_overall(self, enriched_orders_df: pd.DataFrame) -> float:
        """
        Calcula el ticket promedio global considerando el total_amount de todas las órdenes
        disponibles; retorna 0.0 si no hay datos válidos.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            Ticket promedio como float
        """
        avg_ticket = enriched_orders_df["total_amount"].mean()
        return float(avg_ticket) if pd.notna(avg_ticket) else 0.0
