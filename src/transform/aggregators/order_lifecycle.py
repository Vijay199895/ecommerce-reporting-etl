"""
Agregador para el ciclo de vida de órdenes (funnel de estado).
"""

import pandas as pd

from utils.logger import transform_logger


class OrderLifecycleAggregator:
    """
    Calcula métricas de estado de órdenes: funnel por status, cancelación, entregas y backlog.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def status_funnel(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cuenta órdenes por status, calcula la participación de cada estado y devuelve el funnel
        ordenado con totales y proporciones.
        """
        if "status" not in orders_df.columns:
            return pd.DataFrame()
        counts = orders_df["status"].value_counts().reset_index()
        counts.columns = ["status", "orders"]
        total = counts["orders"].sum()
        counts["share"] = counts["orders"] / total if total else 0
        self.logger.info("Funnel por estado generado: %s estados", len(counts))
        return counts

    def cancellation_rate(self, orders_df: pd.DataFrame) -> float:
        """
        Calcula la tasa de cancelación sobre el total de órdenes disponibles; retorna 0.0 si
        no hay columna de estado o no existen registros.
        """
        if "status" not in orders_df.columns or orders_df.empty:
            return 0.0
        rate = (orders_df["status"].str.lower() == "cancelled").mean()
        self.logger.info("Tasa de cancelación: %.2f", rate)
        return float(rate)

    def in_progress_backlog(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra órdenes en curso (pending, processing, shipped), agrega backlog por mes en conteo
        y valor total, y devuelve la serie temporal ordenada.
        """
        if "status" not in orders_df.columns:
            return pd.DataFrame()
        in_progress_status = {"pending", "processing", "shipped"}
        filtered = orders_df[
            orders_df["status"].str.lower().isin(in_progress_status)
        ].copy()
        if "order_month" not in filtered.columns and "order_date" in filtered.columns:
            filtered["order_month"] = filtered["order_date"].dt.to_period("M")
        grouped = (
            filtered.groupby("order_month")
            .agg(
                backlog_orders=("order_id", "count"),
                backlog_value=("total_amount", "sum"),
            )
            .reset_index()
            .sort_values("order_month")
        )
        self.logger.info("Backlog en progreso calculado: %s periodos", len(grouped))
        return grouped

    def delivery_rate(self, orders_df: pd.DataFrame) -> float:
        """
        Calcula la tasa de entrega sobre el total de órdenes; retorna 0.0 si falta el estado
        o no hay registros.
        """
        if "status" not in orders_df.columns or orders_df.empty:
            return 0.0
        rate = (orders_df["status"].str.lower() == "delivered").mean()
        self.logger.info("Tasa de entrega: %.2f", rate)
        return float(rate)
