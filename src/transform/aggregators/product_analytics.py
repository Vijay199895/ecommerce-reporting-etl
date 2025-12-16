"""
Agregaciones y métricas de productos.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing


class ProductAnalyticsAggregator:
    """
    Calcula métricas clave de performance de productos a partir de ítems de orden.
    """

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="order_items | products"
    )
    def top_products_by_quantity(
        self,
        order_items_df: pd.DataFrame,
        products_df: pd.DataFrame,
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Genera el ranking de productos por unidades vendidas, añadiendo revenue por subtotal;
        Se agrega el nombre del producto y entrega el top solicitado.

        Args:
            order_items_df: DataFrame que relaciona productos con órdenes
            products_df: DataFrame de productos
            top_n: Número de productos a retornar (default: 10)

        Returns:
            DataFrame con los productos más vendidos por unidades
        """
        grouped = (
            order_items_df.groupby("product_id")
            .agg(total_units=("quantity", "sum"), revenue=("subtotal", "sum"))
            .reset_index()
        )
        # Se agrega nombre del producto
        grouped = grouped.merge(
            products_df[["product_id", "product_name"]],
            on="product_id",
            how="left",
        )
        result = grouped.sort_values("total_units", ascending=False).head(top_n)
        return result

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="order_items | products"
    )
    def top_products_by_revenue(
        self,
        order_items_df: pd.DataFrame,
        products_df: pd.DataFrame,
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Genera el ranking de productos por revenue total, calculando también unidades vendidas;
        Se agrega el nombre del producto y devuelve el top indicado.

        Args:
            order_items_df: DataFrame que relaciona productos con órdenes
            products_df: DataFrame de productos
            top_n: Número de productos a retornar (default: 10)

        Returns:
            DataFrame con los productos más vendidos por revenue
        """
        grouped = (
            order_items_df.groupby("product_id")
            .agg(revenue=("subtotal", "sum"), total_units=("quantity", "sum"))
            .reset_index()
        )
        # Se agrega nombre del producto
        grouped = grouped.merge(
            products_df[["product_id", "product_name"]],
            on="product_id",
            how="left",
        )
        result = grouped.sort_values("revenue", ascending=False).head(top_n)
        return result
