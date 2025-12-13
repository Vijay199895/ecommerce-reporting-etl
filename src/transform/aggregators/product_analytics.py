"""
Agregaciones y métricas de productos.
"""

from typing import Optional

import pandas as pd

from utils.logger import transform_logger


class ProductAnalyticsAggregator:
    """
    Calcula métricas clave de performance de productos a partir de ítems de orden.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def top_products_by_quantity(
        self,
        order_items_df: pd.DataFrame,
        products_df: Optional[pd.DataFrame] = None,
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Genera el ranking de productos por unidades vendidas, añadiendo revenue por subtotal;
        si hay catálogo, agrega el nombre del producto y entrega el top solicitado.
        """
        grouped = (
            order_items_df.groupby("product_id")
            .agg(total_units=("quantity", "sum"), revenue=("subtotal", "sum"))
            .reset_index()
        )

        if products_df is not None and "product_name" in products_df.columns:
            grouped = grouped.merge(
                products_df[["product_id", "product_name"]],
                on="product_id",
                how="left",
            )

        result = grouped.sort_values("total_units", ascending=False).head(top_n)
        self.logger.info("Top productos por unidades calculados: %s", len(result))
        return result

    def top_products_by_revenue(
        self,
        order_items_df: pd.DataFrame,
        products_df: Optional[pd.DataFrame] = None,
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Genera el ranking de productos por revenue total, calculando también unidades vendidas;
        si se provee catálogo, incorpora el nombre del producto y devuelve el top indicado.
        """
        grouped = (
            order_items_df.groupby("product_id")
            .agg(revenue=("subtotal", "sum"), total_units=("quantity", "sum"))
            .reset_index()
        )

        if products_df is not None and "product_name" in products_df.columns:
            grouped = grouped.merge(
                products_df[["product_id", "product_name"]],
                on="product_id",
                how="left",
            )

        result = grouped.sort_values("revenue", ascending=False).head(top_n)
        self.logger.info("Top productos por revenue calculados: %s", len(result))
        return result
