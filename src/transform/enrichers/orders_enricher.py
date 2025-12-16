"""
Módulo que se encarga del enriquecimiento de la tabla "orders" para posterior análisis.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing, log_substep
from utils.validators import SchemaValidator


class OrdersEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "orders" para posterior análisis.
    """

    @log_table_processing(stage="enrich", logger=transform_logger, table_name="orders")
    def enrich(
        self,
        orders_df: pd.DataFrame,
        customers_df: pd.DataFrame,
        promotions_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de orders lista para análisis de agregación.

        Espera a la tablas "orders", "customers", "promotions" y "order_items" limpias antes del proceso de enriquecimiento.
        """
        enriched_df = self._join_customer_data(orders_df, customers_df)
        enriched_df = self._join_promotion_data(enriched_df, promotions_df)
        enriched_df = self._calculate_order_products_count_and_average_price(
            enriched_df, order_items_df
        )
        enriched_df = self._add_derived_columns(enriched_df)
        return enriched_df

    @log_substep(substep_name="Unión con tabla 'customers'", logger=transform_logger)
    def _join_customer_data(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = [
            "customer_id",
            "segment",
            "registration_date",
            "city",
            "country",
            "email",
        ]
        validator = SchemaValidator(customers_df, transform_logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["customer_id"])
        validator.validate_unique_values(["customer_id"])
        customers_df = customers_df.copy()
        customers_df["registration_date"] = pd.to_datetime(
            customers_df["registration_date"], errors="coerce"
        )
        return orders_df.merge(customers_df[cols], on="customer_id", how="left")

    @log_substep(substep_name="Unión con tabla 'promotions'", logger=transform_logger)
    def _join_promotion_data(
        self, orders_df: pd.DataFrame, promotions_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = [
            "promotion_id",
            "promotion_type",
            "discount_value",
            "start_date",
            "end_date",
            "is_active",
        ]
        validator = SchemaValidator(promotions_df, transform_logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["promotion_id", "promotion_type", "is_active"])
        validator.validate_unique_values(["promotion_id"])
        validator.validate_numeric_range(column="discount_value", min_value=0)
        promotions_df = promotions_df.copy()
        for col in ["start_date", "end_date"]:
            promotions_df[col] = pd.to_datetime(promotions_df[col], errors="coerce")
        return orders_df.merge(promotions_df[cols], on="promotion_id", how="left")

    @log_substep(
        substep_name="Cálculo de cantidad de productos y precio promedio por orden",
        logger=transform_logger,
    )
    def _calculate_order_products_count_and_average_price(
        self, orders_df: pd.DataFrame, order_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        # Cantidad de ítems por orden
        grouped = (
            order_items_df.groupby("order_id")
            .agg({"quantity": "sum"})
            .reset_index()
            .rename(columns={"quantity": "items_count"})
        )
        enriched = orders_df.merge(grouped, on="order_id", how="left")

        # Precio promedio por orden
        enriched["avg_item_price"] = enriched["total_amount"] / enriched[
            "items_count"
        ].replace(0, pd.NA)
        enriched["avg_item_price"] = enriched["avg_item_price"].fillna(0)
        return enriched

    @log_substep(substep_name="Agregar columnas derivadas", logger=transform_logger)
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["order_month"] = df["order_date"].dt.to_period("M")
        df["order_week"] = df["order_date"].dt.to_period("W")
        df["used_promotion"] = df["promotion_id"] != 0
        df["is_free_shipping"] = df["shipping_cost"].fillna(0) == 0
        df["is_high_discount"] = df["discount_percent"].fillna(0) >= 20
        return df
