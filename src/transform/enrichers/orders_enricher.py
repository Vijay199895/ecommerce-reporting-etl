"""
Módulo que se encarga del enriquecimiento de la tabla "orders" para posterior análisis.
"""

import pandas as pd

from transform.cleaners.orders_cleaner import OrdersCleaner
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class OrdersEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "orders" para posterior análisis.
    """

    def __init__(self, cleaner: OrdersCleaner, logger=transform_logger):
        self.cleaner = cleaner
        self.logger = logger

    def enrich(
        self,
        orders_df: pd.DataFrame,
        customers_df: pd.DataFrame,
        promotions_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de orders lista para análisis de agregación.
        """
        self.logger.info("Iniciando enriquecimiento de tabla 'orders'")

        orders_df = self._validate_and_clean_orders(orders_df)
        customers_df = self._validate_customers(customers_df)
        order_items_df = self._validate_order_items(order_items_df)
        promotions_df = self._validate_promotions(promotions_df)

        enriched_df = self._join_customer_data(orders_df, customers_df)
        enriched_df = self._join_promotion_data(enriched_df, promotions_df)
        enriched_df = self._calculate_order_products_count_and_average_price(
            enriched_df, order_items_df
        )
        enriched_df = self._add_derived_columns(enriched_df)

        self.logger.info(
            "Enriquecimiento de tabla 'orders' completado: %s filas", len(enriched_df)
        )
        return enriched_df

    def _validate_and_clean_orders(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        validator = SchemaValidator(orders_df, self.logger)
        validator.validate_required_columns(OrdersCleaner.REQUIRED_COLUMNS)
        return self.cleaner.clean(orders_df)

    def _validate_customers(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        expected = ["customer_id", "segment", "registration_date"]
        validator = SchemaValidator(customers_df, self.logger)
        validator.validate_required_columns(expected)
        customers_df = customers_df.copy()
        customers_df["registration_date"] = pd.to_datetime(
            customers_df["registration_date"], errors="coerce"
        )
        # Campos opcionales útiles para marketing/segmentación
        for col in ["city", "country", "email"]:
            if col in customers_df.columns:
                customers_df[col] = customers_df[col]
        return customers_df

    def _validate_promotions(self, promotions_df: pd.DataFrame) -> pd.DataFrame:
        expected = ["promotion_id", "promotion_type", "discount_value", "is_active"]
        validator = SchemaValidator(promotions_df, self.logger)
        validator.validate_required_columns(expected)
        promotions_df = promotions_df.copy()
        promotions_df["start_date"] = pd.to_datetime(
            promotions_df["start_date"], errors="coerce"
        )
        promotions_df["end_date"] = pd.to_datetime(
            promotions_df["end_date"], errors="coerce"
        )
        return promotions_df

    def _validate_order_items(self, order_items_df: pd.DataFrame) -> pd.DataFrame:
        expected = ["order_id", "product_id", "quantity", "unit_price", "subtotal"]
        validator = SchemaValidator(order_items_df, self.logger)
        validator.validate_required_columns(expected)
        order_items_df = order_items_df.copy()
        for col in ["quantity", "unit_price", "subtotal"]:
            order_items_df[col] = pd.to_numeric(order_items_df[col], errors="coerce")
        return order_items_df

    def _join_customer_data(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = [
            col
            for col in [
                "customer_id",
                "segment",
                "registration_date",
                "city",
                "country",
                "email",
            ]
            if col in customers_df.columns
        ]
        return orders_df.merge(customers_df[cols], on="customer_id", how="left")

    def _join_promotion_data(
        self, orders_df: pd.DataFrame, promotions_df: pd.DataFrame
    ) -> pd.DataFrame:
        promo_cols = [
            "promotion_id",
            "promotion_type",
            "discount_value",
            "start_date",
            "end_date",
            "is_active",
        ]
        available = [c for c in promo_cols if c in promotions_df.columns]
        return orders_df.merge(promotions_df[available], on="promotion_id", how="left")

    def _calculate_order_products_count_and_average_price(
        self, orders_df: pd.DataFrame, order_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        grouped = (
            order_items_df.groupby("order_id")
            .agg({"quantity": "sum"})
            .reset_index()
            .rename(columns={"quantity": "items_count"})
        )

        enriched = orders_df.merge(grouped, on="order_id", how="left")

        # Precio promedio por orden
        if "items_count" in enriched.columns:
            enriched["avg_item_price"] = enriched["total_amount"] / enriched[
                "items_count"
            ].replace(0, pd.NA)
            enriched["avg_item_price"] = enriched["avg_item_price"].fillna(0)
        return enriched

    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if "order_date" in df.columns:
            df["order_month"] = df["order_date"].dt.to_period("M")
            df["order_week"] = df["order_date"].dt.to_period("W")
        df["used_promotion"] = df["promotion_id"].notna()
        if "shipping_cost" in df.columns:
            df["is_free_shipping"] = df["shipping_cost"].fillna(0) == 0
        if "discount_percent" in df.columns:
            df["is_high_discount"] = df["discount_percent"].fillna(0) >= 20
        return df
