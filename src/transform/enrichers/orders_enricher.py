"""
Módulo que se encarga del enriquecimiento de la tabla "orders" para posterior análisis.
"""

from typing import Optional

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
        promotions_df: Optional[pd.DataFrame],
        order_items_df: pd.DataFrame,
        products_df: Optional[pd.DataFrame] = None,
        categories_df: Optional[pd.DataFrame] = None,
        brands_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de orders lista para análisis de agregación.
        """
        self.logger.info("Iniciando enriquecimiento de órdenes")

        orders_df = self._validate_and_clean_orders(orders_df)
        customers_df = self._validate_customers(customers_df)
        order_items_df = self._validate_order_items(order_items_df)
        promotions_df = self._validate_promotions(promotions_df)

        enriched_df = self._join_customer_data(orders_df, customers_df)
        if promotions_df is not None:
            enriched_df = self._join_promotion_data(enriched_df, promotions_df)

        enriched_df = self._calculate_order_totals(enriched_df, order_items_df)
        enriched_df = self._add_derived_columns(enriched_df)

        if products_df is not None:
            enriched_df = self._attach_product_snapshot(
                enriched_df,
                order_items_df,
                products_df,
                categories_df=categories_df,
                brands_df=brands_df,
            )

        self.logger.info(
            "Enriquecimiento de órdenes completado: %s filas", len(enriched_df)
        )
        return enriched_df

    def _validate_and_clean_orders(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        validator = SchemaValidator(orders_df)
        validator.validate_required_columns(OrdersCleaner.REQUIRED_COLUMNS)
        return self.cleaner.clean(orders_df)

    def _validate_customers(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        expected = ["customer_id", "segment", "registration_date"]
        validator = SchemaValidator(customers_df)
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

    def _validate_promotions(
        self, promotions_df: Optional[pd.DataFrame]
    ) -> Optional[pd.DataFrame]:
        if promotions_df is None:
            return None
        expected = ["promotion_id", "promotion_type", "discount_value", "is_active"]
        validator = SchemaValidator(promotions_df)
        validator.validate_required_columns(expected)
        promotions_df = promotions_df.copy()
        promotions_df["start_date"] = pd.to_datetime(
            promotions_df.get("start_date"), errors="coerce"
        )
        promotions_df["end_date"] = pd.to_datetime(
            promotions_df.get("end_date"), errors="coerce"
        )
        return promotions_df

    def _validate_order_items(self, order_items_df: pd.DataFrame) -> pd.DataFrame:
        expected = ["order_id", "product_id", "quantity", "unit_price", "subtotal"]
        validator = SchemaValidator(order_items_df)
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

    def _calculate_order_totals(
        self, orders_df: pd.DataFrame, order_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        grouped = (
            order_items_df.groupby("order_id")
            .agg({"quantity": "sum", "subtotal": "sum"})
            .reset_index()
            .rename(columns={"quantity": "items_count", "subtotal": "items_subtotal"})
        )

        enriched = orders_df.merge(grouped, on="order_id", how="left")

        # Normalizar a numérico antes de rellenar para evitar downcasting silencioso
        for col in ["items_subtotal", "total_amount", "shipping_cost", "tax_amount", "discount_percent"]:
            if col in enriched.columns:
                enriched[col] = pd.to_numeric(enriched[col], errors="coerce")

        # Si falta total_amount o items_subtotal, tratamos de recomputar mínimos
        if "items_subtotal" in enriched.columns:
            enriched["items_subtotal"] = enriched["items_subtotal"].fillna(0)
        if "total_amount" in enriched.columns:
            enriched["total_amount"] = enriched["total_amount"].fillna(
                enriched["items_subtotal"]
            )

        # Precio promedio por item
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

    def _attach_product_snapshot(
        self,
        orders_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
        products_df: pd.DataFrame,
        categories_df: Optional[pd.DataFrame] = None,
        brands_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Adjunta datos resumidos de productos por orden para enriquecer cortes futuros.

        - Promedia costo y precio de lista de los ítems de la orden (sensibilidad de margen).
        - Conserva categoría y marca predominante (toma la primera disponible por simplicidad).
        """
        product_cols = [
            "product_id",
            "category_id",
            "brand_id",
            "cost",
            "price",
            "product_name",
        ]
        available = [c for c in product_cols if c in products_df.columns]
        products_df = products_df[available].copy()

        if categories_df is not None and "category_id" in products_df.columns:
            if (
                "category_id" in categories_df.columns
                and "category_name" in categories_df.columns
            ):
                products_df = products_df.merge(
                    categories_df[["category_id", "category_name"]],
                    on="category_id",
                    how="left",
                )

        if brands_df is not None and "brand_id" in products_df.columns:
            if "brand_id" in brands_df.columns and "brand_name" in brands_df.columns:
                products_df = products_df.merge(
                    brands_df[["brand_id", "brand_name"]],
                    on="brand_id",
                    how="left",
                )

        items_with_products = order_items_df.merge(
            products_df, on="product_id", how="left"
        )
        agg_map = {
            "cost": "mean",
            "price": "mean",
            "category_id": "first",
            "brand_id": "first",
        }
        if "category_name" in items_with_products.columns:
            agg_map["category_name"] = "first"
        if "brand_name" in items_with_products.columns:
            agg_map["brand_name"] = "first"
        if "product_name" in items_with_products.columns:
            agg_map["product_name"] = "first"

        per_order = (
            items_with_products.groupby("order_id")
            .agg(agg_map)
            .reset_index()
            .rename(columns={"cost": "avg_cost", "price": "avg_list_price"})
        )
        return orders_df.merge(per_order, on="order_id", how="left")
