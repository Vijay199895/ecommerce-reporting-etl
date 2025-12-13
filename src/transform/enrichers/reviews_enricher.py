"""
Módulo que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
"""

import pandas as pd

from transform.cleaners.reviews_cleaner import ReviewsCleaner
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class ReviewsEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
    """

    def __init__(self, cleaner: ReviewsCleaner, logger=transform_logger):
        self.cleaner = cleaner
        self.logger = logger

    def enrich(
        self,
        reviews_df: pd.DataFrame,
        products_df: pd.DataFrame,
        customers_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de reviews lista para análisis de agregación.
        """
        self.logger.info("Iniciando enriquecimiento de tabla 'reviews'")
        reviews_df = self._validate_and_clean_reviews(reviews_df)

        enriched_df = self._join_products(reviews_df, products_df)
        enriched_df = self._join_customers(enriched_df, customers_df)
        enriched_df = self._add_derived_columns(enriched_df)

        self.logger.info(
            "Enriquecimiento de tabla 'reviews' completado: %s filas", len(enriched_df)
        )
        return enriched_df

    def _validate_and_clean_reviews(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        validator = SchemaValidator(reviews_df)
        validator.validate_required_columns(ReviewsCleaner.REQUIRED_COLUMNS)
        return self.cleaner.clean(reviews_df)

    def _join_products(
        self, reviews_df: pd.DataFrame, products_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = [
            c
            for c in ["product_id", "product_name", "category_id", "brand_id"]
            if c in products_df.columns
        ]
        return reviews_df.merge(products_df[cols], on="product_id", how="left")

    def _join_customers(
        self, reviews_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = [
            c
            for c in ["customer_id", "segment", "city", "country"]
            if c in customers_df.columns
        ]
        return reviews_df.merge(customers_df[cols], on="customer_id", how="left")

    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if "created_at" in df.columns:
            df["review_month"] = df["created_at"].dt.to_period("M")
        if "comment" in df.columns:
            df["comment_length"] = df["comment"].fillna("").str.len()
        if "rating" in df.columns:
            df["is_positive"] = df["rating"] >= 4
            df["is_negative"] = df["rating"] <= 2
        return df
