"""
Módulo que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing, log_substep
from utils.validators import SchemaValidator


class ReviewsEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
    """
        
    @log_table_processing(stage="enrich", logger=transform_logger, table_name="reviews")
    def enrich(
        self,
        reviews_df: pd.DataFrame,
        products_df: pd.DataFrame,
        customers_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de reviews lista para análisis de agregación.
        
        Espera a la tablas "reviews", "products" y "customers" limpias antes del proceso de enriquecimiento.
        """
        enriched_df = self._join_products(reviews_df, products_df)
        enriched_df = self._join_customers(enriched_df, customers_df)
        enriched_df = self._add_derived_columns(enriched_df)
        return enriched_df

    @log_substep(substep_name="Unión con tabla 'products'", logger=transform_logger)
    def _join_products(
        self, reviews_df: pd.DataFrame, products_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["product_id", "product_name", "category_id", "brand_id"]
        validator = SchemaValidator(products_df, transform_logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls()
        return reviews_df.merge(products_df[cols], on="product_id", how="left")

    @log_substep(substep_name="Unión con tabla 'customers'", logger=transform_logger)
    def _join_customers(
        self, reviews_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["customer_id", "segment", "city", "country"]
        validator = SchemaValidator(customers_df, transform_logger)
        validator.validate_required_columns(["customer_id"])
        return reviews_df.merge(customers_df[cols], on="customer_id", how="left")

    @log_substep(substep_name="Agregar columnas derivadas", logger=transform_logger)
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["review_month"] = df["created_at"].dt.to_period("M")
        df["comment_length"] = df["comment"].fillna("").str.len()
        df["is_positive"] = df["rating"] >= 4
        df["is_negative"] = df["rating"] <= 2
        return df
