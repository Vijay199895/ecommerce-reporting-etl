"""
Módulo que se encarga del enriquecimiento de la tabla "inventory" para posterior análisis.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing, log_substep
from utils.validators import SchemaValidator


class InventoryEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "inventory" para posterior análisis.
    """

    @log_table_processing(
        stage="enrich", logger=transform_logger, table_name="inventory"
    )
    def enrich(
        self,
        inventory_df: pd.DataFrame,
        products_df: pd.DataFrame,
        warehouses_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de inventory lista para análisis de agregación.
        
        Espera a la tablas "inventory", "products" y "warehouses" limpias antes del proceso de enriquecimiento.
        """
        enriched_df = self._join_products(inventory_df, products_df)
        enriched_df = self._join_warehouses(enriched_df, warehouses_df)
        enriched_df = self._add_derived_columns(enriched_df)
        return enriched_df

    @log_substep(substep_name="Unión con tabla 'products'", logger=transform_logger)
    def _join_products(
        self, inventory_df: pd.DataFrame, products_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["product_id", "product_name", "category_id", "brand_id"]
        validator = SchemaValidator(products_df, transform_logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["product_id", "product_name"])
        return inventory_df.merge(products_df[cols], on="product_id", how="left")

    @log_substep(substep_name="Unión con tabla 'warehouses'", logger=transform_logger)
    def _join_warehouses(
        self, inventory_df: pd.DataFrame, warehouses_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["warehouse_id", "location", "capacity_units", "current_occupancy"]
        validator = SchemaValidator(warehouses_df, transform_logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["warehouse_id", "location"])
        return inventory_df.merge(warehouses_df[cols], on="warehouse_id", how="left")

    @log_substep(substep_name="Agregar columnas derivadas", logger=transform_logger)
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["is_low_stock"] = df["quantity"] <= df["min_stock_level"]
        df["is_overstock"] = df["quantity"] >= df["max_stock_level"]
        return df
