"""
Módulo que se encarga del enriquecimiento de la tabla "inventory" para posterior análisis.
"""

import pandas as pd

from transform.cleaners.inventory_cleaner import InventoryCleaner
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class InventoryEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "inventory" para posterior análisis.
    """

    def __init__(self):
        self.cleaner = InventoryCleaner()
        self.logger = transform_logger

    def enrich(
        self,
        inventory_df: pd.DataFrame,
        products_df: pd.DataFrame,
        warehouses_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de inventory lista para análisis de agregación.
        """
        self.logger.info("Iniciando enriquecimiento de tabla 'inventory'")
        inventory_df = self._clean_inventory(inventory_df)
        enriched_df = self._join_products(inventory_df, products_df)
        enriched_df = self._join_warehouses(enriched_df, warehouses_df)
        enriched_df = self._add_derived_columns(enriched_df)
        self.logger.info(
            "Enriquecimiento de tabla 'inventory' completado: %s filas",
            len(enriched_df),
        )
        return enriched_df

    def _clean_inventory(self, inventory_df: pd.DataFrame) -> pd.DataFrame:
        return self.cleaner.clean(inventory_df)

    def _join_products(
        self, inventory_df: pd.DataFrame, products_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["product_id", "product_name", "category_id", "brand_id"]
        validator = SchemaValidator(products_df, self.logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["product_id", "product_name"])
        return inventory_df.merge(products_df[cols], on="product_id", how="left")

    def _join_warehouses(
        self, inventory_df: pd.DataFrame, warehouses_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["warehouse_id", "location", "capacity_units", "current_occupancy"]
        validator = SchemaValidator(warehouses_df, self.logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls(["warehouse_id", "location"])
        return inventory_df.merge(warehouses_df[cols], on="warehouse_id", how="left")

    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["is_low_stock"] = df["quantity"] <= df["min_stock_level"]
        df["is_overstock"] = df["quantity"] >= df["max_stock_level"]
        return df
