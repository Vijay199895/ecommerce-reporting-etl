"""
Módulo que se encarga de la limpieza de la tabla "inventory".
"""

import pandas as pd

from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class InventoryCleaner(DataCleaner):
    """
    Clase que implementa lógica de negocio específica para limpiar tabla "inventory".
    """

    REQUIRED_COLUMNS = [
        "inventory_id",
        "product_id",
        "warehouse_id",
        "quantity",
        "min_stock_level",
        "max_stock_level",
    ]
    NUMERIC_COLUMNS = [
        "quantity",
        "min_stock_level",
        "max_stock_level",
        "current_occupancy",
    ]
    DATE_COLUMNS = ["last_restock_date"]

    def __init__(self, logger=transform_logger):
        super().__init__(logger=logger)

    def handle_nulls(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Elimina registros sin claves de inventario/producto/bodega y rellena cantidades/mínimos/máximos en cero para evaluar stock.
        """
        before = len(df)
        df = df.dropna(subset=["inventory_id", "product_id", "warehouse_id"]).copy()
        dropped = before - len(df)
        if dropped > 0:
            self.logger.warning(
                "Inventario descartado por nulos en claves: %s", dropped
            )
        for col in ["quantity", "min_stock_level", "max_stock_level"]:
            df = self._fill_column(df, col, NullStrategy.FILL_ZERO)
        return df

    def handle_duplicates(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Depura duplicados por inventory_id conservando la última versión para reflejar el stock más reciente.
        """
        before = len(df)
        df = df.drop_duplicates(subset=["inventory_id"], keep="last")
        removed = before - len(df)
        if removed > 0:
            self.logger.info("Inventario duplicado eliminado: %s", removed)
        return df

    def convert_types(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Convierte métricas de stock y ocupación a numérico y normaliza fechas de reabastecimiento para análisis de salud de inventario.
        """
        for col in self.NUMERIC_COLUMNS:
            if col in df.columns:
                before_na = df[col].isna().sum()
                df[col] = pd.to_numeric(df[col], errors="coerce")
                self._log_coercion_stats(df, col, self.logger, before_na)
        for col in self.DATE_COLUMNS:
            if col in df.columns:
                before_na = df[col].isna().sum()
                df[col] = pd.to_datetime(df[col], errors="coerce")
                self._log_coercion_stats(df, col, self.logger, before_na)
        return df

    def validate_cleaned_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Verifica columnas esenciales de inventario antes de enriquecer y calcular riesgos de stock.
        """
        validator = SchemaValidator(df, self.logger)
        validator.validate_required_columns(self.REQUIRED_COLUMNS)
        return df
