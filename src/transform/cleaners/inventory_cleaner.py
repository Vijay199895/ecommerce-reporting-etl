"""
Módulo que se encarga de la limpieza de la tabla "inventory".
"""

import pandas as pd

from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from utils.validators import SchemaValidator

from utils.logger import transform_logger, log_substep


class InventoryCleaner(DataCleaner):
    """
    Clase que implementa lógica de negocio específica para limpiar tabla "inventory".
    """

    TABLE_NAME: str = "inventory"

    REQUIRED_COLUMNS = [
        "inventory_id",
        "product_id",
        "warehouse_id",
        "quantity",
        "min_stock_level",
        "max_stock_level",
    ]
    NUMERIC_COLUMNS = ["quantity", "min_stock_level", "max_stock_level"]
    DATE_COLUMNS = ["last_restock_date"]

    @log_substep(substep_name="Manejo de nulos", logger=transform_logger)
    def handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica que no hayan nulos en claves de inventario/producto/bodega y
        rellena cantidades/mínimos/máximos en cero para evaluar stock.
        """
        null_validator = SchemaValidator(df, transform_logger)
        null_validator.validate_no_nulls(["inventory_id", "product_id", "warehouse_id"])
        for col in ["quantity", "min_stock_level", "max_stock_level"]:
            df = self._fill_column(df, col, NullStrategy.FILL_ZERO)
        return df

    @log_substep(substep_name="Manejo de duplicados", logger=transform_logger)
    def handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Depura duplicados por inventory_id conservando la última versión para reflejar el stock más reciente.
        """
        before = len(df)
        df = df.drop_duplicates(subset=["inventory_id"], keep="last")
        removed = before - len(df)
        if removed > 0:
            transform_logger.info("Inventario duplicado eliminado: %s", removed)
        return df

    @log_substep(substep_name="Conversión de tipos", logger=transform_logger)
    def convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte métricas de stock y ocupación a numérico y normaliza fechas
        para análisis de salud de inventario.
        """
        for col in self.NUMERIC_COLUMNS:
            before_na = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            self._log_coercion_stats(df, col, transform_logger, before_na)
        for col in self.DATE_COLUMNS:
            before_na = df[col].isna().sum()
            df[col] = pd.to_datetime(df[col], errors="coerce")
            self._log_coercion_stats(df, col, transform_logger, before_na)
        return df

    @log_substep(substep_name="Validación post-limpieza", logger=transform_logger)
    def validate_cleaned_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica columnas esenciales de inventario antes de enriquecer y calcular riesgos de stock.
        """
        validator = SchemaValidator(df, transform_logger)
        validator.validate_required_columns(self.REQUIRED_COLUMNS)
        validator.validate_numeric_range(column="quantity", min_value=0)
        validator.validate_numeric_range(column="min_stock_level", min_value=0)
        validator.validate_numeric_range(column="max_stock_level", min_value=0)
        validator.validate_unique_values(columns=["inventory_id"])
        return df
