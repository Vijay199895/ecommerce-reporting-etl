"""
Módulo que se encarga de la limpieza de la tabla "reviews".
"""

import pandas as pd

from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from utils.validators import SchemaValidator

from utils.logger import transform_logger, log_substep


class ReviewsCleaner(DataCleaner):
    """
    Clase que implementa lógica de negocio específica para limpiar tabla "reviews".
    """

    TABLE_NAME: str = "reviews"

    REQUIRED_COLUMNS = [
        "review_id",
        "product_id",
        "customer_id",
        "rating",
        "created_at",
    ]
    NUMERIC_COLUMNS = ["rating", "helpful_votes"]
    DATE_COLUMNS = ["created_at"]

    @log_substep(substep_name="Manejo de nulos", logger=transform_logger)
    def handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica que no hayan nulos en claves esenciales y en rating/fecha;
        rellena helpful_votes con valor cero.
        """
        null_validator = SchemaValidator(df, transform_logger)
        null_validator.validate_no_nulls(
            ["review_id", "product_id", "customer_id", "rating", "created_at"]
        )
        df = self._fill_column(df, "helpful_votes", NullStrategy.FILL_ZERO)
        return df

    @log_substep(substep_name="Manejo de duplicados", logger=transform_logger)
    def handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Depura duplicados por review_id conservando la versión más reciente para métricas de satisfacción coherentes.
        """
        before = len(df)
        df = df.drop_duplicates(subset=["review_id"], keep="last")
        removed = before - len(df)
        if removed > 0:
            transform_logger.info("Reviews duplicadas eliminadas: %s", removed)
        return df

    @log_substep(substep_name="Conversión de tipos", logger=transform_logger)
    def convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza ratings/votos a numérico y fechas de creación a datetime para cálculos de promedio y series temporales.
        """
        for col in self.NUMERIC_COLUMNS:
            if col in df.columns:
                before_na = df[col].isna().sum()
                df[col] = pd.to_numeric(df[col], errors="coerce")
                self._log_coercion_stats(df, col, transform_logger, before_na)
        for col in self.DATE_COLUMNS:
            if col in df.columns:
                before_na = df[col].isna().sum()
                df[col] = pd.to_datetime(df[col], errors="coerce")
                self._log_coercion_stats(df, col, transform_logger, before_na)
        return df

    @log_substep(substep_name="Validación post-limpieza", logger=transform_logger)
    def validate_cleaned_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Comprueba columnas requeridas de la reseña antes de agregaciones de calidad y volumen.
        """
        validator = SchemaValidator(df, transform_logger)
        validator.validate_required_columns(self.REQUIRED_COLUMNS)
        validator.validate_numeric_range(column="rating", min_value=1, max_value=5)
        validator.validate_numeric_range(column="helpful_votes", min_value=0)
        validator.validate_unique_values(columns=["review_id"])
        return df
