"""
Módulo que se encarga de la limpieza de la tabla "reviews".
"""

import pandas as pd

from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class ReviewsCleaner(DataCleaner):
    """
    Clase que implementa lógica de negocio específica para limpiar tabla "reviews".
    """

    REQUIRED_COLUMNS = [
        "review_id",
        "product_id",
        "customer_id",
        "rating",
        "created_at",
    ]
    NUMERIC_COLUMNS = ["rating", "helpful_votes"]
    DATE_COLUMNS = ["created_at"]

    def __init__(self, logger=transform_logger):
        super().__init__(logger=logger)

    def handle_nulls(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Quita reseñas sin claves esenciales o sin rating/fecha y rellena helpful_votes en cero para evitar sesgos.
        """
        before = len(df)
        df = df.dropna(
            subset=["review_id", "product_id", "customer_id", "rating", "created_at"]
        ).copy()
        dropped = before - len(df)
        if dropped > 0:
            self.logger.warning(
                "Reviews descartadas por nulos en claves/rating/fecha: %s", dropped
            )
        df = self._fill_column(df, "helpful_votes", NullStrategy.FILL_ZERO)
        return df

    def handle_duplicates(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Depura duplicados por review_id conservando la versión más reciente para métricas de satisfacción coherentes.
        """
        before = len(df)
        df = df.drop_duplicates(subset=["review_id"], keep="last")
        removed = before - len(df)
        if removed > 0:
            self.logger.info("Reviews duplicadas eliminadas: %s", removed)
        return df

    def convert_types(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Normaliza ratings/votos a numérico y fechas de creación a datetime para cálculos de promedio y series temporales.
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
        Comprueba columnas requeridas de la reseña antes de agregaciones de calidad y volumen.
        """
        validator = SchemaValidator(df)
        validator.validate_required_columns(self.REQUIRED_COLUMNS)
        return df
