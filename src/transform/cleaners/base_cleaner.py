"""
Clases base y estrategias de limpieza de datos.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import pandas as pd

from utils.logger import transform_logger


class NullStrategy(Enum):
    """Estrategias genéricas para manejo de valores nulos."""

    DROP = "drop"
    FILL_MEAN = "fill_mean"
    FILL_MEDIAN = "fill_median"
    FILL_MODE = "fill_mode"
    FILL_VALUE = "fill_value"
    FILL_ZERO = "fill_zero"
    FORWARD_FILL = "ffill"
    BACKWARD_FILL = "bfill"


class DataCleaner(ABC):
    """
    Clase base para limpiadores de tablas.
    Orquesta pasos comunes: nulos, duplicados, conversión de tipos y validación final.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def clean(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Ejecuta el pipeline de limpieza y devuelve un nuevo DataFrame.
        """
        self.logger.info("Iniciando limpieza: %s filas", len(df))

        df = self.handle_nulls(df.copy(), **kwargs)
        df = self.handle_duplicates(df, **kwargs)
        df = self.convert_types(df, **kwargs)
        df = self.validate_cleaned_data(df, **kwargs)

        self.logger.info("Limpieza completada: %s filas", len(df))
        return df

    @abstractmethod
    def handle_nulls(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Implementa lógica de manejo de valores nulos con lógica de negocio
        específica de la tabla.
        """
        raise NotImplementedError()

    @abstractmethod
    def handle_duplicates(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Implementa lógica de manejo de duplicados con lógica de negocio
        específica de la tabla.
        """
        raise NotImplementedError()

    @abstractmethod
    def convert_types(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Implementa lógica de manejo de conversión de tipos con lógica de negocio
        específica de la tabla.
        """
        raise NotImplementedError()

    @abstractmethod
    def validate_cleaned_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Implementa lógica de manejo de validación post-limpieza con lógica de negocio
        específica de la tabla.
        """
        raise NotImplementedError()

    @staticmethod
    def _fill_column(
        df: pd.DataFrame, column: str, strategy: NullStrategy, fill_value: Any = None
    ) -> pd.DataFrame:
        """
        Aplica una estrategia de llenado de nulos a una columna.
        """
        if column not in df.columns:
            return df
        df = df.copy()

        if strategy == NullStrategy.DROP:
            return df.dropna(subset=[column])

        if strategy == NullStrategy.FILL_ZERO:
            df[column] = df[column].fillna(0)
            return df

        if strategy == NullStrategy.FILL_VALUE:
            df[column] = df[column].fillna(fill_value)
            return df

        if strategy == NullStrategy.FILL_MEAN:
            df[column] = pd.to_numeric(df[column], errors="coerce")
            df[column] = df[column].fillna(df[column].mean())
            return df

        if strategy == NullStrategy.FILL_MEDIAN:
            df[column] = pd.to_numeric(df[column], errors="coerce")
            df[column] = df[column].fillna(df[column].median())
            return df

        if strategy == NullStrategy.FILL_MODE:
            mode_value = (
                df[column].mode().iloc[0] if not df[column].mode().empty else fill_value
            )
            df[column] = df[column].fillna(mode_value)
            return df

        if strategy == NullStrategy.FORWARD_FILL:
            df[column] = df[column].ffill()
            return df

        if strategy == NullStrategy.BACKWARD_FILL:
            df[column] = df[column].bfill()
            return df

        return df

    @staticmethod
    def _log_coercion_stats(df: pd.DataFrame, col: str, logger, before_na: int) -> None:
        """
        Loguea cuántos valores se convirtieron a nulos durante coerción.
        """
        after_na = df[col].isna().sum()
        delta = after_na - before_na
        if delta > 0:
            logger.warning("Coerción a NA en '%s': %s valores", col, delta)
