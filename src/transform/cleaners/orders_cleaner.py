"""
Módulo que se encarga de la limpieza de la tabla "orders".
"""

from typing import Dict, List

import pandas as pd

from exceptions import CleaningInvariantError
from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from utils.validators import SchemaValidator

from utils.logger import transform_logger, log_substep


class OrdersCleaner(DataCleaner):
    """
    Clase que implementa lógica de negocio específica para limpiar tabla "orders".
    """

    TABLE_NAME: str = "orders"

    REQUIRED_COLUMNS: List[str] = [
        "order_id",
        "customer_id",
        "order_date",
        "subtotal",
        "total_amount",
    ]

    NUMERIC_COLUMNS: List[str] = [
        "subtotal",
        "discount_percent",
        "shipping_cost",
        "tax_amount",
        "total_amount",
    ]

    ID_COLUMNS: List[str] = ["order_id", "customer_id"]

    DATE_COLUMNS: List[str] = ["order_date"]

    @log_substep(substep_name="Manejo de nulos", logger=transform_logger)
    def handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica que no hayan nulos en claves principales y rellena importes con medias o ceros para mantener consistencia financiera.
        """
        null_validator = SchemaValidator(df, transform_logger)
        null_validator.validate_no_nulls(["order_id", "customer_id", "order_date"])

        # TODO agregar estrategia para columna 'notes'
        # TODO agregar estrategia para columna 'promotion_id' -> rellenar con 0 para simular sin promoción
        # Estrategias específicas por columna
        strategies: Dict[str, NullStrategy] = {
            "subtotal": NullStrategy.FILL_MEAN,
            "total_amount": NullStrategy.FILL_MEAN,
            "discount_percent": NullStrategy.FILL_ZERO,
            "shipping_cost": NullStrategy.FILL_ZERO,
            "tax_amount": NullStrategy.FILL_ZERO,
        }

        for column, strategy in strategies.items():
            before_na = df[column].isna().sum()
            df = self._fill_column(df, column, strategy)
            after_na = df[column].isna().sum()
            filled = before_na - after_na
            if filled > 0:
                transform_logger.info(
                    "Valores rellenados en '%s' con estrategia %s: %s",
                    column,
                    strategy.value,
                    filled,
                )
            if after_na > before_na:
                raise CleaningInvariantError(
                    invariant="Los nulos no deben aumentar tras aplicar estrategia de relleno",
                    logger=transform_logger,
                    column=column,
                    details=f"Estrategia {strategy.value}: antes={before_na}, después={after_na}",
                )

        return df

    @log_substep(substep_name="Manejo de duplicados", logger=transform_logger)
    def handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina duplicados por order_id conservando la versión más reciente para asegurar unicidad de la orden.
        """
        before = len(df)
        df = df.drop_duplicates(subset=["order_id"], keep="last")
        removed = before - len(df)
        if removed > 0:
            transform_logger.info("Filas duplicadas eliminadas en órdenes: %s", removed)
        return df

    @log_substep(substep_name="Conversión de tipos", logger=transform_logger)
    def convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte importes, ids y fechas a tipos numéricos/fecha,
        y recalcula total_amount cuando falte usando sus componentes.
        """
        for col in self.ID_COLUMNS:
            if col in df.columns:
                before_na = df[col].isna().sum()
                df[col] = df[col].astype("int64")
                self._log_coercion_stats(df, col, transform_logger, before_na)

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

        if "total_amount" in df.columns:
            needs_recalc = df["total_amount"].isna()
            if needs_recalc.any():
                # Solo recalcular si subtotal existe; si no, dejamos NaN para revisión
                has_subtotal = df.loc[needs_recalc, "subtotal"].notna()
                idx = needs_recalc[needs_recalc].index.intersection(
                    has_subtotal[has_subtotal].index
                )
                df.loc[idx, "total_amount"] = (
                    df.loc[idx, "subtotal"].fillna(0)
                    + df.loc[idx, "shipping_cost"].fillna(0)
                    + df.loc[idx, "tax_amount"].fillna(0)
                    - (
                        df.loc[idx, "subtotal"].fillna(0)
                        * df.loc[idx, "discount_percent"].fillna(0)
                        / 100
                    )
                )
                if len(idx) > 0:
                    transform_logger.info(
                        "total_amount recalculado para %s filas con componentes disponibles",
                        len(idx),
                    )
            still_nan = df["total_amount"].isna() & df["subtotal"].isna()
            if still_nan.any():
                transform_logger.warning(
                    "total_amount permanece NaN en %s filas porque subtotal está ausente. Revisar calidad de datos.",
                    still_nan.sum(),
                )
        return df

    @log_substep(substep_name="Validación post-limpieza", logger=transform_logger)
    def validate_cleaned_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica presencia de columnas requeridas y tipos esperados para asegurar consistencia previa al enriquecimiento.
        """
        validator = SchemaValidator(df, transform_logger)
        validator.validate_required_columns(self.REQUIRED_COLUMNS)
        expected_types = {
            "order_id": "int64",
            "customer_id": "int64",
            "subtotal": "float",
            "total_amount": "float",
        }
        validator.validate_data_types(expected_types)
        validator.validate_numeric_range(column="subtotal", min_value=0)
        validator.validate_numeric_range(column="total_amount", min_value=0)
        validator.validate_numeric_range(
            column="discount_percent", min_value=0, max_value=100
        )
        validator.validate_numeric_range(column="shipping_cost", min_value=0)
        validator.validate_numeric_range(column="tax_amount", min_value=0)
        validator.validate_unique_values(columns=["order_id"])

        return df
