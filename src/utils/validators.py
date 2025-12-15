"""
Validadores de schema y calidad de datos
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from exceptions import (
    DataTypeMismatchError,
    DuplicateKeyError,
    MissingRequiredColumnsError,
    NullConstraintError,
    RangeValidationError,
    UnexpectedColumnsError,
)


class SchemaValidator:
    """
    Validador de schema para DataFrames.

    Proporciona métodos para validar columnas requeridas, tipos de datos,
    y otras restricciones de schema en DataFrames de pandas.
    """

    def __init__(self, dataframe: pd.DataFrame, logger: logging.Logger):
        """
        Inicializa el validador con un DataFrame.

        Args:
            dataframe: DataFrame a validar
            logger: Logger para registrar mensajes de validación en la etapa correspondiente
        """
        self.df = dataframe
        self.logger = logger

    def validate_required_columns(self, required_columns: List[str]) -> bool:
        """
        Valida que todas las columnas requeridas estén presentes en el DataFrame.

        Args:
            required_columns: Lista de nombres de columnas que deben existir

        Returns:
            bool: True si todas las columnas están presentes

        Raises:
            MissingRequiredColumnsError: Si alguna columna requerida falta
        """
        actual_columns = set(self.df.columns)
        required_set = set(required_columns)
        missing_columns = required_set - actual_columns
        if missing_columns:
            raise MissingRequiredColumnsError(
                missing_columns=list(missing_columns),
                logger=self.logger,
                available_columns=list(actual_columns),
            )
        success_msg = (
            f"Todas las columnas requeridas están presentes: {sorted(required_columns)}"
        )
        self.logger.info(success_msg)
        return True

    def validate_no_extra_columns(self, expected_columns: List[str]) -> bool:
        """
        Valida que no existan columnas adicionales no esperadas.

        Args:
            expected_columns: Lista de columnas que deben existir (y solo esas)

        Returns:
            bool: True si no hay columnas extras

        Raises:
            UnexpectedColumnsError: Si existen columnas no esperadas
        """
        actual_columns = set(self.df.columns)
        expected_set = set(expected_columns)
        extra_columns = actual_columns - expected_set
        if extra_columns:
            raise UnexpectedColumnsError(
                extra_columns=list(extra_columns),
                logger=self.logger,
                expected_columns=list(expected_set),
            )
        success_msg = "No hay columnas extras. Schema exacto validado."
        self.logger.info(success_msg)
        return True

    def validate_data_types(self, expected_types: Dict[str, str]) -> bool:
        """
        Valida que las columnas tengan los tipos de datos esperados.

        Args:
            expected_types: Diccionario {nombre_columna: tipo_esperado}
                           Ejemplos de tipos: 'int64', 'float64', 'object', 'datetime64[ns]'

        Returns:
            bool: True si todos los tipos coinciden

        Raises:
            DataTypeMismatchError: Si algún tipo no coincide
        """
        type_mismatches = []
        for column, expected_type in expected_types.items():
            self._check_column_in_df(column)
            actual_type = str(self.df[column].dtype)
            if not self._types_match(actual_type, expected_type):
                type_mismatches.append(
                    {
                        "columna": column,
                        "esperado": expected_type,
                        "actual": actual_type,
                    }
                )
        if type_mismatches:
            raise DataTypeMismatchError(
                type_mismatches=type_mismatches, logger=self.logger
            )
        success_msg = f"Todos los tipos de datos son correctos: {expected_types}"
        self.logger.info(success_msg)
        return True

    def validate_numeric_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        allow_nulls: bool = True,
    ) -> bool:
        """
        Valida que los valores numéricos estén dentro de un rango esperado.

        Args:
            column: Nombre de la columna a validar
            min_value: Valor mínimo permitido (inclusive)
            max_value: Valor máximo permitido (inclusive)
            allow_nulls: Si False, falla si hay valores nulos

        Returns:
            bool: True si todos los valores están en rango

        Raises:
            NullConstraintError: Si hay valores nulos no permitidos
            RangeValidationError: Si hay valores fuera de rango
        """
        self._check_column_in_df(column)
        col_data = self.df[column]
        # Validar nulos
        if not allow_nulls and col_data.isnull().any():
            null_count = int(col_data.isnull().sum())
            null_percentage = round((null_count / len(self.df)) * 100, 2)
            raise NullConstraintError(
                columns_with_nulls=[
                    {
                        "columna": column,
                        "valores_nulos": null_count,
                        "porcentaje": null_percentage,
                    }
                ],
                logger=self.logger,
            )
        non_null_data = col_data.dropna()
        if len(non_null_data) == 0:
            warning_msg = (
                f"Columna '{column}' no tiene valores no-nulos para validar rango"
            )
            self.logger.warning(warning_msg)
            return True
        # Validar rango mínimo
        if min_value is not None:
            violations = non_null_data < min_value
            if violations.any():
                violation_count = int(violations.sum())
                min_found = float(non_null_data.min())
                raise RangeValidationError(
                    column=column,
                    logger=self.logger,
                    min_value=min_value,
                    max_value=max_value,
                    violation_count=violation_count,
                    actual_min=min_found,
                    actual_max=float(non_null_data.max()),
                )

        # Validar rango máximo
        if max_value is not None:
            violations = non_null_data > max_value
            if violations.any():
                violation_count = int(violations.sum())
                max_found = float(non_null_data.max())
                raise RangeValidationError(
                    column=column,
                    logger=self.logger,
                    min_value=min_value,
                    max_value=max_value,
                    violation_count=violation_count,
                    actual_min=float(non_null_data.min()),
                    actual_max=max_found,
                )
        range_str = (
            f"[{min_value if min_value is not None else '-inf'}, "
            f"{max_value if max_value is not None else '+inf'}]"
        )
        success_msg = (
            f"Columna '{column}': todos los valores están en el rango {range_str}"
        )
        self.logger.info(success_msg)
        return True

    def validate_no_nulls(self, columns: Optional[List[str]] = None) -> bool:
        """
        Valida que las columnas especificadas no contengan valores nulos.

        Args:
            columns: Lista de columnas a validar. Si es None, valida todas las columnas.

        Returns:
            bool: True si no hay valores nulos

        Raises:
            NullConstraintError: Si se encuentran valores nulos
        """
        cols_to_check = columns if columns else list(self.df.columns)
        columns_with_nulls = []
        for col in cols_to_check:
            self._check_column_in_df(col)
            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                null_percentage = round((null_count / len(self.df)) * 100, 2)
                columns_with_nulls.append(
                    {
                        "columna": col,
                        "valores_nulos": int(null_count),
                        "porcentaje": null_percentage,
                    }
                )
        if columns_with_nulls:
            raise NullConstraintError(
                columns_with_nulls=columns_with_nulls, logger=self.logger
            )
        success_msg = f"Ninguna columna contiene valores nulos: {cols_to_check}"
        self.logger.info(success_msg)
        return True

    def validate_unique_values(self, columns: List[str]) -> bool:
        """
        Valida que las columnas especificadas contengan valores únicos (sin duplicados).

        Args:
            columns: Lista de columnas que deben tener valores únicos

        Returns:
            bool: True si todas las columnas tienen valores únicos

        Raises:
            DuplicateKeyError: Si se encuentran valores duplicados
        """
        columns_with_duplicates = []
        total_duplicates = 0
        for col in columns:
            self._check_column_in_df(col)
            duplicate_count = self.df[col].duplicated().sum()
            if duplicate_count > 0:
                columns_with_duplicates.append(col)
                total_duplicates += duplicate_count
        if columns_with_duplicates:
            raise DuplicateKeyError(
                columns=columns_with_duplicates,
                logger=self.logger,
                duplicate_count=int(total_duplicates),
            )
        success_msg = f"Todas las columnas tienen valores únicos: {columns}"
        self.logger.info(success_msg)
        return True

    def _check_column_in_df(self, column: str) -> None:
        """
        Verifica que una columna exista en el DataFrame.

        Args:
            column: Nombre de la columna a verificar

        Raises:
            MissingRequiredColumnsError: Si la columna no existe
        """
        if column not in self.df.columns:
            raise MissingRequiredColumnsError(
                missing_columns=[column],
                logger=self.logger,
                available_columns=list(self.df.columns),
            )

    @staticmethod
    def _types_match(actual_type: str, expected_type: str) -> bool:
        """
        Compara dos tipos de datos con normalización flexible.

        Permite coincidencias como 'int64' con 'int', 'float32' con 'float', etc.

        Args:
            actual_type: Tipo de dato actual (ej: 'int64')
            expected_type: Tipo de dato esperado (ej: 'int' o 'int64')

        Returns:
            bool: True si los tipos coinciden
        """
        # Comparación exacta
        if actual_type == expected_type:
            return True

        # Normalización flexible
        type_mappings = {
            "int": ["int8", "int16", "int32", "int64"],
            "float": ["float16", "float32", "float64"],
            "string": ["object", "str"],
            "datetime": ["datetime64", "datetime64[ns]"],
        }

        for base_type, variants in type_mappings.items():
            if expected_type == base_type and actual_type in variants:
                return True
            if expected_type in variants and actual_type == base_type:
                return True

        return False
