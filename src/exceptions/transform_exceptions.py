"""
Módulo que define las excepciones para la capa de transformación del proceso ETL.
"""

import logging
from typing import Any, Dict, List, Optional

from exceptions.base import ETLError


class TransformError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de transformación del proceso ETL.
    """


class SchemaValidationError(TransformError):
    """
    Excepción lanzada cuando los datos no cumplen con el esquema esperado.
    """


class MissingRequiredColumnsError(SchemaValidationError):
    """
    Excepción lanzada cuando faltan columnas requeridas en los datos.
    """

    def __init__(
        self,
        missing_columns: List[str],
        logger: logging.Logger,
        available_columns: Optional[List[str]] = None,
    ):
        message = f"Columnas requeridas faltantes: {sorted(missing_columns)}"
        if available_columns:
            message += f". Columnas disponibles: {sorted(available_columns)}"
        super().__init__(message, logger=logger)


class UnexpectedColumnsError(SchemaValidationError):
    """
    Excepción lanzada cuando hay columnas inesperadas en los datos.
    """

    def __init__(
        self,
        extra_columns: List[str],
        logger: logging.Logger,
        expected_columns: Optional[List[str]] = None,
    ):
        message = f"Columnas no esperadas encontradas: {sorted(extra_columns)}"
        if expected_columns:
            message += f". Columnas esperadas: {sorted(expected_columns)}"
        super().__init__(message, logger=logger)


class DataTypeMismatchError(SchemaValidationError):
    """
    Excepción lanzada cuando los tipos de datos no coinciden con el esquema esperado.
    """

    def __init__(
        self,
        type_mismatches: List[Dict[str, str]],
        logger: logging.Logger,
    ):
        details = "; ".join(
            f"'{m['columna']}': esperado {m['esperado']}, actual {m['actual']}"
            for m in type_mismatches
        )
        message = f"Tipos de datos no coinciden: [{details}]"
        super().__init__(message, logger=logger)


class DataQualityError(TransformError):
    """
    Excepción lanzada cuando los datos no cumplen con los criterios de calidad esperados.
    """


class RangeValidationError(DataQualityError):
    """
    Excepción lanzada cuando los valores de una columna están fuera del rango esperado.
    """

    def __init__(
        self,
        column: str,
        logger: logging.Logger,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        violation_count: int = 0,
        actual_min: Optional[float] = None,
        actual_max: Optional[float] = None,
    ):
        range_str = (
            f"[{min_value if min_value is not None else '-inf'}, "
            f"{max_value if max_value is not None else '+inf'}]"
        )
        message = (
            f"Columna '{column}': {violation_count} valores fuera del rango {range_str}"
        )
        if actual_min is not None or actual_max is not None:
            actual_range = f"[{actual_min}, {actual_max}]"
            message += f". Rango actual: {actual_range}"
        super().__init__(message, logger=logger)


class NullConstraintError(DataQualityError):
    """
    Excepción lanzada cuando una columna que no permite nulos contiene valores nulos.
    """

    def __init__(
        self, columns_with_nulls: List[Dict[str, Any]], logger: logging.Logger
    ):
        if len(columns_with_nulls) == 1:
            col_info = columns_with_nulls[0]
            message = (
                f"La columna '{col_info['columna']}' contiene "
                f"{col_info['valores_nulos']} valores nulos "
                f"({col_info.get('porcentaje', 'N/A')}%)"
            )
        else:
            details = "; ".join(
                f"'{c['columna']}': {c['valores_nulos']} nulos "
                f"({c.get('porcentaje', 'N/A')}%)"
                for c in columns_with_nulls
            )
            message = f"Columnas con valores nulos no permitidos: [{details}]"
        super().__init__(message, logger=logger)


class DuplicateKeyError(DataQualityError):
    """
    Excepción lanzada cuando se encuentran claves duplicadas en una o más columnas.
    """

    def __init__(
        self,
        columns: List[str],
        duplicate_count: int,
        logger: logging.Logger,
    ):
        cols_str = ", ".join(f"'{c}'" for c in columns)
        message = (
            f"Se encontraron {duplicate_count} valores duplicados "
            f"en las columnas: [{cols_str}]"
        )
        super().__init__(message, logger=logger)


class CleaningInvariantError(TransformError):
    """
    Excepción lanzada cuando no se pueden cumplir las invariantes de limpieza de datos.
    """

    def __init__(
        self,
        invariant: str,
        logger: logging.Logger,
        column: Optional[str] = None,
        details: Optional[str] = None,
        log_level: int = logging.WARNING,
    ):
        message = f"Invariante de limpieza violada: {invariant}"
        if column:
            message += f" en columna '{column}'"
        if details:
            message += f". Detalles: {details}"
        super().__init__(message, logger=logger, log_level=log_level)
