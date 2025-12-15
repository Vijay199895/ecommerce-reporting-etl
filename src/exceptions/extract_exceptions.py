"""
Módulo que define las excepciones para la capa de extracción del proceso ETL.
"""

import logging
from typing import Optional

from exceptions.base import ETLError


class ExtractError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de extracción del proceso ETL.
    """


class SourceNotFoundError(ExtractError):
    """
    Excepción lanzada cuando no se encuentra la fuente de datos especificada.
    """

    def __init__(self, source_path: str, logger: logging.Logger, source_type: str):
        message = f"No se encontró el {source_type} de origen en: '{source_path}'"
        super().__init__(message, logger=logger)


class SourceParseError(ExtractError):
    """
    Excepción lanzada cuando ocurre un error al parsear la fuente de datos.
    """

    def __init__(
        self,
        source_path: str,
        logger: logging.Logger,
        original_error: Optional[Exception] = None,
        details: Optional[str] = None,
    ):
        message = f"Error al parsear la fuente de datos: '{source_path}'"
        if details:
            message += f". Detalles: {details}"
        if original_error:
            message += (
                f". Error original: {type(original_error).__name__}: {original_error}"
            )
        super().__init__(message, logger=logger)


class SourceReadError(ExtractError):
    """
    Excepción lanzada cuando ocurre un error al leer la fuente de datos.
    """

    def __init__(
        self,
        source_path: str,
        logger: logging.Logger,
        original_error: Optional[Exception] = None,
        details: Optional[str] = None,
    ):
        message = f"Error al leer la fuente de datos: '{source_path}'"
        if details:
            message += f". Detalles: {details}"
        if original_error:
            message += (
                f". Error original: {type(original_error).__name__}: {original_error}"
            )
        super().__init__(message, logger=logger)


class SourceNameNotSpecifiedError(ExtractError):
    """
    Excepción lanzada cuando no se especifica el nombre de la fuente de datos.
    """

    def __init__(
        self,
        logger: logging.Logger,
        extractor_type: str = "Extractor"
    ):
        message = (
            f"{extractor_type}: El nombre de la fuente de datos es requerido "
            "y no puede estar vacío."
        )
        super().__init__(message, logger=logger)
