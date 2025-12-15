"""
Módulo que define las excepciones para la capa de carga del proceso ETL.
"""

import logging
from typing import Any, Dict, Optional

from exceptions.base import ETLError


class LoadError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de carga del proceso ETL.
    """


class TargetNotFoundError(LoadError):
    """
    Excepción lanzada cuando no se encuentra el destino de datos especificado.
    """

    def __init__(
        self, target_path: str, logger: logging.Logger, target_type: str = "directorio"
    ):
        message = f"No se encontró el {target_type} de destino: '{target_path}'"
        super().__init__(message, logger=logger)


class TargetNameNotSpecifiedError(LoadError):
    """
    Excepción lanzada cuando no se especifica el nombre del destino de datos.
    """

    def __init__(self, logger: logging.Logger, loader_type: str = "Loader"):
        message = (
            f"{loader_type}: El nombre del destino es requerido y no puede estar vacío."
        )
        super().__init__(message, logger=logger)


class LoadWriteError(LoadError):
    """
    Excepción lanzada cuando ocurre un error al escribir en el destino de datos.
    """

    def __init__(
        self,
        target_path: str,
        logger: logging.Logger,
        original_error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        message = f"Error al escribir en el destino: '{target_path}'"
        if context:
            context_str = ", ".join(f"{k}={v}" for k, v in context.items())
            message += f". Contexto: [{context_str}]"
        if original_error:
            message += (
                f". Error original: {type(original_error).__name__}: {original_error}"
            )
        super().__init__(message, logger=logger)
