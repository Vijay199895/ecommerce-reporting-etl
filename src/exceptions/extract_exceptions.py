"""
Módulo que define las excepciones para la capa de extracción del proceso ETL.
"""

from exceptions.base import ETLError


class ExtractError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de extracción del proceso ETL.
    """
