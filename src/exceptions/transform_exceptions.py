"""
Módulo que define las excepciones para la capa de transformación del proceso ETL.
"""

from exceptions.base import ETLError


class TransformError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de transformación del proceso ETL.
    """
