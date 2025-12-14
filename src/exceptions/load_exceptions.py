"""
Módulo que define las excepciones para la capa de carga del proceso ETL.
"""

from exceptions.base import ETLError


class LoadError(ETLError):
    """
    Excepción base para errores que ocurren durante
    la fase de carga del proceso ETL.
    """
