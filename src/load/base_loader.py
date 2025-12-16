"""
Clase base abstracta para loaders de datos.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict

import pandas as pd


class BaseLoader(ABC):
    """
    Clase base abstracta para todos los loaders de datos.
    Define la interfaz común para guardar DataFrames en distintos destinos.

    NO asume ningún destino específico (archivos, DB, API, etc.).
    Cada subclase define qué parámetros necesita en su constructor.
    """

    def __init__(self):
        self.target_description: str = ""
        self.metadata: Dict[str, Any] = {
            "loader_type": self.__class__.__name__,
            "target": None,
            "load_timestamp": None,
        }

    @abstractmethod
    def save(self, df: pd.DataFrame, name: str) -> None:
        """
        Carga datos hacia el destino.

        Args:
            df (pd.DataFrame): DataFrame con los datos a cargar
            name (str): Nombre o identificador para el destino
        """
        raise NotImplementedError()

    def get_summary(self) -> str:
        """
        Genera un resumen legible de la carga.

        Returns:
            str: Resumen formateado de la carga
        """
        summary_lines = [f"{key}: {value}" for key, value in self.metadata.items()]
        return "\n".join(summary_lines)

    @abstractmethod
    def _validate_target_exists(self, target_location: Any) -> None:
        """
        Verifica que el destino de datos especificada existe.

        Args:
            target_location (Any): Ubicación o identificador del destino

        Raises:
            TargetNotFoundError: Si el destino no existe
        """
        raise NotImplementedError()

    @abstractmethod
    def _profile_data_after_load(self, after_load_info: Any) -> None:
        """
        Genera un perfil completo de los datos después de la carga y actualiza la metadata.

        Incluye información relevante sobre el estado de los datos tras la carga.
        Esta información es útil para auditoría y debugging.
        """
        raise NotImplementedError()

    def _profile_data_before_load(self, df: pd.DataFrame) -> None:
        """
        Genera un perfil completo de los datos antes de la carga y actualiza la metadata.

        Incluye información sobre dimensiones, valores nulos, tipos de datos
        y uso de memoria. Esta información es útil para auditoría y debugging.
        """
        profile = {
            "rows": df.shape[0],
            "columns": df.shape[1],
            "missing_values": int(df.isnull().sum().sum()),
            "missing_percentage": round(
                (df.isnull().sum().sum() / max(df.shape[0] * df.shape[1], 1)) * 100, 2
            ),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        }

        self.metadata.update(profile)

    def _update_load_timestamp(self) -> None:
        """
        Actualiza el timestamp de carga en la metadata.

        Este método debe ser llamado por las subclases al finalizar
        exitosamente la carga.
        """
        self.metadata["load_timestamp"] = datetime.now().isoformat()
