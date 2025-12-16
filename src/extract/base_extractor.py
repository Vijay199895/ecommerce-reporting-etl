"""
Clase base abstracta para extractores de datos
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime

import pandas as pd


class BaseExtractor(ABC):
    """
    Clase base abstracta para todos los extractores de datos.
    Define la interfaz común para extracción, validación y profiling.

    NO asume ninguna fuente específica (archivos, DB, API, etc.).
    Cada subclase define qué parámetros necesita en su constructor.
    """

    def __init__(self):
        self.source_description: str = ""
        self.metadata: Dict[str, Any] = {
            "extraction_timestamp": None,
            "extractor_type": self.__class__.__name__,
        }

    @abstractmethod
    def extract(self, name: str) -> pd.DataFrame:
        """
        Extrae datos desde la fuente.

        Args:
            name (str): Nombre o identificador de la fuente.

        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        raise NotImplementedError()

    def get_summary(self) -> str:
        """
        Genera un resumen legible de la extracción.

        Returns:
            str: Resumen formateado de la extracción
        """
        summary_lines = [f"{key}: {value}" for key, value in self.metadata.items()]
        return "\n".join(summary_lines)

    @abstractmethod
    def _validate_source_exists(self, source_location: Any) -> None:
        """
        Verifica que la fuente de datos especificada existe.

        Args:
            source_location (Any): Ubicación o identificador de la fuente

        Raises:
            SourceNotFoundError: Si la fuente no existe
        """
        raise NotImplementedError()

    def _profile_data(self, df: pd.DataFrame) -> None:
        """
        Genera un perfil completo de los datos extraídos.

        Incluye información sobre dimensiones, valores nulos, tipos de datos
        y uso de memoria. Esta información es útil para auditoría y debugging.
        """
        profile = {
            "rows": df.shape[0],
            "columns": df.shape[1],
            "column_names": list(df.columns),
            "missing_values": int(df.isnull().sum().sum()),
            "missing_percentage": round(
                (df.isnull().sum().sum() / max(df.shape[0] * df.shape[1], 1)) * 100,
                2,
            ),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        }

        self.metadata.update(profile)

    def _update_extraction_timestamp(self) -> None:
        """
        Actualiza el timestamp de extracción en la metadata.

        Este método debe ser llamado por las subclases al finalizar
        exitosamente la extracción.
        """
        self.metadata["extraction_timestamp"] = datetime.now().isoformat()
