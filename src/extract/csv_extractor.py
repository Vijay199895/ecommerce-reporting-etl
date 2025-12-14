"""
Extractor especializado para archivos CSV
"""

from pathlib import Path

import pandas as pd

from extract.base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """
    Extractor especializado para archivos CSV.

    Proporciona funcionalidad específica para leer archivos CSV con
    opciones configurables como encoding, separador, manejo de fechas, etc.
    """

    def __init__(self, source_path: str, encoding: str = "utf-8", sep: str = ","):
        """
        Inicializa el extractor de CSV.

        Args:
            source_path: Directorio base donde se encuentran los archivos CSV
            encoding: Codificación del archivo (default: 'utf-8')
            sep: Separador de columnas (default: ',')

        Raises:
            SourceNotFoundError: Si el archivo no existe
        """
        super().__init__()
        self._validate_source_exists(source_path)
        self.source_path = Path(source_path)
        self.source_description = str(self.source_path)
        self.encoding = encoding
        self.sep = sep

        # Actualizar metadata con opciones de configuración
        self.metadata.update(
            {
                "encoding": encoding,
                "separator": sep,
                "source_path": str(self.source_path),
            }
        )

    def _validate_source_exists(self, source_location: Path) -> None:
        """
        Verifica que el archivo CSV especificado existe.

        Raises:
            SourceNotFoundError: Si el archivo no existe
        """
        if not source_location.exists():
            error_msg = f"Archivo CSV no encontrado: {source_location}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        success_msg = f"Archivo CSV encontrado: {source_location.name}"
        self.logger.info(success_msg)
        print(success_msg)

    def extract(self, name: str) -> pd.DataFrame:
        """
        Extrae datos desde un archivo CSV.

        Args:
            name (str): Nombre o identificador del archivo (sin extensión)

        Returns:
            pd.DataFrame: DataFrame con los datos del CSV

        Raises:
            ValueError: Si el nombre del archivo está vacío
            Exception: Para otros errores durante la extracción
        """

        if not name:
            raise ValueError("El parámetro 'name' no puede estar vacío.")
        source_path = self.source_path / f"{name}.csv"
        try:
            df = pd.read_csv(source_path, encoding=self.encoding, sep=self.sep)
            self._profile_data(df)
            self._update_extraction_timestamp()
            summary = self.get_summary()
            self.logger.info("Datos extraídos desde %s:\n%s", source_path, summary)
            return df
        except Exception as exc:
            self.logger.error("Error al extraer CSV desde %s: %s", source_path, exc)
            raise
