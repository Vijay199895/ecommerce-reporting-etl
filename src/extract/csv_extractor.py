"""
Extractor especializado para archivos CSV
"""

from pathlib import Path

import pandas as pd

from exceptions import (
    SourceNameNotSpecifiedError,
    SourceNotFoundError,
    SourceParseError,
    SourceReadError,
)
from extract.base_extractor import BaseExtractor
from utils.logger import log_io_operation, extract_logger


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
        self._validate_source_exists(source_location=Path(source_path))
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

    @log_io_operation(
        operation="Validar existencia del directorio de origen", logger=extract_logger
    )
    def _validate_source_exists(self, source_location: Path) -> None:
        """
        Verifica que el archivo CSV especificado existe.

        Raises:
            SourceNotFoundError: Si el archivo no existe
        """
        if not source_location.exists():
            raise SourceNotFoundError(
                str(source_location),
                logger=extract_logger,
                source_type="directorio CSV",
            )

    @log_io_operation(
        operation="Extracción de datos en formato CSV", logger=extract_logger
    )
    def extract(self, name: str) -> pd.DataFrame:
        """
        Extrae datos desde un archivo CSV.

        Args:
            name (str): Nombre o identificador del archivo (sin extensión)

        Returns:
            pd.DataFrame: DataFrame con los datos del CSV

        Raises:
            SourceNameNotSpecifiedError: Si el nombre del archivo está vacío
            SourceNotFoundError: Si el archivo específico no existe
            SourceParseError: Si hay error al parsear el CSV
            SourceReadError: Para otros errores durante la lectura
        """

        if not name:
            raise SourceNameNotSpecifiedError(
                logger=extract_logger, extractor_type="CSVExtractor"
            )

        source_path = self.source_path / f"{name}.csv"

        # Se valida que el archivo específico existe
        if not source_path.exists():
            raise SourceNotFoundError(
                str(source_path), logger=extract_logger, source_type="archivo CSV"
            )

        try:
            df = pd.read_csv(source_path, encoding=self.encoding, sep=self.sep)
            self._profile_data(df)
            self._update_extraction_timestamp()
            return df
        except pd.errors.ParserError as exc:
            raise SourceParseError(
                str(source_path),
                logger=extract_logger,
                original_error=exc,
                details=f"encoding={self.encoding}, sep='{self.sep}'",
            ) from exc
        except pd.errors.EmptyDataError as exc:
            raise SourceParseError(
                str(source_path),
                logger=extract_logger,
                original_error=exc,
                details="El archivo está vacío o no contiene datos válidos",
            ) from exc
        except UnicodeDecodeError as exc:
            raise SourceReadError(
                str(source_path),
                logger=extract_logger,
                original_error=exc,
                details=f"Encoding configurado: {self.encoding}. Pruebe con otro encoding.",
            ) from exc
        except OSError as exc:
            raise SourceReadError(
                str(source_path),
                logger=extract_logger,
                original_error=exc,
                details="Error de sistema al acceder al archivo",
            ) from exc
        except Exception as exc:
            raise SourceReadError(
                str(source_path),
                logger=extract_logger,
                original_error=exc,
                details="Error inesperado durante la extracción",
            ) from exc
