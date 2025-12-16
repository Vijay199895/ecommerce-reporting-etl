"""
Loader especializado para archivos CSV
"""

from pathlib import Path

import pandas as pd

from exceptions import LoadWriteError, TargetNameNotSpecifiedError, TargetNotFoundError
from load.base_loader import BaseLoader
from utils.logger import log_io_operation, load_logger


class CSVLoader(BaseLoader):
    """
    Loader especializado para archivos CSV.

    Proporciona funcionalidad específica para cargar archivos CSV con
    opciones configurables como encoding, separador, inclusión de índice, etc.
    """

    def __init__(
        self,
        target_path: str,
        encoding: str = "utf-8",
        sep: str = ",",
        index: bool = False,
    ):
        """
        Inicializa el loader de CSV.

        Args:
            target_path: Directorio base donde se guardaran los archivos CSV
            encoding: Codificación del archivo (default: 'utf-8')
            sep: Separador de columnas (default: ',')
            index: Incluir índice en el archivo (default: False)

        Raises:
            TargetNotFoundError: Si el directorio de destino no existe
        """
        super().__init__()
        self._validate_target_exists(target_location=Path(target_path))
        self.target_path = Path(target_path)
        self.target_description = str(self.target_path)
        self.encoding = encoding
        self.sep = sep
        self.save_index = index

        # Actualizar metadata con opciones de configuración
        self.metadata.update(
            {
                "target_path": str(self.target_path),
                "encoding": self.encoding,
                "separator": self.sep,
            }
        )

    @log_io_operation(operation="Carga de datos en formato CSV", logger=load_logger)
    def save(self, df: pd.DataFrame, name: str) -> None:
        """
        Carga datos hacia un archivo CSV.

        Raises:
            TargetNameNotSpecifiedError: Si el nombre del archivo está vacío
            LoadWriteError: Si ocurre un error durante la escritura
        """

        if not name:
            raise TargetNameNotSpecifiedError(
                logger=load_logger, loader_type="CSVLoader"
            )

        target_path = self.target_path / f"{name}.csv"

        # Se hace profiling antes y después del guardado
        self._profile_data_before_load(df)
        try:
            df.to_csv(
                target_path, index=self.save_index, encoding=self.encoding, sep=self.sep
            )
        except PermissionError as exc:
            raise LoadWriteError(
                str(target_path),
                logger=load_logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "encoding": self.encoding,
                    "reason": "Permiso denegado",
                },
            ) from exc
        except OSError as exc:
            raise LoadWriteError(
                str(target_path),
                logger=load_logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "encoding": self.encoding,
                    "reason": "Error de sistema de archivos",
                },
            ) from exc
        except Exception as exc:
            raise LoadWriteError(
                str(target_path),
                logger=load_logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "encoding": self.encoding,
                },
            ) from exc

        self._profile_data_after_load(target_path)

    @log_io_operation(
        operation="Validar existencia del directorio de destino", logger=load_logger
    )
    def _validate_target_exists(self, target_location: Path) -> None:
        """
        Verifica que el destino de datos especificada existe.

        Args:
            target_location (Path): Ubicación o identificador del destino

        Raises:
            TargetNotFoundError: Si el destino no existe
        """
        if not target_location.exists():
            raise TargetNotFoundError(
                str(target_location), logger=load_logger, target_type="directorio"
            )

    def _profile_data_after_load(self, after_load_info: Path) -> None:
        """
        Genera un perfil completo de los datos después de la carga y actualiza la metadata.

        Mide el tamaño del archivo csv resultante en megabytes y actualiza el timestamp de carga.

        Args:
            after_load_info (Path): Ruta al archivo CSV guardado
        """
        file_size_mb = round(after_load_info.stat().st_size / 1024**2, 2)
        self.metadata.update(
            {
                "target": str(after_load_info),
                "file_size_mb": file_size_mb,
            }
        )
        self._update_load_timestamp()
