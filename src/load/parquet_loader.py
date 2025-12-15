"""
Loader especializado para archivos Parquet.
"""

from pathlib import Path

import pandas as pd

from exceptions import LoadWriteError, TargetNameNotSpecifiedError, TargetNotFoundError
from load.base_loader import BaseLoader


class ParquetLoader(BaseLoader):
    """
    Loader especializado para archivos Parquet.

    Proporciona funcionalidad específica para cargar archivos Parquet con
    opciones configurables como compresión, inclusión de índice, motor de lectura, etc.
    """

    def __init__(
        self,
        target_path: str,
        compression: str = "snappy",
        index: bool = False,
        engine: str = "pyarrow",
    ):
        """
        Inicializa el loader de Parquet.

        Args:
            target_path: Directorio base donde se guardaran los archivos Parquet
            compression: Método de compresión (default: 'snappy')
            index: Incluir índice en el archivo (default: False)
            engine: Motor para leer/escribir Parquet (default: 'pyarrow')

        Raises:
            TargetNotFoundError: Si el directorio de destino no existe
        """
        super().__init__()
        self._validate_target_exists(Path(target_path))
        self.target_path = Path(target_path)
        self.target_description = str(self.target_path)
        self.compression = compression
        self.save_index = index
        self.engine = engine

        # Actualizar metadata con opciones de configuración
        self.metadata.update(
            {
                "target_path": str(self.target_path),
                "compression": self.compression,
                "engine": self.engine,
            }
        )

    def save(self, df: pd.DataFrame, name: str) -> None:
        """
        Carga datos hacia un archivo Parquet.

        Raises:
            TargetNameNotSpecifiedError: Si el nombre del archivo está vacío
            LoadWriteError: Si ocurre un error durante la escritura
        """
        if not name:
            raise TargetNameNotSpecifiedError(
                logger=self.logger, loader_type="ParquetLoader"
            )

        target_path = self.target_path / f"{name}.parquet"

        # Se hace profiling antes y después del guardado
        self._profile_data_before_load(df)
        try:
            df.to_parquet(
                target_path,
                index=self.save_index,
                compression=self.compression,
                engine=self.engine,
            )
        except PermissionError as exc:
            raise LoadWriteError(
                str(target_path),
                logger=self.logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "compression": self.compression,
                    "engine": self.engine,
                    "reason": "Permiso denegado",
                },
            ) from exc
        except ImportError as exc:
            raise LoadWriteError(
                str(target_path),
                logger=self.logger,
                original_error=exc,
                context={
                    "engine": self.engine,
                    "reason": f"Motor '{self.engine}' no está instalado",
                },
            ) from exc
        except OSError as exc:
            raise LoadWriteError(
                str(target_path),
                logger=self.logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "compression": self.compression,
                    "engine": self.engine,
                    "reason": "Error de sistema de archivos",
                },
            ) from exc
        except Exception as exc:
            raise LoadWriteError(
                str(target_path),
                logger=self.logger,
                original_error=exc,
                context={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "compression": self.compression,
                    "engine": self.engine,
                },
            ) from exc

        self._profile_data_after_load(target_path)

        # Una vez el profiling completo se ha hecho, se puede loguear el resumen
        summary = self.get_summary()
        success_msg = f"Archivo Parquet guardado en {target_path}:\n{summary}"
        self.logger.info(success_msg)

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
                str(target_location), logger=self.logger, target_type="directorio"
            )
        success_msg = f"Directorio de destino encontrado: {target_location}"
        self.logger.info(success_msg)
        print(success_msg)

    def _profile_data_after_load(self, after_load_info: Path) -> None:
        """
        Genera un perfil completo de los datos después de la carga y actualiza la metadata.

        Mide el tamaño del archivo parquet resultante en megabytes y actualiza el timestamp de carga.

        Args:
            after_load_info (Path): Ruta al archivo Parquet guardado
        """
        file_size_mb = round(after_load_info.stat().st_size / 1024**2, 2)
        self.metadata.update(
            {
                "target": str(after_load_info),
                "file_size_mb": file_size_mb,
            }
        )
        self._update_load_timestamp()
