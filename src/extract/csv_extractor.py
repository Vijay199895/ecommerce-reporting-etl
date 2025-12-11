"""
Extractor especializado para archivos CSV
"""

from typing import Optional, Dict, List
from pathlib import Path

import pandas as pd

from extract.base_extractor import BaseExtractor
from utils.validators import SchemaValidator


class CSVExtractor(BaseExtractor):
    """
    Extractor especializado para archivos CSV.

    Proporciona funcionalidad específica para leer archivos CSV con
    opciones configurables como encoding, separador, manejo de fechas, etc.

    Ejemplo de uso:
        # Caso simple
        extractor = CSVExtractor('data/raw/ecommerce_orders.csv')
        df = extractor.extract()

        # Con opciones avanzadas
        extractor = CSVExtractor(
            'data/raw/ecommerce_customers.csv',
            encoding='latin-1',
            sep=';',
            parse_dates=['created_at', 'updated_at']
        )
        df = extractor.extract()

        # Con validación de schema
        expected_cols = ['order_id', 'customer_id', 'order_date']
        extractor.validate_schema(expected_cols)
    """

    def __init__(
        self,
        file_path: str,
        encoding: str = "utf-8",
        sep: str = ",",
        parse_dates: Optional[List[str]] = None,
        auto_profile: bool = True,
        **kwargs,
    ):
        """
        Inicializa el extractor de CSV.

        Args:
            file_path: Ruta al archivo CSV
            encoding: Codificación del archivo (default: 'utf-8')
            sep: Separador de columnas (default: ',')
            parse_dates: Lista de columnas a parsear como fechas
            auto_profile: Si True, ejecuta profile_data() automáticamente después de extract()
            **kwargs: Argumentos adicionales para pd.read_csv()
            
        Raises:
            FileNotFoundError: Si el archivo no existe
        """
        super().__init__()
        self.file_path = Path(file_path)
        self._validate_source_exists()
        self.source_description = str(self.file_path)
        self.encoding = encoding
        self.sep = sep
        self.parse_dates = parse_dates or []
        self.auto_profile = auto_profile
        self.read_options = kwargs

        # Actualizar metadata con opciones de configuración
        self.metadata.update(
            {
                "encoding": encoding,
                "separator": sep,
                "file_path": str(self.file_path),
                "parse_dates": parse_dates,
            }
        )

    def _validate_source_exists(self) -> None:
        """
        Verifica que el archivo CSV especificado existe.

        Raises:
            FileNotFoundError: Si el archivo no existe
        """
        if not self.file_path.exists():
            error_msg = f"Archivo CSV no encontrado: {self.file_path}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        success_msg = f"Archivo CSV encontrado: {self.file_path.name}"
        self.logger.info(success_msg)
        print(success_msg)

    def extract(self) -> pd.DataFrame:
        """
        Extrae datos desde un archivo CSV.

        Returns:
            pd.DataFrame: DataFrame con los datos del CSV

        Raises:
            pd.errors.ParserError: Si hay errores parseando el CSV
            Exception: Para otros errores durante la extracción
        """
        try:
            # Preparar argumentos para pd.read_csv
            read_args = {
                "encoding": self.encoding,
                "sep": self.sep,
                **self.read_options,
            }

            # Agregar parse_dates solo si hay columnas especificadas
            if self.parse_dates:
                read_args["parse_dates"] = self.parse_dates

            self.data = pd.read_csv(self.file_path, **read_args)

            success_msg = (
                f"Datos extraídos exitosamente desde: {self.file_path.name} "
                f"({self.data.shape[0]} filas x {self.data.shape[1]} columnas)"
            )
            self.logger.info(success_msg)
            print(success_msg)

            # Actualizar timestamp de extracción
            self._update_extraction_timestamp()

            # Auto-profiling si está habilitado
            if self.auto_profile:
                self.profile_data()

            return self.data

        except pd.errors.ParserError as e:
            error_msg = (
                f"Error al parsear CSV {self.file_path.name}: {e}. "
                f"Verifica el separador (actual: '{self.sep}') y el formato del archivo."
            )
            self.logger.error(error_msg)
            raise pd.errors.ParserError(error_msg)

        except Exception as e:
            error_msg = f"Error al extraer datos desde {self.file_path.name}: {e}"
            self.logger.error(error_msg)
            raise

    def validate_schema(
        self,
        expected_columns: Optional[List[str]] = None,
        expected_dtypes: Optional[Dict[str, str]] = None,
        strict: bool = False,
    ) -> bool:
        """
        Valida el schema del DataFrame extraído.

        Args:
            expected_columns: Lista de columnas que deben estar presentes
            expected_dtypes: Diccionario con tipos de datos esperados {columna: tipo}
            strict: Si True, valida que SOLO existan las columnas esperadas (sin extras)

        Returns:
            bool: True si la validación es exitosa

        Raises:
            ValueError: Si no hay datos cargados o la validación falla
        """
        if self.data is None:
            raise ValueError("No hay datos cargados. Ejecuta extract() primero.")

        validator = SchemaValidator(self.data)

        # Validar columnas requeridas
        if expected_columns:
            validator.validate_required_columns(expected_columns)

            # Si es strict, validar que no hay columnas extras
            if strict:
                validator.validate_no_extra_columns(expected_columns)

        # Validar tipos de datos
        if expected_dtypes:
            validator.validate_data_types(expected_dtypes)

        success_msg = "Validación de schema exitosa"
        self.logger.info(success_msg)
        print(success_msg)

        return True

    def get_column_info(self) -> pd.DataFrame:
        """
        Retorna información detallada sobre cada columna del DataFrame.

        Incluye: tipo de dato, valores nulos, valores únicos, y muestra de valores.

        Returns:
            pd.DataFrame: DataFrame con información de columnas

        Raises:
            ValueError: Si no hay datos cargados
        """
        if self.data is None:
            raise ValueError("No hay datos cargados. Ejecuta extract() primero.")

        column_info = []

        for col in self.data.columns:
            info = {
                "columna": col,
                "tipo": str(self.data[col].dtype),
                "valores_nulos": int(self.data[col].isnull().sum()),
                "porcentaje_nulos": round(
                    (self.data[col].isnull().sum() / len(self.data)) * 100, 2
                ),
                "valores_unicos": int(self.data[col].nunique()),
                "muestra": str(self.data[col].iloc[0]) if len(self.data) > 0 else None,
            }
            column_info.append(info)

        return pd.DataFrame(column_info)
