"""
Validadores de schema y calidad de datos
"""

from typing import List, Dict, Optional

import pandas as pd

from utils.logger import extract_logger


class SchemaValidator:
    """
    Validador de schema para DataFrames.

    Proporciona métodos para validar columnas requeridas, tipos de datos,
    y otras restricciones de schema en DataFrames de pandas.

    Ejemplo de uso:
        validator = SchemaValidator(df)

        # Validar columnas requeridas
        validator.validate_required_columns(['id', 'name', 'price'])

        # Validar tipos de datos
        expected_types = {
            'id': 'int64',
            'name': 'object',
            'price': 'float64'
        }
        validator.validate_data_types(expected_types)

        # Validar rangos numéricos
        validator.validate_numeric_range('price', min_value=0)
    """

    def __init__(self, dataframe: pd.DataFrame):
        """
        Inicializa el validador con un DataFrame.

        Args:
            dataframe: DataFrame a validar
        """
        self.df = dataframe
        self.logger = extract_logger

    def validate_required_columns(self, required_columns: List[str]) -> bool:
        """
        Valida que todas las columnas requeridas estén presentes en el DataFrame.

        Args:
            required_columns: Lista de nombres de columnas que deben existir

        Returns:
            bool: True si todas las columnas están presentes

        Raises:
            ValueError: Si alguna columna requerida falta
        """
        actual_columns = set(self.df.columns)
        required_set = set(required_columns)
        missing_columns = required_set - actual_columns

        if missing_columns:
            error_msg = (
                f"Columnas faltantes en el DataFrame: {sorted(missing_columns)}. "
                f"Columnas presentes: {sorted(actual_columns)}"
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        success_msg = (
            f"Todas las columnas requeridas están presentes: {sorted(required_columns)}"
        )
        self.logger.info(success_msg)
        return True

    def validate_no_extra_columns(self, expected_columns: List[str]) -> bool:
        """
        Valida que no existan columnas adicionales no esperadas.

        Útil para validaciones estrictas donde el schema debe ser exacto.

        Args:
            expected_columns: Lista de columnas que deben existir (y solo esas)

        Returns:
            bool: True si no hay columnas extras

        Raises:
            ValueError: Si existen columnas no esperadas
        """
        actual_columns = set(self.df.columns)
        expected_set = set(expected_columns)
        extra_columns = actual_columns - expected_set

        if extra_columns:
            error_msg = (
                f"Columnas no esperadas en el DataFrame: {sorted(extra_columns)}. "
                f"Solo se esperaban: {sorted(expected_columns)}"
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        success_msg = "No hay columnas extras. Schema exacto validado."
        self.logger.info(success_msg)
        return True

    def validate_data_types(self, expected_types: Dict[str, str]) -> bool:
        """
        Valida que las columnas tengan los tipos de datos esperados.

        Args:
            expected_types: Diccionario {nombre_columna: tipo_esperado}
                           Ejemplos de tipos: 'int64', 'float64', 'object', 'datetime64[ns]'

        Returns:
            bool: True si todos los tipos coinciden

        Raises:
            ValueError: Si algún tipo no coincide
        """
        type_mismatches = []

        for column, expected_type in expected_types.items():
            self._check_column_in_df(column)

            actual_type = str(self.df[column].dtype)

            if not self._types_match(actual_type, expected_type):
                type_mismatches.append(
                    {
                        "columna": column,
                        "esperado": expected_type,
                        "actual": actual_type,
                    }
                )

        if type_mismatches:
            error_msg = f"Tipos de datos no coinciden: {type_mismatches}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        success_msg = f"Todos los tipos de datos son correctos: {expected_types}"
        self.logger.info(success_msg)
        return True

    def validate_numeric_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        allow_nulls: bool = True,
    ) -> bool:
        """
        Valida que los valores numéricos estén dentro de un rango esperado.

        Args:
            column: Nombre de la columna a validar
            min_value: Valor mínimo permitido (inclusive)
            max_value: Valor máximo permitido (inclusive)
            allow_nulls: Si False, falla si hay valores nulos

        Returns:
            bool: True si todos los valores están en rango

        Raises:
            ValueError: Si hay valores fuera de rango o nulos no permitidos
        """
        self._check_column_in_df(column)

        col_data = self.df[column]

        # Validar nulos
        if not allow_nulls and col_data.isnull().any():
            null_count = col_data.isnull().sum()
            error_msg = f"Columna '{column}' contiene {null_count} valores nulos (no permitidos)"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Trabajar solo con valores no nulos para validación de rango
        non_null_data = col_data.dropna()

        if len(non_null_data) == 0:
            warning_msg = (
                f"Columna '{column}' no tiene valores no-nulos para validar rango"
            )
            self.logger.warning(warning_msg)
            return True

        # Validar rango mínimo
        if min_value is not None:
            violations = non_null_data < min_value
            if violations.any():
                violation_count = violations.sum()
                min_found = non_null_data.min()
                error_msg = (
                    f"Columna '{column}': {violation_count} valores por debajo del mínimo "
                    f"permitido {min_value}. Valor mínimo encontrado: {min_found}"
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        # Validar rango máximo
        if max_value is not None:
            violations = non_null_data > max_value
            if violations.any():
                violation_count = violations.sum()
                max_found = non_null_data.max()
                error_msg = (
                    f"Columna '{column}': {violation_count} valores por encima del máximo "
                    f"permitido {max_value}. Valor máximo encontrado: {max_found}"
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        range_str = (
            f"[{min_value if min_value is not None else '-inf'}, "
            f"{max_value if max_value is not None else '+inf'}]"
        )
        success_msg = (
            f"Columna '{column}': todos los valores están en el rango {range_str}"
        )
        self.logger.info(success_msg)
        return True

    def validate_no_nulls(self, columns: Optional[List[str]] = None) -> bool:
        """
        Valida que las columnas especificadas no contengan valores nulos.

        Args:
            columns: Lista de columnas a validar. Si es None, valida todas las columnas.

        Returns:
            bool: True si no hay valores nulos

        Raises:
            ValueError: Si se encuentran valores nulos
        """
        cols_to_check = columns if columns else list(self.df.columns)

        columns_with_nulls = []
        for col in cols_to_check:
            self._check_column_in_df(col)

            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                null_percentage = round((null_count / len(self.df)) * 100, 2)
                columns_with_nulls.append(
                    {
                        "columna": col,
                        "valores_nulos": int(null_count),
                        "porcentaje": null_percentage,
                    }
                )

        if columns_with_nulls:
            error_msg = f"Columnas con valores nulos: {columns_with_nulls}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        success_msg = f"Ninguna columna contiene valores nulos: {cols_to_check}"
        self.logger.info(success_msg)
        return True

    def validate_unique_values(self, columns: List[str]) -> bool:
        """
        Valida que las columnas especificadas contengan valores únicos (sin duplicados).

        Útil para validar columnas de ID o claves primarias.

        Args:
            columns: Lista de columnas que deben tener valores únicos

        Returns:
            bool: True si todas las columnas tienen valores únicos

        Raises:
            ValueError: Si se encuentran valores duplicados
        """
        columns_with_duplicates = []

        for col in columns:
            self._check_column_in_df(col)

            duplicate_count = self.df[col].duplicated().sum()
            if duplicate_count > 0:
                columns_with_duplicates.append(
                    {"columna": col, "duplicados": int(duplicate_count)}
                )

        if columns_with_duplicates:
            error_msg = f"Columnas con valores duplicados: {columns_with_duplicates}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        success_msg = f"Todas las columnas tienen valores únicos: {columns}"
        self.logger.info(success_msg)
        return True

    def _check_column_in_df(self, column: str) -> None:
        """
        Verifica que una columna exista en el DataFrame.

        Args:
            column: Nombre de la columna a verificar

        Raises:
            ValueError: Si la columna no existe
        """
        if column not in self.df.columns:
            error_msg = f"Columna '{column}' no existe en el DataFrame"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    @staticmethod
    def _types_match(actual_type: str, expected_type: str) -> bool:
        """
        Compara dos tipos de datos con normalización flexible.

        Permite coincidencias como 'int64' con 'int', 'float32' con 'float', etc.

        Args:
            actual_type: Tipo de dato actual (ej: 'int64')
            expected_type: Tipo de dato esperado (ej: 'int' o 'int64')

        Returns:
            bool: True si los tipos coinciden
        """
        # Comparación exacta
        if actual_type == expected_type:
            return True

        # Normalización flexible
        type_mappings = {
            "int": ["int8", "int16", "int32", "int64"],
            "float": ["float16", "float32", "float64"],
            "string": ["object", "str"],
            "datetime": ["datetime64", "datetime64[ns]"],
        }

        for base_type, variants in type_mappings.items():
            if expected_type == base_type and actual_type in variants:
                return True
            if expected_type in variants and actual_type == base_type:
                return True

        return False
