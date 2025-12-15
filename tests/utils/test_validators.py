"""
Pruebas unitarias para el validador de esquemas (SchemaValidator).

Verifica las validaciones de schema y calidad de datos incluyendo:
- Columnas requeridas y extras
- Tipos de datos esperados
- Rangos numéricos válidos
- Valores nulos no permitidos
- Unicidad de valores (duplicados)

Las pruebas utilizan excepciones personalizadas del módulo exceptions:
- MissingRequiredColumnsError: columnas requeridas faltantes
- UnexpectedColumnsError: columnas no esperadas en el schema
- DataTypeMismatchError: tipos de datos incorrectos
- NullConstraintError: valores nulos no permitidos
- RangeValidationError: valores fuera de rango
- DuplicateKeyError: valores duplicados en columnas únicas
"""

import logging
import pytest
import pytest_check as check
import pandas as pd

from exceptions import (
    MissingRequiredColumnsError,
    UnexpectedColumnsError,
    DataTypeMismatchError,
    NullConstraintError,
    RangeValidationError,
    DuplicateKeyError,
)
from utils.validators import SchemaValidator


class MockLogger(logging.Logger):
    """
    Logger simulado para pruebas unitarias.

    Extiende logging.Logger para mantener compatibilidad con el constructor
    de SchemaValidator que requiere un logger válido.
    """

    def __init__(self):
        super().__init__(name="mock_logger", level=logging.DEBUG)

    def log(self, level: int, msg: str, *args, **kwargs) -> None:
        pass

    def error(self, msg: str, *args, **kwargs) -> None:
        pass

    def info(self, msg: str, *args, **kwargs) -> None:
        pass

    def warning(self, msg: str, *args, **kwargs) -> None:
        pass


@pytest.mark.unit
@pytest.mark.smoke
class TestSchemaValidatorRequiredColumns:
    """
    Tests para el método validate_required_columns().

    Valida que el DataFrame contenga todas las columnas requeridas
    y lance MissingRequiredColumnsError cuando falten columnas.
    """

    def test_validate_required_columns_should_pass_when_all_columns_present(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar la validación cuando todas las columnas requeridas están presentes.

        Escenario positivo básico: todas las columnas solicitadas existen en el DataFrame.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        required = ["id", "name", "price"]

        result = validator.validate_required_columns(required)

        check.is_true(result, "Debería retornar True cuando todas las columnas existen")

    def test_validate_required_columns_should_pass_when_dataframe_has_extra_columns(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar incluso si hay columnas adicionales.

        Solo valida la presencia de columnas requeridas, ignorando extras.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        required = ["id", "name"]

        result = validator.validate_required_columns(required)

        check.is_true(result)
        check.equal(
            len(sample_valid_dataframe.columns),
            6,
            "El DataFrame debe mantener todas sus columnas originales",
        )

    def test_validate_required_columns_should_raise_missing_columns_error_when_column_missing(
        self, dataframe_missing_required_columns
    ):
        """
        Debe lanzar MissingRequiredColumnsError cuando falta una columna requerida.

        Verifica que la excepción incluya información sobre las columnas faltantes.
        """
        validator = SchemaValidator(dataframe_missing_required_columns, MockLogger())
        required = ["id", "name", "price"]

        with pytest.raises(MissingRequiredColumnsError) as exc_info:
            validator.validate_required_columns(required)

        check.is_in("price", str(exc_info.value))
        check.is_in("faltantes", str(exc_info.value).lower())

    def test_validate_required_columns_should_raise_error_with_all_missing_columns(
        self, dataframe_missing_required_columns
    ):
        """
        Debe reportar todas las columnas faltantes en el mensaje de error.

        Permite identificar múltiples problemas de schema en una sola validación.
        """
        validator = SchemaValidator(dataframe_missing_required_columns, MockLogger())
        required = ["id", "name", "price", "quantity", "category"]

        with pytest.raises(MissingRequiredColumnsError) as exc_info:
            validator.validate_required_columns(required)

        error_msg = str(exc_info.value)
        check.is_in("price", error_msg)
        check.is_in("quantity", error_msg)
        check.is_in("category", error_msg)

    def test_validate_required_columns_should_pass_when_empty_list_provided(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando no se requieren columnas específicas.

        Lista vacía de requerimientos siempre pasa la validación.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        required = []

        result = validator.validate_required_columns(required)

        check.is_true(result)

    def test_validate_required_columns_should_be_case_sensitive(
        self, sample_valid_dataframe
    ):
        """
        Debe ser sensible a mayúsculas/minúsculas en nombres de columnas.

        'ID' != 'id', por lo tanto debe fallar si el case no coincide.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        required = ["ID", "Name", "Price"]

        with pytest.raises(MissingRequiredColumnsError):
            validator.validate_required_columns(required)


@pytest.mark.unit
class TestSchemaValidatorExtraColumns:
    """
    Tests para el método validate_no_extra_columns().

    Valida que el DataFrame no contenga columnas no esperadas
    y lance UnexpectedColumnsError cuando existan extras.
    """

    def test_validate_no_extra_columns_should_pass_when_schema_exact_match(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando el schema es exactamente el esperado.

        Validación estricta: solo las columnas esperadas, sin extras.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected = list(sample_valid_dataframe.columns)

        result = validator.validate_no_extra_columns(expected)

        check.is_true(result)

    def test_validate_no_extra_columns_should_raise_unexpected_columns_error(
        self, dataframe_with_extra_columns
    ):
        """
        Debe lanzar UnexpectedColumnsError cuando hay columnas adicionales no esperadas.

        La excepción debe incluir los nombres de las columnas extras encontradas.
        """
        validator = SchemaValidator(dataframe_with_extra_columns, MockLogger())
        expected = ["id", "name", "price"]

        with pytest.raises(UnexpectedColumnsError) as exc_info:
            validator.validate_no_extra_columns(expected)

        error_msg = str(exc_info.value)
        check.is_in("extra_col_1", error_msg)
        check.is_in("extra_col_2", error_msg)

    def test_validate_no_extra_columns_should_fail_for_subset_of_columns(
        self, sample_valid_dataframe
    ):
        """
        Debe fallar si esperamos menos columnas de las que existen.

        Validación estricta: no permite columnas adicionales.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected = ["id", "name"]

        with pytest.raises(UnexpectedColumnsError):
            validator.validate_no_extra_columns(expected)

    def test_validate_no_extra_columns_should_work_with_empty_dataframe(self):
        """
        Debe funcionar correctamente con DataFrames vacíos.

        El schema de columnas se valida independiente del número de filas.
        """
        df = pd.DataFrame(columns=["a", "b", "c"])
        validator = SchemaValidator(df, MockLogger())
        expected = ["a", "b", "c"]

        result = validator.validate_no_extra_columns(expected)

        check.is_true(result)


@pytest.mark.unit
class TestSchemaValidatorDataTypes:
    """
    Tests para el método validate_data_types().

    Valida que las columnas tengan los tipos de datos esperados
    y lance DataTypeMismatchError cuando los tipos no coincidan.
    """

    def test_validate_data_types_should_pass_when_all_types_correct(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando todos los tipos de datos son correctos.

        Valida tipos exactos como int64, object, float64.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected_types = {
            "id": "int64",
            "name": "object",
            "price": "float64",
            "quantity": "int64",
        }

        result = validator.validate_data_types(expected_types)

        check.is_true(result)

    def test_validate_data_types_should_raise_data_type_mismatch_error(
        self, dataframe_with_wrong_types
    ):
        """
        Debe lanzar DataTypeMismatchError cuando hay discrepancia de tipos.

        La excepción incluye las columnas con tipos incorrectos y sus valores actuales.
        """
        validator = SchemaValidator(dataframe_with_wrong_types, MockLogger())
        expected_types = {
            "id": "int64",
            "price": "float64",
        }

        with pytest.raises(DataTypeMismatchError) as exc_info:
            validator.validate_data_types(expected_types)

        error_msg = str(exc_info.value)
        check.is_in("id", error_msg)
        check.is_in("price", error_msg)

    def test_validate_data_types_should_validate_datetime_types(
        self, sample_valid_dataframe
    ):
        """
        Debe validar correctamente tipos datetime64.

        Soporta el formato datetime64[ns] de pandas.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected_types = {"created_at": "datetime64[ns]"}

        result = validator.validate_data_types(expected_types)

        check.is_true(result)

    def test_validate_data_types_should_handle_partial_validation(
        self, sample_valid_dataframe
    ):
        """
        Debe permitir validar solo un subconjunto de columnas.

        Útil para validaciones focalizadas en columnas críticas.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected_types = {"id": "int64", "price": "float64"}

        result = validator.validate_data_types(expected_types)

        check.is_true(result)

    def test_validate_data_types_should_raise_error_when_column_not_exists(
        self, sample_valid_dataframe
    ):
        """
        Debe lanzar MissingRequiredColumnsError si la columna no existe.

        Valida existencia de columna antes de verificar el tipo.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())
        expected_types = {"nonexistent_column": "int64"}

        with pytest.raises(MissingRequiredColumnsError):
            validator.validate_data_types(expected_types)


@pytest.mark.unit
class TestSchemaValidatorNumericRanges:
    """
    Tests para el método validate_numeric_range().

    Valida que valores numéricos estén dentro de rangos esperados
    y lance RangeValidationError o NullConstraintError según corresponda.
    """

    def test_validate_numeric_range_should_pass_when_all_values_in_range(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando todos los valores están en el rango.

        Validación básica con límites mínimo y máximo.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_numeric_range("price", min_value=0, max_value=100)

        check.is_true(result)

    def test_validate_numeric_range_should_raise_range_error_below_minimum(
        self, dataframe_with_invalid_ranges
    ):
        """
        Debe lanzar RangeValidationError cuando hay valores menores al mínimo.

        La excepción incluye información sobre la cantidad de violaciones.
        """
        validator = SchemaValidator(dataframe_with_invalid_ranges, MockLogger())

        with pytest.raises(RangeValidationError) as exc_info:
            validator.validate_numeric_range("price", min_value=0)

        error_msg = str(exc_info.value)
        check.is_in("fuera del rango", error_msg.lower())

    def test_validate_numeric_range_should_raise_range_error_above_maximum(
        self, dataframe_with_invalid_ranges
    ):
        """
        Debe lanzar RangeValidationError cuando hay valores mayores al máximo.

        Detecta valores que superan el límite superior permitido.
        """
        validator = SchemaValidator(dataframe_with_invalid_ranges, MockLogger())

        with pytest.raises(RangeValidationError) as exc_info:
            validator.validate_numeric_range("price", max_value=100)

        error_msg = str(exc_info.value)
        check.is_in("fuera del rango", error_msg.lower())

    def test_validate_numeric_range_should_handle_only_minimum(
        self, sample_valid_dataframe
    ):
        """
        Debe validar solo el mínimo cuando no se especifica máximo.

        Permite validaciones abiertas hacia arriba.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_numeric_range("quantity", min_value=0)

        check.is_true(result)

    def test_validate_numeric_range_should_handle_only_maximum(
        self, sample_valid_dataframe
    ):
        """
        Debe validar solo el máximo cuando no se especifica mínimo.

        Permite validaciones abiertas hacia abajo.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_numeric_range("quantity", max_value=1000)

        check.is_true(result)

    def test_validate_numeric_range_should_allow_nulls_by_default(
        self, dataframe_with_nulls
    ):
        """
        Debe permitir valores nulos por defecto (allow_nulls=True).

        Los nulos se ignoran en la validación de rango.
        """
        validator = SchemaValidator(dataframe_with_nulls, MockLogger())

        result = validator.validate_numeric_range(
            "price", min_value=0, max_value=100, allow_nulls=True
        )

        check.is_true(result)

    def test_validate_numeric_range_should_raise_null_constraint_when_nulls_not_allowed(
        self, dataframe_with_nulls
    ):
        """
        Debe lanzar NullConstraintError cuando hay nulos y allow_nulls=False.

        Detecta nulos antes de validar el rango.
        """
        validator = SchemaValidator(dataframe_with_nulls, MockLogger())

        with pytest.raises(NullConstraintError) as exc_info:
            validator.validate_numeric_range(
                "price", min_value=0, max_value=100, allow_nulls=False
            )

        check.is_in("nulos", str(exc_info.value).lower())

    def test_validate_numeric_range_should_validate_inclusive_boundaries(
        self, sample_valid_dataframe
    ):
        """
        Debe validar que los límites son inclusivos.

        Valores exactamente en los límites son válidos.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_numeric_range(
            "price", min_value=10.50, max_value=30.00
        )

        check.is_true(result, "Los valores en los límites deben ser válidos")


@pytest.mark.unit
@pytest.mark.smoke
class TestSchemaValidatorNullValues:
    """
    Tests para el método validate_no_nulls().

    Valida que las columnas no contengan valores nulos
    y lance NullConstraintError cuando se encuentren nulos.
    """

    def test_validate_no_nulls_should_pass_when_no_nulls_exist(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando no hay valores nulos.

        DataFrame completamente poblado pasa la validación.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_no_nulls()

        check.is_true(result)

    def test_validate_no_nulls_should_raise_null_constraint_error(
        self, dataframe_with_nulls
    ):
        """
        Debe lanzar NullConstraintError cuando hay valores nulos.

        La excepción incluye las columnas afectadas y conteo de nulos.
        """
        validator = SchemaValidator(dataframe_with_nulls, MockLogger())

        with pytest.raises(NullConstraintError) as exc_info:
            validator.validate_no_nulls()

        error_msg = str(exc_info.value)
        check.is_in("nulos", error_msg.lower())

    def test_validate_no_nulls_should_validate_specific_columns_only(
        self, dataframe_with_nulls
    ):
        """
        Debe validar solo las columnas especificadas.

        Permite validaciones focalizadas en columnas críticas.
        """
        validator = SchemaValidator(dataframe_with_nulls, MockLogger())
        columns_to_check = ["id"]

        with pytest.raises(NullConstraintError) as exc_info:
            validator.validate_no_nulls(columns=columns_to_check)

        error_msg = str(exc_info.value)
        check.is_in("id", error_msg)

    def test_validate_no_nulls_should_pass_when_other_columns_have_nulls(self):
        """
        Debe pasar si solo validamos columnas sin nulos.

        Otras columnas con nulos no afectan la validación.
        """
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": [None, "B", None],
                "price": [10.0, None, 30.0],
            }
        )
        validator = SchemaValidator(df, MockLogger())

        result = validator.validate_no_nulls(columns=["id"])

        check.is_true(result)

    def test_validate_no_nulls_should_report_null_counts(self, dataframe_with_nulls):
        """
        Debe reportar la cantidad de nulos encontrados.

        El mensaje de error incluye información cuantitativa.
        """
        validator = SchemaValidator(dataframe_with_nulls, MockLogger())

        with pytest.raises(NullConstraintError) as exc_info:
            validator.validate_no_nulls()

        error_msg = str(exc_info.value)
        check.is_in("1", error_msg)


@pytest.mark.unit
class TestSchemaValidatorUniqueValues:
    """
    Tests para el método validate_unique_values().

    Valida que las columnas contengan solo valores únicos
    y lance DuplicateKeyError cuando existan duplicados.
    """

    def test_validate_unique_values_should_pass_when_all_values_unique(
        self, sample_valid_dataframe
    ):
        """
        Debe pasar cuando todos los valores son únicos.

        Columnas de identificador sin duplicados.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_unique_values(["id"])

        check.is_true(result)

    def test_validate_unique_values_should_raise_duplicate_key_error(
        self, dataframe_with_duplicates
    ):
        """
        Debe lanzar DuplicateKeyError cuando hay valores duplicados.

        La excepción incluye las columnas afectadas y conteo de duplicados.
        """
        validator = SchemaValidator(dataframe_with_duplicates, MockLogger())

        with pytest.raises(DuplicateKeyError) as exc_info:
            validator.validate_unique_values(["id"])

        error_msg = str(exc_info.value)
        check.is_in("id", error_msg)
        check.is_in("duplicado", error_msg.lower())

    def test_validate_unique_values_should_validate_multiple_columns(
        self, sample_valid_dataframe
    ):
        """
        Debe validar múltiples columnas a la vez.

        Permite validar unicidad en varias columnas en una llamada.
        """
        validator = SchemaValidator(sample_valid_dataframe, MockLogger())

        result = validator.validate_unique_values(["id", "name"])

        check.is_true(result)

    def test_validate_unique_values_should_report_duplicate_count(
        self, dataframe_with_duplicates
    ):
        """
        Debe reportar cuántos duplicados se encontraron.

        El mensaje incluye el número total de valores duplicados.
        """
        validator = SchemaValidator(dataframe_with_duplicates, MockLogger())

        with pytest.raises(DuplicateKeyError) as exc_info:
            validator.validate_unique_values(["id"])

        error_msg = str(exc_info.value)
        check.is_in("1", error_msg)

    def test_validate_unique_values_should_report_all_columns_with_duplicates(
        self, dataframe_with_duplicates
    ):
        """
        Debe reportar todas las columnas con duplicados.

        Permite identificar múltiples problemas de unicidad.
        """
        validator = SchemaValidator(dataframe_with_duplicates, MockLogger())

        with pytest.raises(DuplicateKeyError) as exc_info:
            validator.validate_unique_values(["id", "name"])

        error_msg = str(exc_info.value)
        check.is_in("id", error_msg)
        check.is_in("name", error_msg)
