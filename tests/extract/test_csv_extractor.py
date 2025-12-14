"""
Pruebas unitarias para extractor de CSV.
"""

from pathlib import Path

import pandas as pd
import pytest
import pytest_check as check

from extract.csv_extractor import CSVExtractor


class TestCSVExtractorExtract:
    """
    Tests para la extracción del CSVExtractor.
    Valida que se extraiga correctamente el archivo CSV y se actualice la metadata.
    """

    def test_extract_should_return_dataframe_and_metadata_when_csv_valid(
        self, tmp_path: Path
    ) -> None:
        """
        Debe retornar DataFrame y actualizar metadata cuando el CSV es válido.
        """
        # Crear CSV de prueba en el directorio temporal
        csv_content = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 102, 103],
                "total": [100.0, 200.0, 150.0],
            }
        )
        csv_content.to_csv(tmp_path / "orders.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)
        result = extractor.extract(name="orders")

        metadata = extractor.metadata

        check.equal(len(result), 3)
        check.equal(list(result.columns), ["order_id", "customer_id", "total"])
        check.equal(metadata["rows"], 3)
        check.equal(metadata["columns"], 3)
        check.equal(metadata["missing_values"], 0)
        check.is_not_none(metadata["extraction_timestamp"])
        check.is_true(metadata["memory_usage_mb"] >= 0)

    def test_extract_should_update_profiling_when_dataframe_has_nulls(
        self, tmp_path: Path
    ) -> None:
        """
        Debe calcular correctamente los valores nulos en profiling cuando el CSV tiene datos faltantes.
        """
        csv_content = pd.DataFrame(
            {
                "id": [1, 2, None],
                "name": ["A", None, "C"],
                "value": [10.0, 20.0, None],
            }
        )
        csv_content.to_csv(tmp_path / "with_nulls.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)
        result = extractor.extract(name="with_nulls")

        metadata = extractor.metadata

        check.equal(metadata["missing_values"], 3)
        check.is_true(metadata["missing_percentage"] > 0)


class TestCSVExtractorValidation:
    """
    Tests para validaciones del CSVExtractor.
    Valida que se lancen errores apropiados para entradas inválidas.
    """

    def test_extract_should_raise_valueerror_when_name_empty(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar ValueError cuando el nombre está vacío.
        """
        # Crear un CSV cualquiera para que la inicialización no falle
        pd.DataFrame({"col": [1]}).to_csv(tmp_path / "dummy.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)

        with pytest.raises(ValueError):
            extractor.extract(name="")

    def test_init_should_raise_filenotfounderror_when_source_missing(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar FileNotFoundError cuando la ruta fuente no existe.
        """
        missing_dir = tmp_path / "does_not_exist"

        with pytest.raises(FileNotFoundError):
            CSVExtractor(source_path=missing_dir)

    def test_extract_should_raise_exception_when_csv_not_found(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar excepción cuando el archivo CSV especificado no existe.
        """
        extractor = CSVExtractor(source_path=tmp_path)

        with pytest.raises(Exception):
            extractor.extract(name="nonexistent_file")


class TestCSVExtractorConfiguration:
    """
    Tests para la configuración del CSVExtractor.
    Valida que las opciones de configuración se apliquen correctamente.
    """

    def test_init_should_store_configuration_in_metadata_when_custom_options(
        self, tmp_path: Path
    ) -> None:
        """
        Debe almacenar la configuración personalizada en metadata cuando se especifican opciones.
        """
        extractor = CSVExtractor(source_path=tmp_path, encoding="latin-1", sep=";")

        metadata = extractor.metadata

        check.equal(metadata["encoding"], "latin-1")
        check.equal(metadata["separator"], ";")
        check.equal(metadata["source_path"], str(tmp_path))

    def test_extract_should_parse_with_custom_separator_when_semicolon_csv(
        self, tmp_path: Path
    ) -> None:
        """
        Debe parsear correctamente cuando el CSV usa separador personalizado.
        """
        # Crear CSV con separador punto y coma
        csv_path = tmp_path / "semicolon.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id;name;value\n")
            f.write("1;Product A;100\n")
            f.write("2;Product B;200\n")

        extractor = CSVExtractor(source_path=tmp_path, sep=";")
        result = extractor.extract(name="semicolon")

        check.equal(len(result), 2)
        check.equal(list(result.columns), ["id", "name", "value"])
