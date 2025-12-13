"""
Pruebas unitarias para loader de Parquet.
"""

from pathlib import Path

import pandas as pd
import pytest
import pytest_check as check

from load.parquet_loader import ParquetLoader


class TestParquetLoaderSave:
    """
    Tests para el guardado del ParquetLoader.
    Valida que se guarde correctamente el archivo Parquet y se actualice la metadata.
    """
    def test_save_should_create_parquet_and_metadata_when_dataframe_valid(self, tmp_path: Path, sample_valid_dataframe: pd.DataFrame) -> None:
        """
        Debe crear archivo Parquet y metadata cuando el DataFrame es válido.
        """
        loader = ParquetLoader(target_path=tmp_path, compression="snappy", index=False, engine="pyarrow")       
        loader.save(sample_valid_dataframe, name="orders_enriched")

        target_file = tmp_path / "orders_enriched.parquet"
        loaded = pd.read_parquet(target_file)
        metadata = loader.metadata
        check.is_true(target_file.exists())
        check.equal(len(loaded), len(sample_valid_dataframe))
        check.is_true(set(loaded.columns) == set(sample_valid_dataframe.columns))
        check.equal(metadata["rows"], sample_valid_dataframe.shape[0])
        check.equal(metadata["columns"], sample_valid_dataframe.shape[1])
        check.equal(metadata["missing_values"], 0)
        check.is_true("target" in metadata)
        check.is_true(str(target_file) == metadata["target"])
        check.is_true(metadata["file_size_mb"] >= 0)
        check.is_true(metadata["load_timestamp"] is not None)


class TestParquetLoaderValidation:
    """
    Tests para validaciones del ParquetLoader.
    Valida que se lancen errores apropiados para entradas inválidas.
    """
    def test_save_should_raise_valueerror_when_name_empty(self, tmp_path: Path, sample_valid_dataframe: pd.DataFrame) -> None:
        """   
        Debe lanzar ValueError cuando el nombre está vacío.
        """
        loader = ParquetLoader(target_path=tmp_path)

        with pytest.raises(ValueError):
            loader.save(sample_valid_dataframe, name="")

    def test_init_should_raise_filenotfounderror_when_target_missing(self, tmp_path: Path) -> None:
        """
        Debe lanzar FileNotFoundError cuando la ruta objetivo no existe.
        """
        missing_dir = tmp_path / "does_not_exist"

        with pytest.raises(FileNotFoundError):
            ParquetLoader(target_path=missing_dir)
   