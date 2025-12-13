"""
Pruebas unitarias para loader base.
"""

from typing import Any

import pandas as pd
import pytest_check as check

from load.base_loader import BaseLoader


class DummyLoader(BaseLoader):
	"""
	Implementación mínima para probar utilidades de la clase base.
	"""

	def save(self, df: pd.DataFrame, name: str) -> None:  
		return

	def _validate_target_exists(self, target_location: Any) -> None:  
		return

	def _profile_data_after_load(self, after_load_info: Any) -> None:
		# Actualiza metadata imitando el comportamiento de loaders concretos
		self.metadata.update({"target": after_load_info, "file_size_mb": 0.01})
		self._update_load_timestamp()


class TestBaseLoaderProfiling:
	"""
	Tests para el profiling del BaseLoader.
	Valida que se actualice correctamente la metadata antes y después de la carga.
	"""
	def test_get_summary_should_include_metadata_when_profiled_data_provided(self, sample_valid_dataframe: pd.DataFrame) -> None:
		"""
		Debe incluir metadata cuando se proporciona datos perfilados.
		"""
		loader = DummyLoader()

		loader._profile_data_before_load(sample_valid_dataframe)
		loader._profile_data_after_load("dummy_target")

		summary = loader.get_summary()

		check.is_true("rows" in summary)
		check.is_true("columns" in summary)
		check.is_true("dummy_target" in summary)
		check.is_true(loader.metadata["load_timestamp"] is not None)
  