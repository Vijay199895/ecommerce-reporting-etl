"""
Pruebas unitarias para DataCleaner base y utilidades comunes.
Valida el llenado de columnas numéricas con estrategias de nulos.
"""

import pytest
import pytest_check as check
import pandas as pd

from transform.cleaners.base_cleaner import DataCleaner, NullStrategy


class _DummyCleaner(DataCleaner):
    """Implementación mínima para probar comportamientos genéricos del limpiador."""

    def handle_nulls(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df

    def handle_duplicates(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df

    def convert_types(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df

    def validate_cleaned_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df


@pytest.mark.unit
@pytest.mark.transform
class TestBaseCleanerFillColumn:
    """Tests de `_fill_column` para asegurar estrategias de rellenado numérico."""

    def test_fill_column_should_apply_zero_and_mean_when_strategies_are_set(self):
        """Debe rellenar con cero o con la media según la estrategia configurada."""
        cleaner = _DummyCleaner()
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": [None, 2.0, 4.0]})

        df_zero = cleaner._fill_column(df.copy(), "a", NullStrategy.FILL_ZERO)
        df_mean = cleaner._fill_column(df.copy(), "b", NullStrategy.FILL_MEAN)

        check.almost_equal(df_zero["a"].iloc[1], 0.0, rel=1e-6)
        check.almost_equal(df_mean["b"].iloc[0], (2.0 + 4.0) / 2, rel=1e-6)
