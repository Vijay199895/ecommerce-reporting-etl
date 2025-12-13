"""
Pruebas unitarias para InventoryCleaner.
Cubren manejo de nulos, conversiones y validaciones del pipeline.
"""

import pytest
import pytest_check as check
import pandas as pd

from transform.cleaners.inventory_cleaner import InventoryCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerNulls:
    """Tests de `handle_nulls` para garantizar inventario sin vacíos requeridos."""

    def test_handle_nulls_should_drop_missing_keys_and_fill_thresholds_when_cleaning_inventory(
        self, raw_inventory_dirty
    ):
        """Debe eliminar filas sin claves y rellenar cantidades y umbrales."""
        cleaner = InventoryCleaner()

        cleaned = cleaner.handle_nulls(raw_inventory_dirty.copy())

        check.equal(len(cleaned), 1)
        check.equal(
            cleaned[["quantity", "min_stock_level", "max_stock_level"]]
            .isna()
            .sum()
            .sum(),
            0,
        )


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerConvert:
    """Tests de `convert_types` para normalizar tipos numéricos y fechas."""

    def test_convert_types_should_normalize_numeric_and_dates_when_inventory_dirty(
        self, raw_inventory_dirty
    ):
        """Debe convertir cantidades a numérico y fechas a tipo datetime."""
        cleaner = InventoryCleaner()

        converted = cleaner.convert_types(raw_inventory_dirty.copy())

        check.is_true(pd.api.types.is_numeric_dtype(converted["quantity"]))
        check.is_true(
            pd.api.types.is_datetime64_any_dtype(converted["last_restock_date"])
        )


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerCleanPipeline:
    """Tests del método `clean` para validar obligatoriedad de claves."""

    def test_clean_should_return_inventory_with_required_keys_when_pipeline_executes(
        self, raw_inventory_dirty
    ):
        """Debe retornar inventario con identificadores presentes y consistentes."""
        cleaner = InventoryCleaner()

        cleaned = cleaner.clean(raw_inventory_dirty)

        check.is_true((cleaned["inventory_id"].notna()).all())
