"""
Pruebas unitarias para OrdersCleaner.
Evalúan manejo de nulos, conversiones y el pipeline completo.
"""

import pytest
import pytest_check as check

from transform.cleaners.orders_cleaner import OrdersCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerNulls:
    """Tests de `handle_nulls` para pedidos con claves y costos faltantes."""

    def test_handle_nulls_should_drop_missing_keys_and_fill_numeric_when_cleaning_orders(
        self, raw_orders_dirty
    ):
        """Debe descartar filas sin claves y rellenar columnas numéricas obligatorias."""
        cleaner = OrdersCleaner()

        cleaned = cleaner.handle_nulls(raw_orders_dirty.copy())

        check.equal(len(cleaned), 1)
        check.equal(cleaned["shipping_cost"].isna().sum(), 0)
        check.equal(cleaned["discount_percent"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerConvert:
    """Tests de `convert_types` para recomputar totales y normalizar datos."""

    def test_convert_types_should_recalculate_total_when_missing_values_present(
        self, raw_orders_missing_total_amount
    ):
        """Debe recalcular `total_amount` cuando falte y convertir tipos numéricos."""
        cleaner = OrdersCleaner()

        converted = cleaner.convert_types(raw_orders_missing_total_amount.copy())

        check.equal(converted["total_amount"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerCleanPipeline:
    """Tests del flujo `clean` completo verificando validaciones finales."""

    def test_clean_should_return_non_null_keys_and_totals_when_pipeline_runs(
        self, raw_orders_dirty
    ):
        """Debe ejecutar el pipeline, asegurar claves y totales sin nulos."""
        cleaner = OrdersCleaner()

        cleaned = cleaner.clean(raw_orders_dirty)

        check.is_true((cleaned["order_id"].notna()).all())
        check.equal(cleaned["total_amount"].isna().sum(), 0)
