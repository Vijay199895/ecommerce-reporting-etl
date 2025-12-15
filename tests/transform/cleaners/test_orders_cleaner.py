"""
Pruebas unitarias para OrdersCleaner.

Este módulo valida:
- Validación de nulos en columnas clave (order_id, customer_id, order_date)
- Relleno de columnas numéricas (subtotal, shipping_cost, etc.)
- Conversión de tipos y recálculo de total_amount
- Pipeline completo de limpieza

Fixtures utilizados:
- raw_orders_dirty: Datos con nulos en claves (para probar excepciones)
- raw_orders_missing_total_amount: Datos válidos para probar recálculo
- raw_orders_valid_keys: Datos con claves completas para probar relleno
"""

import pytest
import pytest_check as check

from exceptions import NullConstraintError
from transform.cleaners.orders_cleaner import OrdersCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerNulls:
    """
    Tests de `handle_nulls` para validación de claves y relleno numérico.

    Verifica que:
    - Se lanza NullConstraintError cuando hay nulos en columnas clave
    - Se rellenan columnas numéricas con estrategias apropiadas
    """

    def test_handle_nulls_should_raise_null_constraint_error_when_key_columns_have_nulls(
        self, raw_orders_dirty
    ):
        """
        Dado un DataFrame con nulos en order_id, customer_id o order_date,
        Cuando se ejecuta handle_nulls,
        Entonces debe lanzar NullConstraintError indicando las columnas afectadas.
        """
        cleaner = OrdersCleaner()

        with pytest.raises(NullConstraintError) as exc_info:
            cleaner.handle_nulls(raw_orders_dirty.copy())

        check.is_in("order_id", str(exc_info.value))

    def test_handle_nulls_should_fill_numeric_columns_when_key_columns_are_valid(
        self, raw_orders_valid_keys
    ):
        """
        Dado un DataFrame con claves completas pero nulos en columnas numéricas,
        Cuando se ejecuta handle_nulls,
        Entonces debe rellenar shipping_cost y discount_percent sin lanzar excepción.
        """
        cleaner = OrdersCleaner()

        cleaned = cleaner.handle_nulls(raw_orders_valid_keys.copy())

        check.equal(len(cleaned), 3)
        check.equal(cleaned["shipping_cost"].isna().sum(), 0)
        check.equal(cleaned["discount_percent"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerConvert:
    """
    Tests de `convert_types` para recálculo de totales y normalización.

    Verifica que total_amount se recalcula cuando está ausente
    y que los tipos de datos se normalizan correctamente.
    """

    def test_convert_types_should_recalculate_total_when_missing_values_present(
        self, raw_orders_missing_total_amount
    ):
        """
        Dado un DataFrame con total_amount nulo pero componentes válidos,
        Cuando se ejecuta convert_types,
        Entonces debe recalcular total_amount y no quedar valores nulos.
        """
        cleaner = OrdersCleaner()

        converted = cleaner.convert_types(raw_orders_missing_total_amount.copy())

        check.equal(converted["total_amount"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersCleanerCleanPipeline:
    """
    Tests del flujo `clean` completo con datos válidos.

    Verifica que el pipeline ejecuta todas las etapas correctamente
    y produce un DataFrame con claves y totales sin nulos.
    """

    def test_clean_should_raise_null_constraint_error_when_keys_have_nulls(
        self, raw_orders_dirty
    ):
        """
        Dado un DataFrame con nulos en columnas clave,
        Cuando se ejecuta el pipeline clean,
        Entonces debe lanzar NullConstraintError durante handle_nulls.
        """
        cleaner = OrdersCleaner()

        with pytest.raises(NullConstraintError):
            cleaner.clean(raw_orders_dirty)

    def test_clean_should_return_non_null_keys_and_totals_when_pipeline_runs(
        self, raw_orders_valid_keys
    ):
        """
        Dado un DataFrame con claves completas y nulos solo en numéricas,
        Cuando se ejecuta el pipeline clean,
        Entonces debe retornar datos con claves y totales sin nulos.
        """
        cleaner = OrdersCleaner()

        cleaned = cleaner.clean(raw_orders_valid_keys)

        check.is_true((cleaned["order_id"].notna()).all())
        check.equal(cleaned["total_amount"].isna().sum(), 0)
