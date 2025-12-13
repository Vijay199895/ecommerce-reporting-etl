"""
Fixtures compartidas para pruebas unitarias del proyecto ETL.

Este módulo contiene fixtures de pytest que son reutilizables
en múltiples suites de tests. Incluye:
- DataFrames de prueba para validadores
- Archivos CSV temporales para extractores
- Datos de ejemplo para diferentes escenarios
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest


# FIXTURES: DataFrames para pruebas de validadores


@pytest.fixture
def sample_valid_dataframe() -> pd.DataFrame:
    """
    DataFrame válido y completo para pruebas positivas.

    Contiene:
    - Datos numéricos en rango válido
    - Sin valores nulos
    - Sin duplicados
    - Tipos de datos correctos
    """
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Product A", "Product B", "Product C", "Product D", "Product E"],
            "price": [10.50, 25.00, 15.75, 30.00, 12.50],
            "quantity": [100, 50, 75, 200, 120],
            "category": ["Electronics", "Clothing", "Electronics", "Books", "Clothing"],
            "created_at": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                ]
            ),
        }
    )


@pytest.fixture
def dataframe_with_nulls() -> pd.DataFrame:
    """DataFrame con valores nulos para pruebas de validación de nulos."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, None, 5],
            "name": ["Product A", None, "Product C", "Product D", "Product E"],
            "price": [10.50, 25.00, None, 30.00, 12.50],
        }
    )


@pytest.fixture
def dataframe_with_duplicates() -> pd.DataFrame:
    """DataFrame con duplicados para pruebas de unicidad."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 2, 5],  # ID 2 duplicado
            "name": ["Product A", "Product B", "Product C", "Product B", "Product E"],
            "price": [10.50, 25.00, 15.75, 25.00, 12.50],
        }
    )


@pytest.fixture
def dataframe_with_invalid_ranges() -> pd.DataFrame:
    """DataFrame con valores fuera de rango para pruebas de validación."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.50, -5.00, 15.75, 150.00, 12.50],  # Precio negativo y muy alto
            "quantity": [100, 50, -10, 200, 120],  # Cantidad negativa
        }
    )


@pytest.fixture
def dataframe_with_wrong_types() -> pd.DataFrame:
    """DataFrame con tipos de datos incorrectos."""
    return pd.DataFrame(
        {
            "id": ["1", "2", "3", "4", "5"],  # Strings en lugar de int
            "price": ["10.50", "25.00", "15.75", "30.00", "12.50"],  # Strings
            "quantity": [100.5, 50.2, 75.8, 200.1, 120.9],  # Floats en lugar de int
        }
    )


@pytest.fixture
def dataframe_with_extra_columns() -> pd.DataFrame:
    """DataFrame con columnas adicionales no esperadas."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "price": [10.0, 20.0, 30.0],
            "extra_col_1": ["X", "Y", "Z"],  # No esperada
            "extra_col_2": [1, 2, 3],  # No esperada
        }
    )


@pytest.fixture
def dataframe_missing_required_columns() -> pd.DataFrame:
    """DataFrame al que le faltan columnas requeridas."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            # Falta 'price' que podría ser requerida
        }
    )


# FIXTURES: Archivos CSV temporales para extractores


@pytest.fixture
def valid_csv_file(tmp_path: Path) -> Path:
    """
    Crea un archivo CSV válido temporal.

    Args:
        tmp_path: Directorio temporal provisto por pytest

    Returns:
        Path: Ruta al archivo CSV temporal
    """
    csv_path = tmp_path / "test_data.csv"
    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [101, 102, 103, 104, 105],
            "product_id": [201, 202, 203, 204, 205],
            "quantity": [2, 1, 3, 1, 2],
            "price": [29.99, 49.99, 19.99, 99.99, 39.99],
            "order_date": [
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
            ],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def csv_with_latin1_encoding(tmp_path: Path) -> Path:
    """
    Crea un archivo CSV con encoding latin-1.
    Útil para probar lectura con diferentes codificaciones.
    """
    csv_path = tmp_path / "test_latin1.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["José", "María", "Ángel"],  # Caracteres especiales
            "description": ["Niño", "Año", "España"],
        }
    )
    df.to_csv(csv_path, index=False, encoding="latin-1")
    return csv_path


@pytest.fixture
def csv_with_semicolon_separator(tmp_path: Path) -> Path:
    """Crea un CSV con punto y coma como separador."""
    csv_path = tmp_path / "test_semicolon.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Product A", "Product B", "Product C"],
            "price": [10.5, 20.0, 30.5],
        }
    )
    df.to_csv(csv_path, index=False, sep=";")
    return csv_path


@pytest.fixture
def csv_with_dates(tmp_path: Path) -> Path:
    """Crea un CSV con columnas de fechas para probar parse_dates."""
    csv_path = tmp_path / "test_dates.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "event_name": ["Sale", "Purchase", "Return"],
            "created_at": [
                "2024-01-01 10:30:00",
                "2024-01-02 14:45:00",
                "2024-01-03 09:15:00",
            ],
            "updated_at": [
                "2024-01-01 11:00:00",
                "2024-01-02 15:00:00",
                "2024-01-03 10:00:00",
            ],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def malformed_csv_file(tmp_path: Path) -> Path:
    """
    Crea un archivo CSV malformado con inconsistencias.
    Útil para probar manejo de errores.
    """
    csv_path = tmp_path / "malformed.csv"
    # Escribir CSV manualmente con formato incorrecto
    with open(csv_path, "w") as f:
        f.write("id,name,price\n")
        f.write("1,Product A,10.50\n")
        f.write("2,Product B\n")  # Falta una columna
        f.write("3,Product C,30.00,extra\n")  # Columna extra
    return csv_path


@pytest.fixture
def large_csv_file(tmp_path: Path) -> Path:
    """
    Crea un CSV grande para pruebas de rendimiento.
    Genera 10,000 filas para simular datos reales.
    """
    csv_path = tmp_path / "large_data.csv"

    # Generar datos realistas
    base_date = datetime(2024, 1, 1)
    dates = [base_date + timedelta(days=i % 365) for i in range(10000)]

    df = pd.DataFrame(
        {
            "order_id": range(1, 10001),
            "customer_id": [i % 1000 + 1 for i in range(10000)],
            "product_id": [i % 500 + 1 for i in range(10000)],
            "quantity": [i % 10 + 1 for i in range(10000)],
            "price": [round(10 + (i % 100), 2) for i in range(10000)],
            "order_date": [d.strftime("%Y-%m-%d") for d in dates],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def empty_csv_file(tmp_path: Path) -> Path:
    """Crea un archivo CSV vacío (solo headers)."""
    csv_path = tmp_path / "empty.csv"
    df = pd.DataFrame(columns=["id", "name", "value"])
    df.to_csv(csv_path, index=False)
    return csv_path


# FIXTURES: Datos para pruebas de agregadores


@pytest.fixture
def enriched_orders_sample() -> pd.DataFrame:
    """Órdenes enriquecidas con totales, fechas y promociones para pruebas de ventas/clientes."""
    return pd.DataFrame(
        {
            "order_id": [101, 102, 103, 104, 105],
            "customer_id": [1, 1, 2, 3, 3],
            "order_date": pd.to_datetime(
                [
                    "2024-01-15",
                    "2024-01-20",
                    "2024-02-10",
                    "2024-02-18",
                    "2024-03-05",
                ]
            ),
            "total_amount": [120.0, 80.0, 200.0, 150.0, 50.0],
            "used_promotion": [True, False, True, False, False],
            "channel": ["online", "store", "online", "store", "online"],
            "email": [
                "user1@example.com",
                "user1@example.com",
                "user2@example.com",
                "user3@example.com",
                "user3@example.com",
            ],
        }
    )


@pytest.fixture
def order_items_sample() -> pd.DataFrame:
    """Ítems de orden con cantidades y subtotales para pruebas de productos."""
    return pd.DataFrame(
        {
            "order_id": [101, 101, 102, 103, 104, 105],
            "product_id": [10, 11, 10, 12, 12, 13],
            "quantity": [2, 1, 1, 3, 2, 4],
            "subtotal": [40.0, 30.0, 20.0, 90.0, 60.0, 120.0],
        }
    )


@pytest.fixture
def products_catalog() -> pd.DataFrame:
    """Catálogo de productos con nombres."""
    return pd.DataFrame(
        {
            "product_id": [10, 11, 12, 13],
            "product_name": ["Gadget", "Widget", "Device", "Bundle"],
        }
    )


@pytest.fixture
def inventory_sample() -> pd.DataFrame:
    """Inventario con flags de low/over stock y capacidad para pruebas de métricas."""
    return pd.DataFrame(
        {
            "product_id": [10, 11, 12, 13, 14],
            "warehouse_id": [1, 1, 2, 2, 3],
            "quantity": [5, 50, 200, 300, 20],
            "min_stock_level": [10, 20, 100, 100, 15],
            "max_stock_level": [100, 80, 250, 260, 50],
            "is_low_stock": [True, False, False, False, False],
            "is_overstock": [False, False, False, True, False],
            "capacity_units": [500, 500, 400, 400, 200],
        }
    )


@pytest.fixture
def reviews_enriched_sample() -> pd.DataFrame:
    """Reviews con ratings y banderas de sentimiento para pruebas de agregadores."""
    return pd.DataFrame(
        {
            "review_id": [1, 2, 3, 4, 5, 6],
            "product_id": [10, 10, 11, 12, 12, 12],
            "rating": [5, 4, 2, 3, 1, 5],
            "is_positive": [True, True, False, False, False, True],
            "is_negative": [False, False, True, False, True, False],
            "created_at": pd.to_datetime(
                [
                    "2024-01-10",
                    "2024-01-12",
                    "2024-02-05",
                    "2024-02-20",
                    "2024-03-01",
                    "2024-03-15",
                ]
            ),
        }
    )


@pytest.fixture
def orders_status_sample() -> pd.DataFrame:
    """Órdenes con estados variados para métricas de ciclo de vida."""
    return pd.DataFrame(
        {
            "order_id": [201, 202, 203, 204, 205, 206],
            "status": [
                "pending",
                "processing",
                "shipped",
                "delivered",
                "cancelled",
                "delivered",
            ],
            "order_date": pd.to_datetime(
                [
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-02-10",
                    "2024-02-12",
                    "2024-03-01",
                ]
            ),
            "total_amount": [100, 120, 80, 150, 60, 200],
        }
    )


# FIXTURES: Datos crudos para cleaners/enrichers


@pytest.fixture
def raw_orders_dirty() -> pd.DataFrame:
    """Órdenes con nulos y tipos mezclados para probar limpieza de órdenes."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, None],
            "customer_id": [10, None, 12],
            "order_date": ["2024-01-01", "2024-01-02", None],
            "subtotal": [100.0, None, 50.0],
            "discount_percent": [10, None, 5],
            "shipping_cost": [None, 5.0, None],
            "tax_amount": [8.0, None, 4.0],
            "total_amount": [None, 150.0, None],
        }
    )


@pytest.fixture
def raw_orders_missing_total_amount() -> pd.DataFrame:
    """Órdenes con nulos y tipos mezclados para probar cálculo de total_amount."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [10, 2, 12],
            "order_date": ["2024-01-01", "2024-01-02", None],
            "subtotal": [100.0, None, 50.0],
            "discount_percent": [10, None, 5],
            "shipping_cost": [None, 5.0, None],
            "tax_amount": [8.0, None, 4.0],
            "total_amount": [None, 150.0, None],
        }
    )


@pytest.fixture
def raw_inventory_dirty() -> pd.DataFrame:
    """Inventario con claves faltantes y nulos para probar limpieza."""
    return pd.DataFrame(
        {
            "inventory_id": [1, 2, None],
            "product_id": [10, None, 12],
            "warehouse_id": [1, 1, 2],
            "quantity": [None, 50, 20],
            "min_stock_level": [10, None, 5],
            "max_stock_level": [100, 80, None],
            "current_occupancy": [200, None, 50],
            "last_restock_date": ["2024-01-01", None, "2024-01-05"],
        }
    )


@pytest.fixture
def raw_reviews_dirty() -> pd.DataFrame:
    """Reviews con nulos y tipos para probar limpieza."""
    return pd.DataFrame(
        {
            "review_id": [1, 2, None],
            "product_id": [10, 11, 12],
            "customer_id": [101, None, 103],
            "rating": [5, None, 3],
            "helpful_votes": [None, 2, None],
            "created_at": ["2024-01-10", "2024-01-12", None],
        }
    )


@pytest.fixture
def orders_enricher_inputs() -> dict:
    """Entradas mínimas para probar orders_enricher."""
    return {
        "orders": pd.DataFrame(
            {
                "order_id": [1],
                "customer_id": [10],
                "order_date": pd.to_datetime(["2024-01-15"]),
                "promotion_id": [100],
                "subtotal": [100.0],
                "discount_percent": [10.0],
                "shipping_cost": [0.0],
                "tax_amount": [19.0],
                "total_amount": [None],
            }
        ),
        "customers": pd.DataFrame(
            {
                "customer_id": [10],
                "segment": ["vip"],
                "registration_date": pd.to_datetime(["2023-12-01"]),
                "email": ["vip@example.com"],
            }
        ),
        "promotions": pd.DataFrame(
            {
                "promotion_id": [100],
                "promotion_type": ["coupon"],
                "discount_value": [10],
                "is_active": [True],
                "start_date": pd.to_datetime(["2024-01-01"]),
                "end_date": pd.to_datetime(["2024-02-01"]),
            }
        ),
        "order_items": pd.DataFrame(
            {
                "order_id": [1, 1],
                "product_id": [10, 11],
                "quantity": [1, 2],
                "unit_price": [40.0, 30.0],
                "subtotal": [40.0, 60.0],
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": [10, 11],
                "product_name": ["Gadget", "Widget"],
                "category_id": [1000, 1001],
                "brand_id": [2000, 2001],
                "cost": [20.0, 10.0],
                "price": [40.0, 35.0],
            }
        ),
        "categories": pd.DataFrame(
            {"category_id": [1000, 1001], "category_name": ["CatA", "CatB"]}
        ),
        "brands": pd.DataFrame(
            {"brand_id": [2000, 2001], "brand_name": ["BrandA", "BrandB"]}
        ),
    }


@pytest.fixture
def inventory_enricher_inputs(raw_inventory_dirty) -> dict:
    """Entradas mínimas para inventory_enricher con productos/bodegas."""
    return {
        "inventory": raw_inventory_dirty.dropna(subset=["inventory_id"]).copy(),
        "products": pd.DataFrame(
            {
                "product_id": [10, 12],
                "product_name": ["Gadget", "Device"],
                "category_id": [1000, 1002],
                "brand_id": [2000, 2002],
            }
        ),
        "warehouses": pd.DataFrame(
            {
                "warehouse_id": [1, 2],
                "location": ["A", "B"],
                "capacity_units": [500, 400],
                "current_occupancy": [250, 100],
            }
        ),
    }


@pytest.fixture
def reviews_enricher_inputs() -> dict:
    """Entradas mínimas para reviews_enricher con productos/clientes."""
    return {
        "reviews": pd.DataFrame(
            {
                "review_id": [1, 2],
                "product_id": [10, 11],
                "customer_id": [101, 102],
                "rating": [5, 2],
                "created_at": pd.to_datetime(["2024-01-10", "2024-01-12"]),
                "comment": ["great", "bad"],
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": [10, 11],
                "product_name": ["Gadget", "Widget"],
                "category_id": [1000, 1001],
                "brand_id": [2000, 2001],
            }
        ),
        "customers": pd.DataFrame(
            {
                "customer_id": [101, 102],
                "segment": ["vip", "standard"],
                "city": ["X", "Y"],
                "country": ["AR", "BR"],
            }
        ),
    }


# FIXTURES: Constantes y configuraciones de prueba


@pytest.fixture
def expected_schema_orders() -> dict:
    """Schema esperado para tabla de órdenes."""
    return {
        "expected_columns": ["order_id", "customer_id", "order_date"],
        "expected_dtypes": {"order_id": "int64", "customer_id": "int64"},
    }


@pytest.fixture
def expected_schema_products() -> dict:
    """Schema esperado para tabla de productos."""
    return {
        "expected_columns": ["product_id", "product_name", "price", "category_id"],
        "expected_dtypes": {
            "product_id": "int64",
            "price": "float64",
            "category_id": "int64",
        },
    }
