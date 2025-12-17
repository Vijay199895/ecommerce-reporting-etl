"""
Configuración centralizada del pipeline ETL.

Centraliza paths, configuración de tablas fuente, y parámetros
del pipeline en un solo lugar para facilitar mantenimiento.
"""

from pathlib import Path

# =============================================================================
# RUTAS BASE
# =============================================================================

# Raíz del proyecto (dos niveles arriba de este archivo)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Directorios de datos
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR_CSV = PROCESSED_DIR / "csv"
PROCESSED_DIR_PARQUET = PROCESSED_DIR / "parquet"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR_CSV = OUTPUT_DIR / "csv"
OUTPUT_DIR_PARQUET = OUTPUT_DIR / "parquet"

# Directorio de logs
LOGS_DIR = PROJECT_ROOT / "logs"

# =============================================================================
# CONFIGURACIÓN DE FUENTES DE DATOS
# =============================================================================

# Mapeo de nombres lógicos a archivos CSV fuente
SOURCE_TABLES = {
    "orders": "ecommerce_orders",
    "order_items": "ecommerce_order_items",
    "customers": "ecommerce_customers",
    "promotions": "ecommerce_promotions",
    "products": "ecommerce_products",
    "reviews": "ecommerce_reviews",
    "inventory": "ecommerce_inventory",
    "warehouses": "ecommerce_warehouses",
}

# =============================================================================
# PARÁMETROS DE AGREGACIÓN
# =============================================================================

# Customer Analytics
TOP_SPENDERS_N = 5
TOP_SPENDERS_PERCENTILE = 0.8
RECURRING_CUSTOMERS_MIN_ORDERS = 2

# Product Analytics
TOP_PRODUCTS_N = 10

# Inventory Analytics
LOW_STOCK_ITEMS_N = 20

# Review Analytics
MIN_REVIEWS_FOR_PRODUCT = 3
TOP_REVIEWED_PRODUCTS_N = 20

# =============================================================================
# CONFIGURACIÓN DE SALIDA
# =============================================================================

# Formatos de salida habilitados
OUTPUT_FORMATS = {
    "parquet": True,
    "csv": True,
}

# Datasets enriquecidos a guardar
ENRICHED_DATASETS = ["orders", "inventory", "reviews"]

# =============================================================================
# FUNCIONES HELPER
# =============================================================================


# Se ejecuta al importar el módulo para asegurar que los directorios existen
def ensure_directories() -> None:
    """Crea los directorios necesarios si no existen."""
    for directory in [
        RAW_DATA_DIR,
        PROCESSED_DIR,
        PROCESSED_DIR_CSV,
        PROCESSED_DIR_PARQUET,
        OUTPUT_DIR,
        OUTPUT_DIR_CSV,
        OUTPUT_DIR_PARQUET,
        LOGS_DIR,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


ensure_directories()


def get_raw_path() -> str:
    """Retorna el path de datos crudos como string."""
    return str(RAW_DATA_DIR)


def get_processed_csv_path() -> str:
    """Retorna el path de datos procesados en CSV como string."""
    return str(PROCESSED_DIR_CSV)


def get_processed_parquet_path() -> str:
    """Retorna el path de datos procesados en Parquet como string."""
    return str(PROCESSED_DIR_PARQUET)


def get_output_csv_path() -> str:
    """Retorna el path de salida en CSV como string."""
    return str(OUTPUT_DIR_CSV)


def get_output_parquet_path() -> str:
    """Retorna el path de salida en Parquet como string."""
    return str(OUTPUT_DIR_PARQUET)
