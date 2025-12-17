# Flujo ETL Detallado

Descripción paso a paso del pipeline, incluyendo validaciones, errores esperados y manejo de inputs mal formados.

---

## Punto de Entrada

```python
# src/main.py
def main():
    run_context.start_run()  # Genera Run ID único
    try:
        run_pipeline()
    finally:
        print_summary_report(pipeline_logger)

def run_pipeline():
    tables = run_extract()                      # Paso 1
    enriched, aggregated = run_transform(tables) # Paso 2
    run_load(enriched, aggregated)               # Paso 3
```

---

## Paso 1: Extracción

### Flujo

```
data/raw/*.csv
      │
      ▼
┌─────────────────────────────────────────────┐
│           CSVExtractor.extract()            │
├─────────────────────────────────────────────┤
│ 1. Validar que el directorio existe         │
│ 2. Por cada tabla en SOURCE_TABLES:         │
│    a. Validar que el CSV existe             │
│    b. Leer con pd.read_csv()                │
│    c. Profiling: rows, nulls, dtypes, memory│
│    d. Actualizar metadata con timestamp     │
│ 3. Retornar Dict[str, DataFrame]            │
└─────────────────────────────────────────────┘
```

### Tablas Extraídas (8 de 11)

```python
# config/settings.py
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
```

### Errores Posibles

| Error | Causa | Excepción |
|-------|-------|-----------|
| Directorio no existe | `data/raw/` no creado | `SourceNotFoundError` |
| CSV no existe | Falta `ecommerce_orders.csv` | `SourceNotFoundError` |
| CSV corrupto | Encoding incorrecto, separador mal | `SourceParseError` |
| Error de lectura | Permisos, disco lleno | `SourceReadError` |

### Log de Ejemplo

```
[a1b2c3d4] 10:30:00 INFO extraction - ═══ INICIO ETAPA: EXTRACCIÓN ═══
[a1b2c3d4] 10:30:00 INFO extraction - Extracción de datos en formato CSV...
[a1b2c3d4] 10:30:01 INFO extraction - Procesamiento completado: orders 1000 filas en 45ms
[a1b2c3d4] 10:30:01 INFO extraction - ═══ FIN ETAPA: EXTRACCIÓN (duración: 234ms) ═══
```

---

## Paso 2: Transformación

### Subflujo General

```
Dict[str, DataFrame]
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│                    _clean_and_enrich()                        │
├───────────────────────────────────────────────────────────────┤
│  ORDERS:                                                      │
│    OrdersCleaner.clean() → OrdersEnricher.enrich()            │
│                                                               │
│  INVENTORY:                                                   │
│    InventoryCleaner.clean() → InventoryEnricher.enrich()      │
│                                                               │
│  REVIEWS:                                                     │
│    ReviewsCleaner.clean() → ReviewsEnricher.enrich()          │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│                       _aggregate()                            │
├───────────────────────────────────────────────────────────────┤
│  CustomerAnalyticsAggregator → top_spenders, recurring, ticket│
│  ProductAnalyticsAggregator  → top_products                   │
│  SalesAnalyticsAggregator    → monthly_sales, promo_rate      │
│  InventoryAnalyticsAggregator→ stock_health, low_stock        │
│  ReviewAnalyticsAggregator   → rating_overview, by_product    │
│  OrderLifecycleAggregator    → funnel, cancellation, delivery │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
(Dict[enriched], Dict[aggregated])
```

### 2.1 Limpieza (Cleaners)

Cada cleaner sigue el Template Method:

```python
def clean(self, df):
    df = self._handle_nulls(df.copy())
    df = self._handle_duplicates(df)
    df = self._convert_types(df)
    df = self._validate_cleaned_data(df)
    return df
```

#### OrdersCleaner

| Paso | Acción |
|------|--------|
| `_handle_nulls` | Validar no-nulls en `order_id`, `customer_id`, `order_date`. Fill mean: `subtotal`, `total_amount`. Fill zero: `discount_percent`, `shipping_cost`, `tax_amount`, `promotion_id`. Fill string: `notes` → "Sin Especificar" |
| `_handle_duplicates` | Drop duplicates por `order_id`, keep last |
| `_convert_types` | IDs → int64, montos → float64, fechas → datetime64 |
| `_validate_cleaned_data` | Columnas requeridas presentes, tipos correctos, rangos válidos (subtotal ≥ 0, discount 0-100), order_id único |

#### InventoryCleaner

| Paso | Acción |
|------|--------|
| `_handle_nulls` | Validar no-nulls en `inventory_id`, `product_id`, `warehouse_id`. Fill zero: `quantity`, `min_stock_level`, `max_stock_level` |
| `_handle_duplicates` | Drop duplicates por `inventory_id`, keep last |
| `_convert_types` | Cantidades → numeric, fechas → datetime64 |
| `_validate_cleaned_data` | Columnas requeridas, quantity ≥ 0, stock levels ≥ 0, inventory_id único |

#### ReviewsCleaner

| Paso | Acción |
|------|--------|
| `_handle_nulls` | Validar no-nulls en `review_id`, `product_id`, `customer_id`, `rating`, `created_at`. Fill zero: `helpful_votes` |
| `_handle_duplicates` | Drop duplicates por `review_id`, keep last |
| `_convert_types` | rating → numeric, helpful_votes → numeric, created_at → datetime64 |
| `_validate_cleaned_data` | Columnas requeridas, rating 1-5, helpful_votes ≥ 0, review_id único |

### 2.2 Enriquecimiento (Enrichers)

#### OrdersEnricher

```
orders_cleaned
    │
    ├── JOIN customers ON customer_id
    │   → segment, registration_date, city, country, email
    │
    ├── JOIN promotions ON promotion_id
    │   → promotion_type, discount_value, start_date, end_date, is_active
    │
    ├── JOIN order_items (aggregated) ON order_id
    │   → items_count, avg_item_price
    │
    └── ADD derived columns
        → order_month, order_week, used_promotion, is_free_shipping, is_high_discount
```

**Validaciones pre-join:**
- `customers`: required columns, no nulls en customer_id, customer_id único
- `promotions`: required columns, no nulls en promotion_id, discount_value ≥ 0

#### InventoryEnricher

```
inventory_cleaned
    │
    ├── JOIN products ON product_id
    │   → product_name, category_id, brand_id
    │
    ├── JOIN warehouses ON warehouse_id
    │   → location, capacity_units, current_occupancy
    │
    └── ADD derived columns
        → is_low_stock, is_overstock
```

#### ReviewsEnricher

```
reviews_cleaned
    │
    ├── JOIN products ON product_id
    │   → product_name, category_id, brand_id
    │
    ├── JOIN customers ON customer_id
    │   → segment, city, country
    │
    └── ADD derived columns
        → review_month, comment_length, is_positive, is_negative
```

### 2.3 Agregación (Aggregators)

| Aggregator | Métrica | Descripción |
|------------|---------|-------------|
| **CustomerAnalytics** | `top_spenders` | Top N clientes por gasto total, filtrado por percentil |
| | `recurring_customers` | Clientes con ≥ min_orders |
| | `average_ticket` | Ticket promedio global |
| **ProductAnalytics** | `top_products` | Ranking por unidades vendidas |
| **SalesAnalytics** | `monthly_sales` | Revenue y órdenes por mes |
| | `promotion_usage_rate` | % órdenes con promoción |
| **InventoryAnalytics** | `stock_health_summary` | Total, low_stock, overstock con % |
| | `low_stock_items` | Top N items por déficit de stock |
| | `warehouse_utilization` | Unidades y ratio por bodega |
| **ReviewAnalytics** | `reviews_overview` | Rating promedio, tasas positiva/negativa |
| | `reviews_by_product` | Ranking por rating (min reviews) |
| | `reviews_monthly` | Volumen y rating por mes |
| **OrderLifecycle** | `status_funnel` | Conteo y % por status |
| | `cancellation_rate` | % cancelaciones |
| | `delivery_rate` | % entregas |
| | `backlog_in_progress` | Órdenes pendientes por mes |

### Errores Posibles en Transform

| Error | Causa | Excepción |
|-------|-------|-----------|
| Columna faltante | CSV no tiene `order_id` | `MissingRequiredColumnsError` |
| Tipo incorrecto | `price` es string, esperado float | `DataTypeMismatchError` |
| Null no permitido | `customer_id` tiene nulls | `NullConstraintError` |
| Fuera de rango | `rating = 6` (máximo 5) | `RangeValidationError` |
| Duplicados | `order_id` repetido | `DuplicateKeyError` |
| Invariante violada | Nulls aumentan después de fill | `CleaningInvariantError` |

---

## Paso 3: Carga

### Flujo

```
enriched (3 DataFrames)  +  aggregated (17 DataFrames)
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│                     run_load()                              │
├─────────────────────────────────────────────────────────────┤
│  Si OUTPUT_FORMATS["parquet"]:                              │
│    ParquetLoader → data/processed/*.parquet (enriched)      │
│    ParquetLoader → data/output/*.parquet (aggregated)       │
│                                                             │
│  Si OUTPUT_FORMATS["csv"]:                                  │
│    CSVLoader → data/processed/*.csv (enriched)              │
│    CSVLoader → data/output/*.csv (aggregated)               │
└─────────────────────────────────────────────────────────────┘
```

### Archivos Generados

**data/processed/** (3 datasets enriquecidos):
- `orders_enriched.parquet` / `.csv`
- `inventory_enriched.parquet` / `.csv`
- `reviews_enriched.parquet` / `.csv`

**data/output/** (17 métricas):
- `top_spenders.parquet` / `.csv`
- `recurring_customers.parquet` / `.csv`
- `average_ticket.parquet` / `.csv`
- `top_products.parquet` / `.csv`
- `monthly_sales.parquet` / `.csv`
- `promotion_usage_rate.parquet` / `.csv`
- `status_funnel.parquet` / `.csv`
- `cancellation_rate.parquet` / `.csv`
- `delivery_rate.parquet` / `.csv`
- `backlog_in_progress.parquet` / `.csv`
- `inventory_health.parquet` / `.csv`
- `low_stock_items.parquet` / `.csv`
- `warehouse_utilization.parquet` / `.csv`
- `reviews_overview.parquet` / `.csv`
- `reviews_by_product.parquet` / `.csv`
- `reviews_monthly.parquet` / `.csv`

### Errores Posibles en Load

| Error | Causa | Excepción |
|-------|-------|-----------|
| Directorio no existe | `data/output/` no creado | `TargetNotFoundError` |
| Nombre vacío | Se pasó `name=""` | `TargetNameNotSpecifiedError` |
| Error de escritura | Disco lleno, permisos | `LoadWriteError` |

---

## Manejo de Inputs Mal Formados

### Escenario 1: CSV con encoding incorrecto

```
Síntoma: UnicodeDecodeError o caracteres raros
Excepción: SourceParseError
Log: ERROR extraction - Error parseando CSV: 'utf-8' codec can't decode...
Acción: Verificar encoding del CSV, ajustar CSVExtractor(encoding="latin-1")
```

### Escenario 2: Columna requerida faltante

```
Síntoma: Pipeline falla en transform
Excepción: MissingRequiredColumnsError
Log: ERROR transformation - Columnas requeridas faltantes: ['order_id']
Acción: Verificar que el CSV tiene todas las columnas esperadas
```

### Escenario 3: Tipo de dato incorrecto

```
Síntoma: Pipeline falla en validación post-limpieza
Excepción: DataTypeMismatchError
Log: ERROR transformation - Tipos no coinciden: 'price': esperado float64, actual object
Acción: Verificar datos fuente, pueden haber strings en columnas numéricas
```

### Escenario 4: Valores fuera de rango

```
Síntoma: Pipeline falla en validación
Excepción: RangeValidationError
Log: ERROR transformation - Columna 'rating': 5 valores fuera del rango [1, 5]
Acción: Limpiar datos fuente o ajustar reglas de validación
```

### Escenario 5: Claves duplicadas

```
Síntoma: Pipeline falla en validación de uniqueness
Excepción: DuplicateKeyError
Log: ERROR transformation - Se encontraron 10 valores duplicados en: ['order_id']
Acción: Investigar duplicados en datos fuente
```

---

## Resumen de Ejecución

Al finalizar, el pipeline genera un resumen ejecutivo:

```
═══════════════════════════════════════════════════════════════════════════
                    RESUMEN DE EJECUCIÓN DEL PIPELINE
═══════════════════════════════════════════════════════════════════════════
  Run ID:           a1b2c3d4
  Inicio:           2024-01-15T10:30:00
  Duración Total:   2.34s
═══════════════════════════════════════════════════════════════════════════
  Estado:           SUCCESS
  Etapas:           3
  Tablas:           8
  Errores:          0
═══════════════════════════════════════════════════════════════════════════
  MÉTRICAS POR ETAPA:
      Extracción      → SUCCESS  (234ms) │ 8 tablas, 10,000 filas
      Transformación  → SUCCESS  (1.8s)  │ 3 enriquecidas, 17 métricas
      Carga           → SUCCESS  (300ms) │ 40 archivos guardados
═══════════════════════════════════════════════════════════════════════════
  DETECCIÓN DE CUELLOS DE BOTELLA:
      aggregate:enriched_orders → 1000 filas en 450ms (2222 filas/s)
      clean:orders              →  500 filas en 120ms (4166 filas/s)
═══════════════════════════════════════════════════════════════════════════
```
