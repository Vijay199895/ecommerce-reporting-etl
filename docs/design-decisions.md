# Decisiones de Diseño

Documento de Architecture Decision Records (ADRs) que justifica las decisiones técnicas tomadas en el proyecto.

---

## ADR-001: Dual Output Format (Parquet + CSV)

### Contexto

El pipeline genera métricas para dos audiencias distintas:
- **Equipo técnico/BI:** Necesita formatos optimizados para queries y pipelines downstream
- **Stakeholders no técnicos:** Necesita poder abrir resultados en Excel

### Decisión

Generar output en **ambos formatos** simultáneamente.

### Consecuencias

| Formato | Ventajas | Trade-off |
|---------|----------|-----------|
| **Parquet** | 8x compresión, preserva tipos, columnar, ideal para Athena/Spark | No legible en Excel |
| **CSV** | Universal, Excel-friendly, fácil validación manual | Sin tipos, más pesado |

### Configuración

```python
# config/settings.py
OUTPUT_FORMATS = {
    "parquet": True,
    "csv": True,
}
```

Se puede deshabilitar cualquiera si cambian los requerimientos.

---

## ADR-002: Cleaners Solo para 3 Tablas Críticas

### Contexto

El dataset tiene 11 tablas, pero no todas requieren el mismo nivel de procesamiento.

### Decisión

Implementar cleaners específicos solo para:
- **orders** - Tabla principal del negocio, columnas críticas con valores faltantes (`promotion_id`, `notes`)
- **inventory** - Métricas de stock críticas para operaciones diarias
- **reviews** - Voz del cliente, base para decisiones estratégicas

### Justificación

| Tabla | ¿Por qué cleaner? |
|-------|-------------------|
| orders | Alta criticidad, valores nulos en columnas financieras, requiere recálculo de `total_amount` |
| inventory | Niveles de stock afectan operaciones diarias, requiere fill_zero para métricas |
| reviews | Rating es entrada del usuario, requiere validación de rango 1-5 |

### Tablas sin Cleaner

Las tablas auxiliares (`customers`, `products`, `promotions`, `warehouses`, `order_items`) pasan por **validación en runtime** durante el enriquecimiento:

```python
# En enrichers, antes de cada join:
validator = SchemaValidator(customers_df, transform_logger)
validator.validate_required_columns(cols)
validator.validate_no_nulls(["customer_id"])
validator.validate_unique_values(["customer_id"])
```

Si hay inconsistencias, se lanza excepción y el pipeline falla. Para datasets pequeños con bajo volumen de entrada, esto permite revisión manual del issue.

### Evolución Futura

Si el negocio crece y estas tablas empiezan a tener problemas de calidad, se implementarían cleaners específicos siguiendo el mismo patrón.

---

## ADR-003: Tablas No Procesadas (suppliers, brands, categories)

### Contexto

El directorio `data/raw/` contiene 11 CSVs, pero solo se procesan 8.

### Decisión

Excluir de `SOURCE_TABLES`:
- `ecommerce_suppliers.csv`
- `ecommerce_brands.csv`
- `ecommerce_categories.csv`

### Justificación

Estas tablas **no tienen preguntas de negocio asociadas** en la primera iteración. El pipeline responde a necesidades actuales de NovaMart, no a un modelo completo.

### Extensibilidad

Agregar una tabla nueva es trivial:

```python
# config/settings.py
SOURCE_TABLES = {
    # ... existentes ...
    "brands": "ecommerce_brands",      # Agregar aquí
    "categories": "ecommerce_categories",
}
```

Luego implementar cleaner/enricher si es necesario.

---

## ADR-004: Reglas de Negocio en Capa Transform

### Contexto

¿Dónde vive la lógica como "un cliente es recurrente si tiene ≥2 órdenes"?

### Decisión

**Toda la lógica de negocio vive en `transform/`**, específicamente:
- **Cleaners:** Reglas de imputación (fill_mean para subtotal, fill_zero para discount)
- **Enrichers:** Columnas derivadas (`is_low_stock = quantity <= min_stock_level`)
- **Aggregators:** Umbrales de métricas (`min_orders=2` para recurrencia)

### Justificación

| Alternativa | Por qué no |
|-------------|------------|
| En extract | Extract solo lee datos, no los interpreta |
| En load | Load solo escribe, no transforma |
| En config | Constantes sí van en config, lógica no |

### Parametrización

Los umbrales numéricos están en `config/settings.py`:

```python
TOP_SPENDERS_N = 5
TOP_SPENDERS_PERCENTILE = 0.8
RECURRING_CUSTOMERS_MIN_ORDERS = 2
LOW_STOCK_ITEMS_N = 20
MIN_REVIEWS_FOR_PRODUCT = 3
```

Esto permite ajustar sin tocar código.

---

## ADR-005: Sistema de Logging con Run ID

### Contexto

En pipelines ETL, correlacionar logs de una misma ejecución es crítico para debugging.

### Decisión

Implementar `RunContext` singleton que:
1. Genera **Run ID único** (UUID 8 chars) al inicio
2. Acumula métricas por etapa y tabla
3. Genera resumen ejecutivo al final

### Implementación

```python
# Cada log incluye el Run ID
[a1b2c3d4] 2024-01-15 10:30:00 INFO extraction - Extrayendo orders...

# Resumen final
═══════════════════════════════════════════════════════════════════
                    RESUMEN DE EJECUCIÓN DEL PIPELINE
═══════════════════════════════════════════════════════════════════
  Run ID:           a1b2c3d4
  Duración Total:   2.34s
  Estado:           SUCCESS
  Tablas:           8
  Métricas:         17
```

### Beneficios

- **Trazabilidad:** Un Run ID = una ejecución completa
- **Debugging:** Filtrar logs por Run ID en producción
- **Métricas:** `stage_metrics` y `table_metrics` para profiling
- **Detección de cuellos de botella:** Automática en el resumen

---

## ADR-006: Validación Fail-Fast con Excepciones Tipadas

### Contexto

¿Qué pasa cuando los datos no cumplen el schema esperado?

### Decisión

**Fail-fast:** Lanzar excepción tipada inmediatamente y detener el pipeline.

### Jerarquía de Excepciones

```
ETLError
├── ExtractError (fuente no existe, parse error)
├── TransformError
│   ├── SchemaValidationError (columnas, tipos)
│   └── DataQualityError (nulls, rangos, duplicados)
└── LoadError (destino no existe, write error)
```

### Justificación

| Alternativa | Por qué no |
|-------------|------------|
| Continuar con warnings | Datos corruptos propagándose silenciosamente |
| Retornar None/vacío | El consumidor no sabe que algo falló |
| Códigos de error | Menos expresivo que excepciones tipadas |

### Auto-logging

Las excepciones se loguean automáticamente al instanciarse:

```python
class ETLError(Exception):
    def __init__(self, message: str, logger: logging.Logger, ...):
        super().__init__(message)
        logger.log(log_level, message)  # Auto-log
```

---

## ADR-007: Template Method en Cleaners

### Contexto

Los 3 cleaners siguen el mismo flujo: nulls → duplicates → types → validate.

### Decisión

Usar **Template Method** en `DataCleaner`:

```python
class DataCleaner(ABC):
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._handle_nulls(df.copy())
        df = self._handle_duplicates(df)
        df = self._convert_types(df)
        df = self._validate_cleaned_data(df)
        return df

    @abstractmethod
    def _handle_nulls(self, df): ...
    @abstractmethod
    def _handle_duplicates(self, df): ...
    # ...
```

### Beneficios

- **Consistencia:** Todos los cleaners siguen el mismo orden
- **Extensibilidad:** Nuevo cleaner solo implementa los 4 métodos abstractos
- **Testing:** Se puede testear cada paso aisladamente

---

## ADR-008: Estrategias de Null Handling como Enum

### Contexto

Diferentes columnas requieren diferentes estrategias de imputación.

### Decisión

Enum `NullStrategy` con 6 estrategias predefinidas:

```python
class NullStrategy(Enum):
    DROP = "drop"
    FILL_MEAN = "fill_mean"
    FILL_MEDIAN = "fill_median"
    FILL_MODE = "fill_mode"
    FILL_STRING = "fill_string"  # "Sin Especificar"
    FILL_ZERO = "fill_zero"
```

### Uso

```python
# OrdersCleaner
strategies = {
    "subtotal": NullStrategy.FILL_MEAN,
    "discount_percent": NullStrategy.FILL_ZERO,
    "notes": NullStrategy.FILL_STRING,
}
```

### Beneficios

- **Type safety:** IDE autocompleta estrategias válidas
- **Documentación:** El código es auto-explicativo
- **Extensibilidad:** Agregar `FILL_FORWARD`, `FILL_BACKWARD` si se necesita

---

## Trade-offs Aceptados

### 1. Enrichers Hacen Validación

**Situación actual:** Los enrichers validan las tablas auxiliares antes de cada join.

**Violación:** Single Responsibility Principle (validar + enriquecer = 2 cosas).

**Por qué se aceptó:** Primera iteración, datasets pequeños, el overhead es menor que crear una capa de validación separada.

**Mejora futura:** Introducir tipos como `ValidatedCustomers` que encapsulen un DataFrame ya validado.

### 2. DataFrames Crudos Entre Etapas

**Situación actual:** Se pasan `pd.DataFrame` directamente.

**Riesgo:** Un DataFrame "sucio" podría llegar a la agregación.

**Por qué se aceptó:** Simplicidad. Los cleaners garantizan la limpieza de las 3 tablas críticas.

**Mejora futura:** Tipos wrapper como `CleanedOrders`, `EnrichedOrders` con invariantes validadas en constructor.

### 3. Validación Manual para Tablas Auxiliares

**Situación actual:** Si `customers` tiene un null en `customer_id`, el pipeline falla y requiere intervención manual.

**Por qué se aceptó:** Volumen bajo de datos, baja frecuencia de cambios en tablas dimensionales.

**Mejora futura:** Cleaners para todas las tablas si el volumen crece.
