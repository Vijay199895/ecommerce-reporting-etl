# Arquitectura del Sistema

Documento que profundiza en la arquitectura del sistema y el rol de cada componente dentro del mismo.

---

## Separación de Capas

### Extract (`src/extract/`)

| Componente | Responsabilidad |
|------------|-----------------|
| `BaseExtractor` | Contrato abstracto: `extract()`, `_validate_source_exists()`, `_profile_data()` |
| `CSVExtractor` | Implementación concreta para CSV con encoding y separador configurable |

**Extensibilidad:** Agregar `JSONExtractor`, `APIExtractor` o `DatabaseExtractor` solo requiere heredar de `BaseExtractor`.

### Transform (`src/transform/`)

Tres subcapas con responsabilidades claras:

| Subcapa | Qué hace | Patrón |
|---------|----------|--------|
| **cleaners/** | Manejo de nulls, duplicados, tipos, validación post-limpieza | Template Method |
| **enrichers/** | Joins con tablas dimensionales, columnas derivadas | Composition |
| **aggregators/** | Métricas de negocio agrupadas | Single Responsibility |

### Load (`src/load/`)

| Componente | Responsabilidad |
|------------|-----------------|
| `BaseLoader` | Contrato abstracto: `save()`, `_validate_target_exists()`, `_profile_data_*()` |
| `ParquetLoader` | Compresión snappy, motor pyarrow |
| `CSVLoader` | UTF-8, sin índice por defecto |

### Pipeline (`src/pipeline/`)

Orquestación de alto nivel:

```python
# src/main.py
tables = run_extract()                    
enriched, aggregated = run_transform(tables)
run_load(enriched, aggregated)
```

### Utils (`src/utils/`)

| Módulo | Función |
|--------|---------|
| `logger.py` | Run ID, decoradores `@log_stage`, `@log_table_processing`, resumen ejecutivo |
| `validators.py` | `SchemaValidator` con 6 métodos de validación |

### Exceptions (`src/exceptions/`)

Jerarquía por fase:

```
ETLError (base)
├── ExtractError
│   ├── SourceNotFoundError
│   ├── SourceParseError
│   └── SourceReadError
├── TransformError
│   ├── SchemaValidationError
│   │   ├── MissingRequiredColumnsError
│   │   └── DataTypeMismatchError
│   └── DataQualityError
│       ├── NullConstraintError
│       └── RangeValidationError
└── LoadError
    ├── TargetNotFoundError
    └── LoadWriteError
```

## ¿Por qué Pandas y no Spark?

| Factor | Pandas | Spark |
|--------|--------|-------|
| **Volumen actual** | ~10K registros | Overkill |
| **Latencia** | Milisegundos | Segundos de startup |
| **Infraestructura** | Python puro | Requiere cluster/JVM |
| **Curva de aprendizaje** | Baja | Media-alta |
| **Debugging** | Trivial | Complejo (lazy eval) |

**Conclusión:** Para el volumen actual de NovaMart, Pandas es la herramienta correcta. El overhead de Spark no se justifica.

## Escenarios Futuros de Escalabilidad

### Volúmenes > 1M registros

```
Opción A: Polars (drop-in replacement, más rápido que Pandas)
Opción B: DuckDB (SQL sobre archivos, excelente para analytics)
Opción C: Spark (si hay cluster disponible)
```

### Migración a Cloud (AWS)

```
data/raw/       →  S3 bucket (raw zone)
data/processed/ →  S3 bucket (processed zone)
data/output/    →  S3 bucket (curated zone) + Athena/Redshift

Orquestación:   →  Airflow / Step Functions
Cómputo:        →  Lambda (volumen bajo) / EMR (volumen alto)
```

### Tiempo Real

```
Kafka/Kinesis  →  Flink/Spark Streaming  →  S3/Redshift
                  (micro-batches)
```

**El diseño actual facilita estas migraciones** porque:
- Los extractores/loaders son intercambiables (herencia)
- La lógica de transformación es agnóstica al storage
- Los parámetros están centralizados en `config/settings.py`
