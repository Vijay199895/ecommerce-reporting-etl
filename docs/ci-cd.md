# CI/CD Pipeline

Documentación del pipeline de integración continua con GitHub Actions.

---

## Overview

El proyecto usa **GitHub Actions** para CI. El workflow se dispara en:
- Push a `main`
- Pull Requests hacia `main`

Badge de estado: ![CI Status](https://github.com/Gerardo1909/ecommerce-reporting-etl/actions/workflows/ci.yml/badge.svg)

---

## Workflow: `ci.yml`

```yaml
name: CI Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

### Jobs

#### Job: `test`

Corre en `ubuntu-latest` con Python 3.13.

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - Checkout code
      - Setup Python 3.13
      - Install uv (gestor de dependencias)
      - Install dependencies
      - Run smoke tests
      - Run all tests
      - Upload artifacts
```

---

## Pasos del Pipeline

### 1. Checkout

```yaml
- name: Checkout code
  uses: actions/checkout@v4
```

Clona el repositorio en el runner.

### 2. Setup Python

```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: '3.13'
```

Instala Python 3.13 (versión mínima requerida por el proyecto).

### 3. Install uv

```yaml
- name: Install uv
  uses: astral-sh/setup-uv@v4
  with:
    enable-cache: true
    cache-dependency-glob: "uv.lock"
```

[uv](https://github.com/astral-sh/uv) es un gestor de dependencias Python ultrarrápido. El cache acelera builds subsecuentes.

### 4. Install Dependencies

```yaml
- name: Install dependencies
  run: uv sync
```

Instala todas las dependencias del proyecto desde `pyproject.toml`.

### 5. Smoke Tests

```yaml
- name: Run Pytest smoke tests
  run: uv run pytest tests/ -m smoke -v --tb=short
  timeout-minutes: 15
```

Ejecuta **solo tests marcados con `@pytest.mark.smoke`**. Estos son tests críticos que deben pasar siempre. Timeout de 15 minutos como safety.

### 6. All Tests

```yaml
- name: Run Pytest all tests
  run: uv run pytest tests/ -v --tb=short
  timeout-minutes: 30
  continue-on-error: true
```

Ejecuta **toda la suite de tests**. `continue-on-error: true` permite que el pipeline continúe aunque fallen tests (útil para debugging).

### 7. Upload Artifacts

```yaml
- name: Upload test reports
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: test-reports
    path: reports/
    retention-days: 30

- name: Upload test logs
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: test-logs
    path: logs/
    retention-days: 30
```

**Siempre** (incluso si fallan tests) sube:
- `reports/` - Reportes HTML de pytest y coverage
- `logs/` - Logs del pipeline ETL

Retención de 30 días.

---

## Qué Corre en Cada Evento

| Evento | Qué corre | Comportamiento |
|--------|-----------|----------------|
| **PR a main** | Smoke tests + All tests | Validación antes de merge |
| **Push a main** | Smoke tests + All tests | Validación post-merge |

---

## Artifacts Generados

Los artifacts se pueden descargar desde la pestaña "Actions" en GitHub:

| Artifact | Contenido |
|----------|-----------|
| `test-reports` | `pytest_report.html`, `coverage/` |
| `test-logs` | `extract.log`, `transform.log`, `load.log`, `errors.log`, `pipeline.log` |

---
