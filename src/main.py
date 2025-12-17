"""
Punto de entrada principal para el proceso ETL.
"""

import sys
from pathlib import Path

# Agregar config al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import run_extract, run_transform, run_load
from utils.logger import pipeline_logger, run_context, print_summary_report


def run_pipeline() -> None:
    """Ejecuta el flujo ETL completo: Extract → Transform → Load."""
    tables = run_extract()
    enriched, aggregated = run_transform(tables)
    run_load(enriched, aggregated)


def main() -> None:
    """Orquesta el flujo ETL con inicialización y reporte final."""
    run_context.start_run()

    try:
        run_pipeline()
    finally:
        print_summary_report(pipeline_logger)


if __name__ == "__main__":
    main()
