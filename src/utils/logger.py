"""
Módulo para logging del pipeline ETL.

Proporciona:
- Run ID único para correlacionar logs de una misma ejecución
- Contexto granular (etapa, tabla, paso)
- Handlers separados para consola, archivo y errores
- Decoradores para logging automático de funciones
- Medición de tiempos de ejecución
- Alertas de calidad de datos
"""

import logging
import os
import uuid
import functools
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, Dict, List, Optional
import time


# ============================================================================
# CONFIGURACIÓN DE DIRECTORIOS
# ============================================================================

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)


# ============================================================================
# GESTIÓN DE RUN ID (IDENTIFICADOR ÚNICO DE EJECUCIÓN)
# ============================================================================


class RunContext:
    """
    Singleton que mantiene el contexto de la ejecución actual del pipeline.

    Almacena el Run ID y métricas agregadas para el resumen final.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.run_id: str = ""
        self.start_time: Optional[datetime] = None
        self.stage_metrics: Dict[str, Dict[str, Any]] = {}
        self.table_metrics: Dict[str, Dict[str, Any]] = {}
        self.errors: List[Dict[str, Any]] = []

    def start_run(self) -> str:
        """Inicia una nueva ejecución del pipeline y genera un Run ID único."""
        self.run_id = uuid.uuid4().hex[:8]
        self.start_time = datetime.now()
        self.stage_metrics = {}
        self.table_metrics = {}
        self.errors = []
        return self.run_id

    def get_run_id(self) -> str:
        """Obtiene el Run ID actual o genera uno si no existe."""
        if not self.run_id:
            self.start_run()
        return self.run_id

    def record_stage_metric(self, stage: str, metric: str, value: Any):
        """Registra una métrica para una etapa específica."""
        if stage not in self.stage_metrics:
            self.stage_metrics[stage] = {}
        self.stage_metrics[stage][metric] = value

    def record_table_metric(
        self, table: str, stage: str, rows: int, duration_ms: float
    ):
        """Registra métricas de procesamiento de una tabla."""
        key = f"{stage}:{table}"
        self.table_metrics[key] = {
            "table": table,
            "stage": stage,
            "rows": rows,
            "duration_ms": round(duration_ms, 2),
        }

    def add_error(self, stage: str, table: str, error: str):
        """Registra un error ocurrido durante el pipeline."""
        self.errors.append(
            {
                "stage": stage,
                "table": table,
                "error": error,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def get_total_duration_seconds(self) -> float:
        """Calcula la duración total del pipeline en segundos."""
        if self.start_time:
            return (datetime.now() - self.start_time).total_seconds()
        return 0.0

    def get_summary(self) -> Dict[str, Any]:
        """Genera un resumen completo de la ejecución."""
        return {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "total_duration_seconds": round(self.get_total_duration_seconds(), 2),
            "stages_processed": len(self.stage_metrics),
            "tables_processed": len(self.table_metrics),
            "errors_count": len(self.errors),
            "stage_metrics": self.stage_metrics,
            "table_metrics": self.table_metrics,
            "errors": self.errors,
        }


# Instancia global del contexto de ejecución
run_context = RunContext()


# ============================================================================
# FORMATEADORES PERSONALIZADOS
# ============================================================================


class ETLFormatter(logging.Formatter):
    """
    Formateador que incluye Run ID y contexto adicional en los mensajes.
    """

    def format(self, record: logging.LogRecord) -> str:
        record.run_id = run_context.get_run_id()
        record.table = getattr(record, "table", "")
        record.step = getattr(record, "step", "")

        return super().format(record)


# ============================================================================
# CONFIGURACIÓN DE LOGGERS
# ============================================================================


def get_logger(name: str, filename: str) -> logging.Logger:
    """
    Obtiene un logger configurado con handlers de consola, archivo y errores.

    Args:
        name: Nombre del logger (ej: 'extraction', 'transformation')
        filename: Nombre del archivo de log principal

    Returns:
        Logger configurado con múltiples handlers
    """
    logger = logging.getLogger(name)

    # Evitar duplicar handlers si el logger ya existe
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Formato con Run ID para archivos
    file_formatter = ETLFormatter(
        "[%(run_id)s] %(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Formato más simple para consola (sin run_id completo para legibilidad)
    console_formatter = ETLFormatter(
        "%(asctime)s %(levelname)-8s %(name)s - %(message)s", datefmt="%H:%M:%S"
    )

    # Formato para errores con más detalle
    error_formatter = ETLFormatter(
        "[%(run_id)s] %(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler para archivo principal con rotación
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=1024 * 1024,  # 1MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Handler para consola (solo INFO y superior)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Handler separado para errores
    error_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "errors.log"),
        maxBytes=1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)

    return logger


# ============================================================================
# DECORADORES PARA LOGGING AUTOMÁTICO
# ============================================================================


def log_stage(stage_name: str, logger: logging.Logger):
    """
    Decorador que loguea automáticamente el inicio, fin y duración de una etapa.

    Args:
        stage_name: Nombre de la etapa (ej: 'extract', 'transform', 'load')
        logger: Logger a utilizar

    Example:
        @log_stage("extract", extract_logger)
        def extract_stage():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"═══ INICIO ETAPA: {stage_name.upper()} ═══")

            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                duration_str = _format_duration(duration_ms)

                run_context.record_stage_metric(
                    stage_name, "duration_ms", round(duration_ms, 2)
                )
                run_context.record_stage_metric(stage_name, "status", "SUCCESS")

                logger.info(
                    f"═══ FIN ETAPA: {stage_name.upper()} (duración: {duration_str}) ═══"
                )
                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                run_context.record_stage_metric(stage_name, "status", "FAILED")
                run_context.record_stage_metric(stage_name, "error", str(e))
                run_context.add_error(stage_name, "N/A", str(e))
                logger.error(f"═══ ERROR EN ETAPA: {stage_name.upper()} - {e} ═══")
                raise

        return wrapper

    return decorator


def log_io_operation(operation: str, logger: logging.Logger):
    """
    Decorador que loguea operaciones de entrada/salida (extracción/carga de datos).

    Args:
        operation: Nombre de la operación (ej: 'load_csv', 'save_parquet')
        logger: Logger a utilizar
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            # Las funciones IO pueden recibir parámetros: 'name' para nombre particular de recurso
            # o alguno que contenga 'location' en su nombre para indicar la ubicación del recurso
            resource_info = kwargs.get("name", None)
            source_location = kwargs.get("source_location", None)
            target_location = kwargs.get("target_location", None)
            resource_info = (
                str(source_location)
                if source_location
                else str(target_location)
                if target_location
                else resource_info
            )
            logger.info(
                f"Iniciando operación I/O sobre {resource_info}: {operation}..."
            )

            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                duration_str = _format_duration(duration_ms)

                logger.info(
                    f"Operación I/O completada: {operation} (duración: {duration_str})"
                )
                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"ERROR en operación I/O: {operation} - {e} (después de {duration_ms:.1f}ms)"
                )
                raise

        return wrapper

    return decorator


def log_table_processing(stage: str, logger: logging.Logger, table_name: str = ""):
    """
    Decorador que loguea el procesamiento de una tabla específica.

    Args:
        stage: Etapa del pipeline (extract, clean, enrich, aggregate, load)
        logger: Logger a utilizar
        table_name: Nombre de la tabla a procesar
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            instance = args[0]
            actual_table_name = (
                table_name if table_name else getattr(instance, "TABLE_NAME", "unknown")
            )
            logger.info(
                f"Iniciando procesamiento de la tabla {actual_table_name} en la etapa {stage}..."
            )

            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                duration_str = _format_duration(duration_ms)

                # Intentar obtener el número de filas del resultado
                rows = _get_row_count(result)
                run_context.record_table_metric(actual_table_name, stage, rows, duration_ms)

                logger.info(
                    f"Procesamiento completado de la tabla {actual_table_name} en la etapa {stage}: "
                    f"{rows} filas en {duration_str}"
                )
                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                run_context.add_error(stage, actual_table_name, str(e))
                logger.error(
                    f"Error en procesamiento de la tabla {actual_table_name} en la etapa {stage} tras {duration_ms:.1f}ms: {e}"
                )
                # Opcional: registrar métrica de tabla fallida con 0 filas
                run_context.record_table_metric(actual_table_name, stage, 0, duration_ms)
                raise

        return wrapper

    return decorator


def log_substep(substep_name: str, logger: logging.Logger):
    """
    Decorador para loguear sub-pasos dentro de una transformación.

    Args:
        substep_name: Nombre del sub-paso (ej: 'join_customers', 'add_derived_columns')
        logger: Logger a utilizar
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.debug(f"{substep_name}: iniciando...")

            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000

                rows = _get_row_count(result)
                if rows > 0:
                    logger.debug(
                        f"{substep_name}: completado ({rows} filas, {duration_ms:.1f}ms)"
                    )
                else:
                    logger.debug(f"{substep_name}: completado ({duration_ms:.1f}ms)")

                return result

            except Exception as e:
                logger.error(f"{substep_name}: ERROR - {e}")
                raise

        return wrapper

    return decorator


# ============================================================================
# UTILIDADES
# ============================================================================


def _format_duration(duration_ms: float) -> str:
    """Formatea una duración en milisegundos a formato legible."""
    if duration_ms < 1000:
        return f"{duration_ms:.1f}ms"
    elif duration_ms < 60000:
        return f"{duration_ms / 1000:.2f}s"
    else:
        minutes = int(duration_ms // 60000)
        seconds = (duration_ms % 60000) / 1000
        return f"{minutes}m {seconds:.1f}s"


def _get_row_count(obj) -> int:
    """Intenta obtener el número de filas de un objeto (DataFrame, dict, etc)."""
    if hasattr(obj, "__len__"):
        if hasattr(obj, "shape"):  # DataFrame
            return obj.shape[0]
        elif isinstance(obj, dict):
            # Si es un dict de DataFrames, sumar filas
            total = 0
            for v in obj.values():
                if hasattr(v, "shape"):
                    total += v.shape[0]
            return total
        return len(obj)
    return 0


def print_summary_report(logger: logging.Logger):
    """
    Imprime un resumen ejecutivo de la ejecución del pipeline.
    """
    summary = run_context.get_summary()

    separator = "═" * 70
    logger.info("")
    logger.info(separator)
    logger.info("                    RESUMEN DE EJECUCIÓN DEL PIPELINE")
    logger.info(separator)
    logger.info(f"  Run ID:           {summary['run_id']}")
    logger.info(f"  Inicio:           {summary['start_time']}")
    logger.info(
        f"  Duración Total:   {_format_duration(summary['total_duration_seconds'] * 1000)}"
    )
    logger.info(separator)

    # Estado final
    status = "SUCCESS" if summary["errors_count"] == 0 else "FAILED"
    logger.info(f"  Estado:           {status}")
    logger.info(f"  Etapas:           {summary['stages_processed']}")
    logger.info(f"  Tablas:           {summary['tables_processed']}")
    logger.info(f"  Errores:          {summary['errors_count']}")
    logger.info(separator)

    # Métricas por etapa
    if summary["stage_metrics"]:
        logger.info("  MÉTRICAS POR ETAPA:")
        for stage, metrics in summary["stage_metrics"].items():
            duration = metrics.get("duration_ms", 0)
            status = metrics.get("status", "N/A")
            logger.info(f"      {stage:15} → {status:8} ({_format_duration(duration)})")
        logger.info(separator)

    # Detección de cuellos de botella
    if summary["table_metrics"]:
        logger.info("  DETECCIÓN DE CUELLOS DE BOTELLA:")

        # Ordenar por duración descendente
        sorted_tables = sorted(
            summary["table_metrics"].items(),
            key=lambda x: x[1]["duration_ms"],
            reverse=True,
        )

        # Mostrar los 5 más lentos
        for key, metrics in sorted_tables[:5]:
            table = metrics["table"]
            stage = metrics["stage"]
            rows = metrics["rows"]
            duration = metrics["duration_ms"]

            # Calcular velocidad de procesamiento
            if rows > 0 and duration > 0:
                rows_per_sec = (rows / duration) * 1000
                logger.info(
                    f"      {stage}:{table:15} → {rows:>6} filas en {_format_duration(duration)} "
                    f"({rows_per_sec:.0f} filas/s)"
                )
            else:
                logger.info(
                    f"      {stage}:{table:15} → {rows:>6} filas en {_format_duration(duration)}"
                )

        logger.info(separator)

    # Errores
    if summary["errors"]:
        logger.info("  ERRORES REGISTRADOS:")
        for error in summary["errors"]:
            logger.info(f"      [{error['stage']}:{error['table']}] {error['error']}")
        logger.info(separator)

    logger.info("")


# ============================================================================
# LOGGERS SEPARADOS PARA CADA ETAPA DEL PROCESO ETL
# ============================================================================

extract_logger = get_logger("extraction", "extract.log")
transform_logger = get_logger("transformation", "transform.log")
load_logger = get_logger("loading", "load.log")
pipeline_logger = get_logger("pipeline", "pipeline.log")
