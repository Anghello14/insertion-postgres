"""
main.py
Punto de entrada del pipeline ETL.

Flujo:
  1. Configurar logging (consola + archivo)
  2. Cargar configuración desde .env
  3. EXTRACT  — leer archivos .xlsx del directorio data/
  4. TRANSFORM — perfilar los datos crudos (sin limpieza)
  5. LOAD      — conectar a PostgreSQL e insertar los datos crudos,
                 documentando todos los errores
"""
from __future__ import annotations

import sys
import time

import config
from logger_config import setup_logger
from extract.extractor import load_xlsx_files
from transform.profiler import profile_all
from load.loader import get_connection, load_all


def main() -> int:
    # ── 0. Logger ─────────────────────────────────────────────────────────────
    logger = setup_logger(config.LOGS_DIR)
    logger.info("=" * 70)
    logger.info("PIPELINE ETL INICIADO")
    logger.info("  data_dir   : %s", config.DATA_DIR)
    logger.info("  logs_dir   : %s", config.LOGS_DIR)
    logger.info("  batch_size : %d", config.BATCH_SIZE)
    logger.info("  db_host    : %s", config.DB_HOST)
    logger.info("  db_port    : %d", config.DB_PORT)
    logger.info("  db_name    : %s", config.DB_NAME)
    logger.info("  db_schema  : %s", config.DB_SCHEMA)
    logger.info("=" * 70)

    pipeline_start = time.perf_counter()

    # ── 1. EXTRACT ────────────────────────────────────────────────────────────
    logger.info("FASE 1 — EXTRACT")
    datasets = load_xlsx_files(config.DATA_DIR)

    if not datasets:
        logger.warning(
            "No hay datasets para procesar. "
            "Coloque archivos .xlsx en '%s' y vuelva a ejecutar.",
            config.DATA_DIR,
        )
        logger.info("Pipeline finalizado sin datos.")
        return 0

    # ── 2. TRANSFORM (perfilado) ──────────────────────────────────────────────
    logger.info("FASE 2 — TRANSFORM (perfilado, sin limpieza)")
    profiles = profile_all(datasets)

    # ── 3. LOAD ───────────────────────────────────────────────────────────────
    logger.info("FASE 3 — LOAD")

    if not config.DB_NAME or not config.DB_USER:
        logger.error(
            "Credenciales de base de datos incompletas. "
            "Configure DB_NAME y DB_USER en el archivo .env y reintente."
        )
        return 1

    try:
        conn = get_connection(
            host=config.DB_HOST,
            port=config.DB_PORT,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
        )
    except Exception:
        logger.critical(
            "No se pudo establecer la conexión con PostgreSQL. "
            "Verifique la configuración en .env y que el servidor esté activo."
        )
        return 1

    try:
        load_results = load_all(
            conn=conn,
            schema=config.DB_SCHEMA,
            datasets=datasets,
            batch_size=config.BATCH_SIZE,
        )
    finally:
        conn.close()
        logger.info("Conexión a PostgreSQL cerrada.")

    # ── Resumen final ─────────────────────────────────────────────────────────
    pipeline_elapsed = time.perf_counter() - pipeline_start
    total_errors = sum(
        len(r.get("todos_los_errores", [])) for r in load_results.values()
    )

    logger.info("=" * 70)
    logger.info("PIPELINE ETL FINALIZADO en %.4f s", pipeline_elapsed)
    logger.info(
        "Tablas procesadas: %d | Errores totales documentados: %d",
        len(load_results),
        total_errors,
    )

    if total_errors > 0:
        logger.warning(
            "Se documentaron %d error(es) durante la carga. "
            "Revise el archivo de log para el análisis detallado.",
            total_errors,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
