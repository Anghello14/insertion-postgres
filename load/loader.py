"""
load/loader.py
Conecta a PostgreSQL, crea las tablas destino dinámicamente (si no existen)
e inserta los registros crudos fila a fila y/o en lotes.

El objetivo principal es CAPTURAR y DOCUMENTAR todos los errores que surjan
durante la inserción:
  - Tipo de error (IntegrityError, DataError, OperationalError, etc.)
  - Código de error de PostgreSQL (pgcode)
  - Mensaje de error
  - Detalle adicional (pgerror)
  - Fila completa que generó el error
  - Columna(s) implicada(s) cuando sea deducible
  - Tiempos de carga por lote y totales
  - Embotellamientos (lotes que tardan más del umbral)
"""
from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql

logger = logging.getLogger("etl_pipeline")

# Umbral en segundos para alertar sobre lotes lentos
SLOW_BATCH_THRESHOLD_S = 5.0

# Longitud máxima (chars) al truncar valores de fila en mensajes de error
MAX_ERROR_VALUE_LENGTH = 200


# ── Conexión ─────────────────────────────────────────────────────────────────

def get_connection(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> psycopg2.extensions.connection:
    """Abre y devuelve una conexión psycopg2. Loga el resultado."""
    logger.info(
        "Intentando conectar a PostgreSQL → host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )
    t0 = time.perf_counter()
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10,
        )
        elapsed = time.perf_counter() - t0
        logger.info(
            "Conexión establecida correctamente en %.3f s. "
            "Versión del servidor: %s",
            elapsed,
            conn.server_version,
        )
        return conn
    except psycopg2.OperationalError as exc:
        elapsed = time.perf_counter() - t0
        logger.error(
            "No se pudo conectar a PostgreSQL en %.3f s. "
            "Tipo: %s | pgcode: %s | Detalle: %s",
            elapsed,
            type(exc).__name__,
            getattr(exc, "pgcode", "N/A"),
            exc,
            exc_info=True,
        )
        raise


# ── Creación de tabla ─────────────────────────────────────────────────────────

def create_table_if_not_exists(
    conn: psycopg2.extensions.connection,
    schema: str,
    table_name: str,
    columns: list[str],
) -> None:
    """
    Crea la tabla con todas las columnas de tipo TEXT (para recibir datos crudos).
    Si ya existe, no hace nada.
    """
    col_defs = ", ".join(
        sql.Identifier(col).as_string(conn) + " TEXT" for col in columns
    )
    create_stmt = (
        f"CREATE TABLE IF NOT EXISTS "
        f"{sql.Identifier(schema, table_name).as_string(conn)} ({col_defs});"
    )
    logger.info(
        "Verificando/creando tabla '%s.%s' con %d columnas...",
        schema,
        table_name,
        len(columns),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(create_stmt)
        conn.commit()
        logger.info("Tabla '%s.%s' lista.", schema, table_name)
    except Exception as exc:
        conn.rollback()
        logger.error(
            "Error al crear tabla '%s.%s'. Tipo: %s | pgcode: %s | Detalle: %s",
            schema,
            table_name,
            type(exc).__name__,
            getattr(exc, "pgcode", "N/A"),
            exc,
            exc_info=True,
        )
        raise


# ── Inserción por lotes ───────────────────────────────────────────────────────

def _insert_batch(
    conn: psycopg2.extensions.connection,
    schema: str,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    batch_index: int,
) -> dict[str, Any]:
    """
    Intenta insertar un lote de filas usando execute_values (bulk insert).
    Si el lote falla, hace fallback fila a fila para identificar exactamente
    qué fila causó el error.

    Returns
    -------
    dict con estadísticas del lote: insertadas, fallidas, errores detallados.
    """
    batch_result: dict[str, Any] = {
        "batch_index": batch_index,
        "total": len(rows),
        "insertadas": 0,
        "fallidas": 0,
        "errores": [],
        "tiempo_s": 0.0,
        "modo": "bulk",
    }

    table_id = sql.Identifier(schema, table_name).as_string(conn)
    col_ids = ", ".join(sql.Identifier(c).as_string(conn) for c in columns)
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    insert_sql = f"INSERT INTO {table_id} ({col_ids}) VALUES {placeholders}"

    values = [[row.get(col) for col in columns] for row in rows]

    t0 = time.perf_counter()

    # ── Intento bulk ────────────────────────────────────────────────────────
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {table_id} ({col_ids}) VALUES %s",
                values,
                template=None,
                page_size=len(rows),
            )
        conn.commit()
        batch_result["insertadas"] = len(rows)
        batch_result["tiempo_s"] = round(time.perf_counter() - t0, 4)

        if batch_result["tiempo_s"] > SLOW_BATCH_THRESHOLD_S:
            logger.warning(
                "⚠ LOTE LENTO — batch #%d tardó %.3f s (umbral=%.1f s)",
                batch_index,
                batch_result["tiempo_s"],
                SLOW_BATCH_THRESHOLD_S,
            )
        else:
            logger.debug(
                "Lote #%d insertado (bulk) en %.4f s — %d filas",
                batch_index,
                batch_result["tiempo_s"],
                len(rows),
            )
        return batch_result

    except Exception as bulk_exc:
        conn.rollback()
        logger.warning(
            "Lote #%d falló en modo bulk. Tipo: %s | pgcode: %s | "
            "Mensaje: %s — Cambiando a modo fila a fila.",
            batch_index,
            type(bulk_exc).__name__,
            getattr(bulk_exc, "pgcode", "N/A"),
            bulk_exc,
        )
        batch_result["modo"] = "row_by_row"

    # ── Fallback: fila a fila ────────────────────────────────────────────────
    for row_offset, row in enumerate(rows):
        row_values = [row.get(col) for col in columns]
        try:
            with conn.cursor() as cur:
                cur.execute(insert_sql, row_values)
            conn.commit()
            batch_result["insertadas"] += 1
        except Exception as row_exc:
            conn.rollback()
            batch_result["fallidas"] += 1

            error_detail = _build_error_detail(
                exc=row_exc,
                batch_index=batch_index,
                row_offset=row_offset,
                row_data=row,
                columns=columns,
            )
            batch_result["errores"].append(error_detail)

            logger.error(
                "ERROR fila #%d (batch #%d, offset %d) | "
                "Tipo: %s | pgcode: %s | pgerror: %s | "
                "Mensaje: %s | Fila: %s",
                batch_index * len(rows) + row_offset + 1,
                batch_index,
                row_offset,
                error_detail["tipo_error"],
                error_detail["pgcode"],
                error_detail["pgerror"],
                error_detail["mensaje"],
                error_detail["fila_datos"],
            )

    batch_result["tiempo_s"] = round(time.perf_counter() - t0, 4)
    return batch_result


def _build_error_detail(
    exc: Exception,
    batch_index: int,
    row_offset: int,
    row_data: dict[str, Any],
    columns: list[str],
) -> dict[str, Any]:
    """Construye un dict estructurado con toda la información del error."""
    pgcode = getattr(exc, "pgcode", None)
    pgerror = getattr(exc, "pgerror", None)
    diag = getattr(exc, "diag", None)

    # Intentar deducir la columna implicada desde el mensaje de error
    implicated_col = None
    if pgerror:
        for col in columns:
            if col.lower() in pgerror.lower():
                implicated_col = col
                break

    return {
        "batch_index": batch_index,
        "row_offset": row_offset,
        "tipo_error": type(exc).__name__,
        "pgcode": pgcode,
        "pgerror": pgerror,
        "mensaje": str(exc).strip(),
        "columna_implicada": implicated_col,
        "diagnostico_tabla": getattr(diag, "table_name", None) if diag else None,
        "diagnostico_columna": getattr(diag, "column_name", None) if diag else None,
        "diagnostico_tipo_dato": getattr(diag, "datatype_name", None) if diag else None,
        "diagnostico_constraint": getattr(diag, "constraint_name", None) if diag else None,
        "fila_datos": {
            k: (str(v)[:MAX_ERROR_VALUE_LENGTH] if v is not None else None)
            for k, v in row_data.items()
        },
    }


# ── Carga principal ───────────────────────────────────────────────────────────

def load_dataframe(
    conn: psycopg2.extensions.connection,
    schema: str,
    table_name: str,
    df: pd.DataFrame,
    batch_size: int = 100,
) -> dict[str, Any]:
    """
    Inserta todas las filas del DataFrame en la tabla indicada.

    Flujo:
      1. Crear tabla (si no existe) con todas las cols como TEXT.
      2. Dividir el DataFrame en lotes de `batch_size` filas.
      3. Intentar inserción bulk por lote; fallback fila a fila si falla.
      4. Documentar cada error con máximo detalle.

    Returns
    -------
    dict con el resumen completo de la carga.
    """
    columns = list(df.columns)
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    total_rows = len(rows)

    logger.info("=" * 70)
    logger.info(
        "CARGA iniciada → tabla '%s.%s' | filas=%d | cols=%d | batch_size=%d",
        schema,
        table_name,
        total_rows,
        len(columns),
        batch_size,
    )

    # Crear tabla destino
    create_table_if_not_exists(conn, schema, table_name, columns)

    # Estadísticas globales
    global_result: dict[str, Any] = {
        "tabla": f"{schema}.{table_name}",
        "total_filas": total_rows,
        "total_insertadas": 0,
        "total_fallidas": 0,
        "total_lotes": 0,
        "lotes_con_error": 0,
        "todos_los_errores": [],
        "tiempo_total_s": 0.0,
    }

    t_global = time.perf_counter()
    batch_index = 0

    for start in range(0, total_rows, batch_size):
        batch_rows = rows[start : start + batch_size]
        batch_index += 1
        global_result["total_lotes"] += 1

        logger.info(
            "Procesando lote #%d — filas %d a %d de %d",
            batch_index,
            start + 1,
            min(start + batch_size, total_rows),
            total_rows,
        )

        batch_result = _insert_batch(
            conn=conn,
            schema=schema,
            table_name=table_name,
            columns=columns,
            rows=batch_rows,
            batch_index=batch_index,
        )

        global_result["total_insertadas"] += batch_result["insertadas"]
        global_result["total_fallidas"] += batch_result["fallidas"]

        if batch_result["errores"]:
            global_result["lotes_con_error"] += 1
            global_result["todos_los_errores"].extend(batch_result["errores"])

        logger.info(
            "Lote #%d completado | modo=%s | insertadas=%d | fallidas=%d | "
            "tiempo=%.4f s",
            batch_index,
            batch_result["modo"],
            batch_result["insertadas"],
            batch_result["fallidas"],
            batch_result["tiempo_s"],
        )

    global_result["tiempo_total_s"] = round(time.perf_counter() - t_global, 4)

    # ── Resumen final de la tabla ────────────────────────────────────────────
    logger.info(
        "CARGA COMPLETADA → '%s.%s' | "
        "total=%d | insertadas=%d | fallidas=%d | lotes=%d | "
        "lotes_con_error=%d | tiempo_total=%.4f s",
        schema,
        table_name,
        global_result["total_filas"],
        global_result["total_insertadas"],
        global_result["total_fallidas"],
        global_result["total_lotes"],
        global_result["lotes_con_error"],
        global_result["tiempo_total_s"],
    )

    # Loguear resumen de errores agrupados por tipo
    if global_result["todos_los_errores"]:
        _log_error_summary(table_name, global_result["todos_los_errores"])

    return global_result


def _log_error_summary(table_name: str, errors: list[dict]) -> None:
    """Agrupa y loguea errores por tipo para facilitar el análisis."""
    from collections import Counter

    type_counter: Counter = Counter(e["tipo_error"] for e in errors)
    code_counter: Counter = Counter(e["pgcode"] for e in errors if e["pgcode"])
    col_counter: Counter = Counter(
        e["columna_implicada"] for e in errors if e["columna_implicada"]
    )

    logger.warning("── RESUMEN DE ERRORES en tabla '%s' ──", table_name)
    logger.warning("  Total errores: %d", len(errors))
    logger.warning("  Por tipo de excepción: %s", dict(type_counter))
    logger.warning("  Por código PostgreSQL (pgcode): %s", dict(code_counter))
    logger.warning("  Por columna implicada: %s", dict(col_counter))


# ── Carga de todos los datasets ───────────────────────────────────────────────

def load_all(
    conn: psycopg2.extensions.connection,
    schema: str,
    datasets: dict[str, pd.DataFrame],
    batch_size: int = 100,
) -> dict[str, dict]:
    """
    Itera sobre todos los datasets y carga cada uno en su tabla correspondiente.

    Returns
    -------
    dict[str, dict]  Resultados de carga por nombre de tabla.
    """
    results: dict[str, dict] = {}
    total_tables = len(datasets)

    logger.info(
        "Iniciando carga global. Tablas a cargar: %d", total_tables
    )

    for i, (table_name, df) in enumerate(datasets.items(), start=1):
        logger.info(
            "Tabla %d/%d: '%s'", i, total_tables, table_name
        )
        results[table_name] = load_dataframe(
            conn=conn,
            schema=schema,
            table_name=table_name,
            df=df,
            batch_size=batch_size,
        )

    # ── Resumen global ───────────────────────────────────────────────────────
    total_inserted = sum(r["total_insertadas"] for r in results.values())
    total_failed = sum(r["total_fallidas"] for r in results.values())
    total_rows = sum(r["total_filas"] for r in results.values())
    total_time = sum(r["tiempo_total_s"] for r in results.values())

    logger.info("=" * 70)
    logger.info("RESUMEN GLOBAL DE CARGA")
    logger.info("  Tablas procesadas : %d", total_tables)
    logger.info("  Filas totales     : %d", total_rows)
    logger.info("  Insertadas        : %d", total_inserted)
    logger.info("  Fallidas          : %d", total_failed)
    logger.info("  Tiempo total      : %.4f s", total_time)

    if total_rows > 0 and total_time > 0:
        tasa = round(total_rows / total_time, 1)
        logger.info("  Throughput aprox. : %.1f filas/s", tasa)

    return results
