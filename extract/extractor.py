"""
extract/extractor.py
Lee archivos .xlsx desde el directorio de datos configurado y devuelve
un dict de {nombre_tabla: DataFrame} listo para pasar al profiler y al loader.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

logger = logging.getLogger("etl_pipeline")


def load_xlsx_files(data_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Escanea `data_dir` en busca de archivos .xlsx y carga cada hoja
    como un DataFrame independiente.

    Returns
    -------
    dict[str, pd.DataFrame]
        Clave  → nombre seguro para tabla (archivo + hoja, sin espacios).
        Valor  → DataFrame con los datos crudos (sin limpieza ni conversión).
    """
    xlsx_files = sorted(data_dir.glob("*.xlsx"))

    if not xlsx_files:
        logger.warning(
            "No se encontraron archivos .xlsx en el directorio: %s", data_dir
        )
        return {}

    logger.info(
        "Archivos .xlsx encontrados en '%s': %d archivo(s) → %s",
        data_dir,
        len(xlsx_files),
        [f.name for f in xlsx_files],
    )

    datasets: dict[str, pd.DataFrame] = {}

    for xlsx_path in xlsx_files:
        logger.info("=" * 70)
        logger.info("Iniciando extracción del archivo: %s", xlsx_path.name)
        t0 = time.perf_counter()

        try:
            workbook = pd.ExcelFile(xlsx_path, engine="openpyxl")
        except Exception as exc:
            logger.error(
                "No se pudo abrir el archivo '%s'. Tipo de error: %s | Detalle: %s",
                xlsx_path.name,
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            continue

        logger.info(
            "Hojas detectadas en '%s': %s", xlsx_path.name, workbook.sheet_names
        )

        for sheet_name in workbook.sheet_names:
            table_key = _safe_table_name(xlsx_path.stem, sheet_name)
            logger.info(
                "Leyendo hoja '%s' → clave de tabla: '%s'", sheet_name, table_key
            )
            t1 = time.perf_counter()

            try:
                # dtype=object preserva los valores exactamente como están en el xlsx
                df = pd.read_excel(
                    workbook,
                    sheet_name=sheet_name,
                    dtype=object,
                    keep_default_na=False,
                )
            except Exception as exc:
                logger.error(
                    "Error al leer la hoja '%s' del archivo '%s'. "
                    "Tipo: %s | Detalle: %s",
                    sheet_name,
                    xlsx_path.name,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                )
                continue

            elapsed = time.perf_counter() - t1
            logger.info(
                "Hoja '%s' leída correctamente. "
                "Filas: %d | Columnas: %d | Tiempo: %.3f s",
                sheet_name,
                len(df),
                len(df.columns),
                elapsed,
            )
            logger.debug("Columnas de '%s': %s", sheet_name, list(df.columns))

            if df.empty:
                logger.warning(
                    "La hoja '%s' del archivo '%s' está vacía. Se omite.",
                    sheet_name,
                    xlsx_path.name,
                )
                continue

            datasets[table_key] = df

        file_prefix = _safe_table_name(xlsx_path.stem, "")
        total_elapsed = time.perf_counter() - t0
        logger.info(
            "Extracción del archivo '%s' completada en %.3f s. "
            "Hojas cargadas: %d",
            xlsx_path.name,
            total_elapsed,
            sum(1 for k in datasets if k.startswith(file_prefix)),
        )

    logger.info(
        "Extracción global finalizada. Datasets disponibles: %s",
        list(datasets.keys()),
    )
    return datasets


def _safe_table_name(file_stem: str, sheet_name: str) -> str:
    """Genera un nombre de tabla seguro para PostgreSQL."""
    base = f"{file_stem}__{sheet_name}" if sheet_name else file_stem
    # Reemplazar caracteres no válidos con guion bajo
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in base)
    # Truncar a 63 caracteres (límite de PostgreSQL para identificadores)
    safe = safe[:63].lower()
    return safe
