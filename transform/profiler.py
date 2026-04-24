"""
transform/profiler.py
Perfila los datos crudos de cada DataFrame:
  - Tipo de dato Python real por columna y por celda
  - Longitud de cadena (máx, mín, promedio)
  - Conteo de nulos / vacíos
  - Valores únicos y muestra representativa
  - Detección de valores mixtos (columna con múltiples tipos)
  - Codificación y caracteres especiales

NO realiza limpieza ni conversión: los datos pasan tal cual hacia el loader.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

logger = logging.getLogger("etl_pipeline")


# ── Tipos Python simplificados ───────────────────────────────────────────────

def _classify_value(val: Any) -> str:
    """Devuelve una etiqueta legible del tipo Python del valor."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "null"
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, int):
        return "int"
    if isinstance(val, float):
        return "float"
    if isinstance(val, str):
        return "str"
    return type(val).__name__


# ── Perfilado de una columna ─────────────────────────────────────────────────

def _profile_column(series: pd.Series) -> dict:
    """Genera el perfil completo de una columna."""
    total = len(series)
    profile: dict[str, Any] = {
        "total_registros": total,
        "pandas_dtype": str(series.dtype),
    }

    # Conteo por tipo Python real
    type_counts: dict[str, int] = {}
    str_lengths: list[int] = []
    null_count = 0
    empty_str_count = 0
    special_char_count = 0

    for val in series:
        t = _classify_value(val)
        type_counts[t] = type_counts.get(t, 0) + 1

        if t == "null":
            null_count += 1
        elif t == "str":
            length = len(val)
            str_lengths.append(length)
            if length == 0:
                empty_str_count += 1
            # Caracteres fuera del rango ASCII imprimible
            if any(ord(c) > 127 or ord(c) < 32 for c in val):
                special_char_count += 1

    profile["tipos_por_valor"] = type_counts
    profile["tipos_distintos"] = len(type_counts)
    profile["tiene_tipos_mixtos"] = len(type_counts) > 1

    profile["nulos"] = null_count
    profile["porcentaje_nulos"] = round(null_count / total * 100, 2) if total else 0.0
    profile["vacios_cadena"] = empty_str_count

    # Longitudes de cadena
    if str_lengths:
        profile["str_longitud_max"] = max(str_lengths)
        profile["str_longitud_min"] = min(str_lengths)
        profile["str_longitud_promedio"] = round(sum(str_lengths) / len(str_lengths), 2)
    else:
        profile["str_longitud_max"] = None
        profile["str_longitud_min"] = None
        profile["str_longitud_promedio"] = None

    profile["caracteres_especiales_o_no_ascii"] = special_char_count

    # Unicidad
    try:
        unique_vals = series.dropna().unique()
        profile["valores_unicos"] = int(len(unique_vals))
        profile["porcentaje_unicidad"] = (
            round(len(unique_vals) / total * 100, 2) if total else 0.0
        )
        # Muestra de hasta 5 valores únicos representativos
        sample = [str(v) for v in unique_vals[:5]]
        profile["muestra_valores"] = sample
    except Exception as exc:
        logger.warning(
            "No se pudo calcular unicidad en columna. Detalle: %s", exc
        )
        profile["valores_unicos"] = None
        profile["porcentaje_unicidad"] = None
        profile["muestra_valores"] = []

    return profile


# ── Perfilado de un DataFrame ────────────────────────────────────────────────

def profile_dataframe(table_name: str, df: pd.DataFrame) -> dict:
    """
    Genera el perfil completo del DataFrame y lo registra en el logger.

    Returns
    -------
    dict  Perfil estructurado con metadatos del dataset y por columna.
    """
    logger.info("=" * 70)
    logger.info("PERFILADO iniciado para tabla: '%s'", table_name)
    t0 = time.perf_counter()

    profile: dict[str, Any] = {
        "tabla": table_name,
        "total_filas": len(df),
        "total_columnas": len(df.columns),
        "columnas": {},
    }

    logger.info(
        "Dataset '%s' → %d filas × %d columnas",
        table_name,
        len(df),
        len(df.columns),
    )
    logger.debug("Lista de columnas: %s", list(df.columns))

    mixed_type_columns: list[str] = []
    high_null_columns: list[str] = []

    for col in df.columns:
        col_profile = _profile_column(df[col])
        profile["columnas"][col] = col_profile

        # Log detallado por columna
        logger.info(
            "  Columna %-30s | pandas_dtype=%-10s | nulos=%d (%.1f%%) | "
            "tipos_distintos=%d | str_max=%s | unicos=%s",
            f"'{col}'",
            col_profile["pandas_dtype"],
            col_profile["nulos"],
            col_profile["porcentaje_nulos"],
            col_profile["tipos_distintos"],
            col_profile["str_longitud_max"],
            col_profile["valores_unicos"],
        )

        if col_profile["tiene_tipos_mixtos"]:
            mixed_type_columns.append(col)
            logger.warning(
                "  ⚠ TIPOS MIXTOS detectados en columna '%s': %s",
                col,
                col_profile["tipos_por_valor"],
            )

        if col_profile["porcentaje_nulos"] > 50:
            high_null_columns.append(col)
            logger.warning(
                "  ⚠ ALTA TASA DE NULOS (%.1f%%) en columna '%s'",
                col_profile["porcentaje_nulos"],
                col,
            )

        if col_profile["caracteres_especiales_o_no_ascii"] > 0:
            logger.info(
                "  ℹ Caracteres especiales/no-ASCII en columna '%s': %d celda(s)",
                col,
                col_profile["caracteres_especiales_o_no_ascii"],
            )

    # Resumen del dataset
    profile["columnas_con_tipos_mixtos"] = mixed_type_columns
    profile["columnas_con_alta_nulidad"] = high_null_columns

    elapsed = time.perf_counter() - t0
    profile["tiempo_perfilado_s"] = round(elapsed, 4)

    logger.info(
        "PERFILADO completado para '%s' en %.3f s | "
        "Cols con tipos mixtos: %d | Cols con alta nulidad: %d",
        table_name,
        elapsed,
        len(mixed_type_columns),
        len(high_null_columns),
    )

    return profile


# ── Perfil de todos los datasets ─────────────────────────────────────────────

def profile_all(datasets: dict[str, pd.DataFrame]) -> dict[str, dict]:
    """
    Itera sobre todos los datasets y devuelve un diccionario de perfiles.
    Los DataFrames se devuelven sin modificaciones (pasan tal cual).
    """
    profiles: dict[str, dict] = {}
    for table_name, df in datasets.items():
        profiles[table_name] = profile_dataframe(table_name, df)
    logger.info(
        "Perfilado global completado. Tablas perfiladas: %d", len(profiles)
    )
    return profiles
