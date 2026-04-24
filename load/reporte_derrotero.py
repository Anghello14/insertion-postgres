import pandas as pd
import logging
from pathlib import Path

def generar_reporte_auditoria(nombre_tabla, df_perfilado, df_derrotero, exitos, fallas, ruta_salida):
    """
    Consolida perfilado, estadísticas de error y derrotero en un Excel de auditoría.

    Pestañas generadas:
      1. RESUMEN_EJECUTIVO   — KPIs de la carga
      2. METADATOS_ORIGEN    — Perfilado profundo por columna
      3. ERRORES_POSTGRES    — Derrotero fila a fila
      4. ANALISIS_ERRORES    — Agrupaciones: por categoría, por columna, por hora
      5. ALERTAS_PERFILADO   — Columnas con alertas detectadas antes de insertar
    """
    archivo_reporte = ruta_salida / f"AUDITORIA_{nombre_tabla}.xlsx"

    logging.info(f"[{nombre_tabla}] Generando reporte de auditoria en {archivo_reporte.name} ...")

    total       = exitos + fallas
    tasa_exito  = (exitos / total * 100) if total > 0 else 0
    tasa_falla  = (fallas / total * 100) if total > 0 else 0

    # ── Pestaña 1: Resumen ejecutivo ────────────────────────────────────────
    df_resumen = pd.DataFrame([{
        'TABLA'                     : nombre_tabla,
        'FECHA_EJECUCION'           : pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        'REGISTROS_PROCESADOS'      : total,
        'INSERCIONES_EXITOSAS'      : exitos,
        'INSERCIONES_FALLIDAS'      : fallas,
        'TASA_EXITO_%'              : f"{tasa_exito:.2f}%",
        'TASA_FALLA_%'              : f"{tasa_falla:.2f}%",
        'COLUMNAS_ANALIZADAS'       : len(df_perfilado),
        'OBSERVACION'               : (
            'CARGA TOTALMENTE COMPATIBLE' if fallas == 0
            else f'REVISAR {fallas} ERRORES EN PESTAÑA ERRORES_POSTGRES'
        ),
    }])

    # ── Pestaña 4: Análisis de errores (si hay fallas) ──────────────────────
    df_cat_errores    = pd.DataFrame()
    df_col_errores    = pd.DataFrame()
    df_timeline       = pd.DataFrame()

    if not df_derrotero.empty:
        # Distribución por categoría
        df_cat_errores = (
            df_derrotero.groupby('CATEGORIA_ERROR')
            .agg(
                TOTAL_ERRORES=('CATEGORIA_ERROR', 'count'),
            )
            .reset_index()
            .sort_values('TOTAL_ERRORES', ascending=False)
        )
        df_cat_errores['PCT_DEL_TOTAL'] = (
            df_cat_errores['TOTAL_ERRORES'] / fallas * 100
        ).round(2).astype(str) + '%'

        # Distribución por columna afectada
        df_col_errores = (
            df_derrotero.groupby(['COLUMNA_AFECTADA', 'CATEGORIA_ERROR'])
            .agg(OCURRENCIAS=('FILA_EXCEL', 'count'))
            .reset_index()
            .sort_values('OCURRENCIAS', ascending=False)
        )

        # Timeline: errores por timestamp (si la columna existe)
        if 'TIMESTAMP_ERROR' in df_derrotero.columns:
            df_timeline = (
                df_derrotero.groupby('TIMESTAMP_ERROR')
                .size()
                .reset_index(name='ERRORES_EN_SEGUNDO')
                .sort_values('TIMESTAMP_ERROR')
            )

    # ── Pestaña 5: Alertas del perfilado ────────────────────────────────────
    if 'ALERTAS_POSTGRES' in df_perfilado.columns:
        df_alertas = df_perfilado[
            df_perfilado['ALERTAS_POSTGRES'].str.strip() != 'SIN_ALERTAS'
        ][['COLUMNA', 'TIPO_PANDAS', 'TIPO_SEMANTICO', 'LEN_MAX',
           'NULOS', 'VALORES_UNICOS', 'ALERTAS_POSTGRES']].copy()
    else:
        df_alertas = pd.DataFrame()

    # ── Escritura del Excel ──────────────────────────────────────────────────
    try:
        with pd.ExcelWriter(archivo_reporte, engine='openpyxl') as writer:
            df_resumen.to_excel(writer, sheet_name='RESUMEN_EJECUTIVO', index=False)

            df_perfilado.to_excel(writer, sheet_name='METADATOS_ORIGEN', index=False)

            if not df_derrotero.empty:
                df_derrotero.to_excel(writer, sheet_name='ERRORES_POSTGRES', index=False)
            else:
                pd.DataFrame({
                    'MENSAJE': ['Sin errores. Todos los registros fueron aceptados por Postgres.']
                }).to_excel(writer, sheet_name='ERRORES_POSTGRES', index=False)

            if not df_cat_errores.empty:
                df_cat_errores.to_excel(writer, sheet_name='ANALISIS_ERRORES_CATEG', index=False)
            if not df_col_errores.empty:
                df_col_errores.to_excel(writer, sheet_name='ANALISIS_ERRORES_COLUMNA', index=False)
            if not df_timeline.empty:
                df_timeline.to_excel(writer, sheet_name='TIMELINE_ERRORES', index=False)

            if not df_alertas.empty:
                df_alertas.to_excel(writer, sheet_name='ALERTAS_PERFILADO', index=False)
            else:
                pd.DataFrame({
                    'MENSAJE': ['Sin alertas previas detectadas durante el perfilado.']
                }).to_excel(writer, sheet_name='ALERTAS_PERFILADO', index=False)

        logging.info(f"[{nombre_tabla}] Reporte guardado: {archivo_reporte.name} "
                     f"| KPIs: total={total} exitos={exitos} fallas={fallas} "
                     f"({tasa_falla:.2f}% tasa de error)")
        return archivo_reporte

    except Exception as e:
        logging.error(f"[{nombre_tabla}] Error escribiendo reporte de auditoria: {e}")
        return None