import pandas as pd
import logging
from pathlib import Path
from datetime import datetime


def generar_reporte_maestro(output_dir: Path, timestamp: str) -> Path:
    """
    Lee todos los AUDITORIA_*.xlsx del output_dir y consolida un reporte maestro
    con las siguientes pestanas:

      1. RESUMEN_MAESTRO         -- Una fila por tabla con sus KPIs
      2. RANKING_TABLAS          -- Tablas ordenadas por tasa de fallo (mayor a menor)
      3. ERRORES_CONSOLIDADOS    -- Todos los errores de todas las tablas en un solo lugar
      4. CATEGORIAS_GLOBAL       -- Distribucion global de categorias de error
      5. COLUMNAS_PROBLEMATICAS  -- Top columnas con mas errores entre todas las tablas
      6. ALERTAS_CONSOLIDADAS    -- Todas las alertas de perfilado de todas las tablas
      7. METADATOS_CONSOLIDADOS  -- Perfil completo de todas las columnas de todas las tablas
    """
    archivos = sorted(output_dir.glob("AUDITORIA_*.xlsx"))
    nombre_reporte = output_dir / f"REPORTE_MAESTRO_{timestamp}.xlsx"

    logging.info("")
    logging.info("=" * 80)
    logging.info("GENERANDO REPORTE MAESTRO")
    logging.info("=" * 80)
    logging.info(f"Archivos de auditoria encontrados: {len(archivos)}")
    for a in archivos:
        logging.info(f"  - {a.name}")

    if not archivos:
        logging.warning("No se encontraron archivos AUDITORIA_*.xlsx en el output. Reporte maestro omitido.")
        return None

    # Acumuladores
    filas_resumen        = []
    filas_errores        = []
    filas_alertas        = []
    filas_metadatos      = []

    for archivo in archivos:
        nombre_tabla = archivo.stem.replace("AUDITORIA_", "")
        logging.info(f"Leyendo: {archivo.name}")

        try:
            xls = pd.ExcelFile(archivo)

            # ── RESUMEN_EJECUTIVO ────────────────────────────────────────────
            if 'RESUMEN_EJECUTIVO' in xls.sheet_names:
                df_res = pd.read_excel(xls, sheet_name='RESUMEN_EJECUTIVO')
                if not df_res.empty:
                    filas_resumen.append(df_res.iloc[0].to_dict())

            # ── ERRORES_POSTGRES ─────────────────────────────────────────────
            if 'ERRORES_POSTGRES' in xls.sheet_names:
                df_err = pd.read_excel(xls, sheet_name='ERRORES_POSTGRES')
                # Ignorar la hoja "sin errores" (tiene solo columna MENSAJE)
                if not df_err.empty and 'CATEGORIA_ERROR' in df_err.columns:
                    df_err.insert(0, 'TABLA_ORIGEN', nombre_tabla)
                    filas_errores.append(df_err)

            # ── ALERTAS_PERFILADO ────────────────────────────────────────────
            if 'ALERTAS_PERFILADO' in xls.sheet_names:
                df_alerta = pd.read_excel(xls, sheet_name='ALERTAS_PERFILADO')
                if not df_alerta.empty and 'COLUMNA' in df_alerta.columns:
                    df_alerta.insert(0, 'TABLA_ORIGEN', nombre_tabla)
                    filas_alertas.append(df_alerta)

            # ── METADATOS_ORIGEN ─────────────────────────────────────────────
            if 'METADATOS_ORIGEN' in xls.sheet_names:
                df_meta = pd.read_excel(xls, sheet_name='METADATOS_ORIGEN')
                if not df_meta.empty:
                    df_meta.insert(0, 'TABLA_ORIGEN', nombre_tabla)
                    filas_metadatos.append(df_meta)

        except Exception as e:
            logging.error(f"Error leyendo {archivo.name}: {e}")
            continue

    # ── Armar DataFrames consolidados ────────────────────────────────────────

    df_resumen_maestro = pd.DataFrame(filas_resumen) if filas_resumen else pd.DataFrame()
    df_errores_todos   = pd.concat(filas_errores,   ignore_index=True) if filas_errores   else pd.DataFrame()
    df_alertas_todas   = pd.concat(filas_alertas,   ignore_index=True) if filas_alertas   else pd.DataFrame()
    df_metadatos_todos = pd.concat(filas_metadatos, ignore_index=True) if filas_metadatos else pd.DataFrame()

    # ── Ranking de tablas por tasa de fallo ──────────────────────────────────
    df_ranking = pd.DataFrame()
    if not df_resumen_maestro.empty:
        df_ranking = df_resumen_maestro.copy()
        # Convertir TASA_FALLA_% a numerico para ordenar
        if 'TASA_FALLA_%' in df_ranking.columns:
            df_ranking['_tasa_falla_num'] = (
                df_ranking['TASA_FALLA_%']
                .astype(str)
                .str.replace('%', '', regex=False)
                .pipe(pd.to_numeric, errors='coerce')
                .fillna(0)
            )
            df_ranking = df_ranking.sort_values('_tasa_falla_num', ascending=False)
            df_ranking = df_ranking.drop(columns=['_tasa_falla_num'])
        # Agregar columna de clasificacion
        def _clasificar(row):
            total = row.get('REGISTROS_PROCESADOS', 0)
            fallas = row.get('INSERCIONES_FALLIDAS', 0)
            if total == 0:
                return 'SIN_DATOS'
            ratio = fallas / total
            if ratio == 0:
                return 'COMPATIBLE_100%'
            elif ratio < 0.05:
                return 'BAJO_RIESGO'
            elif ratio < 0.30:
                return 'RIESGO_MEDIO'
            else:
                return 'ALTO_RIESGO'
        df_ranking['CLASIFICACION_RIESGO'] = df_ranking.apply(_clasificar, axis=1)

    # ── Categorias de error a nivel global ───────────────────────────────────
    df_categorias_global = pd.DataFrame()
    if not df_errores_todos.empty and 'CATEGORIA_ERROR' in df_errores_todos.columns:
        total_errores_global = len(df_errores_todos)
        df_categorias_global = (
            df_errores_todos.groupby('CATEGORIA_ERROR')
            .agg(
                TOTAL_ERRORES=('CATEGORIA_ERROR', 'count'),
                TABLAS_AFECTADAS=('TABLA_ORIGEN', 'nunique'),
            )
            .reset_index()
            .sort_values('TOTAL_ERRORES', ascending=False)
        )
        df_categorias_global['PCT_DEL_TOTAL'] = (
            df_categorias_global['TOTAL_ERRORES'] / total_errores_global * 100
        ).round(2).astype(str) + '%'
        df_categorias_global['DESCRIPCION'] = df_categorias_global['CATEGORIA_ERROR'].map({
            'TIPO_DATO_INCOMPATIBLE' : 'El valor no puede convertirse al tipo de columna en Postgres',
            'OVERFLOW_NUMERICO'      : 'El numero excede el rango del tipo numerico definido',
            'OVERFLOW_TEXTO'         : 'La cadena de texto supera la longitud maxima de la columna',
            'VIOLACION_NOT_NULL'     : 'Se intento insertar NULL en una columna definida como NOT NULL',
            'VIOLACION_UNIQUE'       : 'El valor ya existe en una columna con restriccion UNIQUE/PK',
            'VIOLACION_FK'           : 'El valor no existe en la tabla referenciada (FK)',
            'VIOLACION_CHECK'        : 'El valor no cumple una restriccion CHECK de la tabla',
            'ERROR_ENCODING'         : 'El caracter no es valido en la codificacion del servidor',
            'ERROR_FECHA'            : 'El valor no puede interpretarse como fecha/timestamp valido',
            'ERROR_DESCONOCIDO'      : 'Error no clasificado - revisar MENSAJE_POSTGRES directamente',
        })

    # ── Columnas mas problematicas a nivel global ─────────────────────────────
    df_cols_problematicas = pd.DataFrame()
    if not df_errores_todos.empty and 'COLUMNA_AFECTADA' in df_errores_todos.columns:
        df_cols_problematicas = (
            df_errores_todos[df_errores_todos['COLUMNA_AFECTADA'] != 'N/A']
            .groupby(['TABLA_ORIGEN', 'COLUMNA_AFECTADA', 'CATEGORIA_ERROR'])
            .agg(OCURRENCIAS=('FILA_EXCEL', 'count'))
            .reset_index()
            .sort_values('OCURRENCIAS', ascending=False)
            .head(100)
        )

    # ── Log de totales del reporte maestro ───────────────────────────────────
    total_tablas   = len(df_resumen_maestro)
    total_registros = int(df_resumen_maestro['REGISTROS_PROCESADOS'].sum()) if not df_resumen_maestro.empty and 'REGISTROS_PROCESADOS' in df_resumen_maestro.columns else 0
    total_exitos   = int(df_resumen_maestro['INSERCIONES_EXITOSAS'].sum())   if not df_resumen_maestro.empty and 'INSERCIONES_EXITOSAS'  in df_resumen_maestro.columns else 0
    total_fallas   = int(df_resumen_maestro['INSERCIONES_FALLIDAS'].sum())   if not df_resumen_maestro.empty and 'INSERCIONES_FALLIDAS'  in df_resumen_maestro.columns else 0
    tasa_global    = total_exitos / total_registros * 100 if total_registros > 0 else 0

    logging.info(f"REPORTE MAESTRO | Tablas={total_tablas} | Registros totales={total_registros:,} | "
                 f"OK={total_exitos:,} | FAIL={total_fallas:,} | Tasa exito global={tasa_global:.2f}%")

    if not df_categorias_global.empty:
        cats = df_categorias_global[['CATEGORIA_ERROR', 'TOTAL_ERRORES', 'PCT_DEL_TOTAL']].to_dict('records')
        for c in cats:
            logging.warning(f"  CATEGORIA [{c['CATEGORIA_ERROR']}]: {c['TOTAL_ERRORES']} errores ({c['PCT_DEL_TOTAL']})")

    # ── Escritura del reporte maestro ────────────────────────────────────────
    try:
        with pd.ExcelWriter(nombre_reporte, engine='openpyxl') as writer:

            # 1. Resumen maestro (una fila por tabla)
            if not df_resumen_maestro.empty:
                df_resumen_maestro.to_excel(writer, sheet_name='RESUMEN_MAESTRO', index=False)
            else:
                pd.DataFrame({'MENSAJE': ['Sin datos de resumen disponibles.']}).to_excel(
                    writer, sheet_name='RESUMEN_MAESTRO', index=False)

            # 2. Ranking de tablas por riesgo
            if not df_ranking.empty:
                df_ranking.to_excel(writer, sheet_name='RANKING_TABLAS', index=False)

            # 3. Todos los errores consolidados
            if not df_errores_todos.empty:
                df_errores_todos.to_excel(writer, sheet_name='ERRORES_CONSOLIDADOS', index=False)
            else:
                pd.DataFrame({'MENSAJE': ['Sin errores registrados en ninguna tabla.']}).to_excel(
                    writer, sheet_name='ERRORES_CONSOLIDADOS', index=False)

            # 4. Categorias de error globales
            if not df_categorias_global.empty:
                df_categorias_global.to_excel(writer, sheet_name='CATEGORIAS_GLOBAL', index=False)

            # 5. Top columnas problematicas
            if not df_cols_problematicas.empty:
                df_cols_problematicas.to_excel(writer, sheet_name='COLUMNAS_PROBLEMATICAS', index=False)

            # 6. Alertas consolidadas del perfilado
            if not df_alertas_todas.empty:
                df_alertas_todas.to_excel(writer, sheet_name='ALERTAS_CONSOLIDADAS', index=False)
            else:
                pd.DataFrame({'MENSAJE': ['Sin alertas de perfilado registradas.']}).to_excel(
                    writer, sheet_name='ALERTAS_CONSOLIDADAS', index=False)

            # 7. Metadatos consolidados de todas las columnas de todas las tablas
            if not df_metadatos_todos.empty:
                df_metadatos_todos.to_excel(writer, sheet_name='METADATOS_CONSOLIDADOS', index=False)

        logging.info(f"Reporte maestro generado: {nombre_reporte.name}")
        logging.info("=" * 80)
        return nombre_reporte

    except Exception as e:
        logging.error(f"Error generando reporte maestro: {e}")
        return None
