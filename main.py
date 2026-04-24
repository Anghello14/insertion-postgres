import os
import time
import logging
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from extract.excel_reader import ExcelExtractor
from transform.profiler_data import DataProfiler
from load.insert_postgres import PostgresLoader
from load.reporte_derrotero import generar_reporte_auditoria

# Cargar variables de entorno
load_dotenv()

# ── Rutas ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR    = BASE_DIR / "logs"

for folder in [INPUT_DIR, OUTPUT_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────────
timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
log_archivo = LOG_DIR / f"ejecucion_migracion_{timestamp}.log"

logging.basicConfig(
    level=logging.DEBUG,                    # DEBUG → capturamos todo
    format='%(asctime)s [%(levelname)-8s] %(message)s',
    handlers=[
        logging.FileHandler(log_archivo, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# Los módulos de terceros generan mucho ruido en DEBUG; los silenciamos
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('openpyxl').setLevel(logging.WARNING)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _hms(segundos: float) -> str:
    m, s = divmod(int(segundos), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}h {m:02d}m {s:02d}s"


# ── Pipeline ─────────────────────────────────────────────────────────────────

def ejecutar_pipeline():
    t0_global = time.perf_counter()

    logging.info("=" * 80)
    logging.info(f"INICIANDO PIPELINE DE AUDITORIA Y CARGA CRUDA - ID: {timestamp}")
    logging.info("=" * 80)
    logging.info(f"Directorio base   : {BASE_DIR}")
    logging.info(f"Directorio input  : {INPUT_DIR}")
    logging.info(f"Directorio output : {OUTPUT_DIR}")
    logging.info(f"Archivo de log    : {log_archivo}")

    # ── Variables de entorno ─────────────────────────────────────────────────
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_port = os.getenv("DB_PORT", "5432")

    logging.info(f"Config BD -> host={db_host} db={db_name} user={db_user} port={db_port}")

    missing = [k for k, v in {"HOST": db_host, "DB": db_name, "USER": db_user, "PASS": db_pass}.items()
               if v is None]
    if missing:
        logging.critical(f"Faltan variables en .env: {missing}. Abortando.")
        return

    # ── Conexión ─────────────────────────────────────────────────────────────
    try:
        loader = PostgresLoader(host=db_host, database=db_name,
                                user=db_user, password=db_pass, port=db_port)
        loader.conectar()
    except Exception as e:
        logging.critical(f"No se pudo establecer comunicacion con el motor Postgres: {e}")
        logging.debug(traceback.format_exc())
        return

    # ── Escaneo de archivos ──────────────────────────────────────────────────
    archivos_excel = sorted([
        f for f in os.listdir(INPUT_DIR)
        if f.endswith('.xlsx') and not f.startswith('~$')
    ])
    total_archivos = len(archivos_excel)
    logging.info(f"Se detectaron {total_archivos} archivos .xlsx para procesar: {archivos_excel}")

    # Contadores globales
    g_exitos = g_fallas = g_errores_criticos = 0

    for i, nombre_archivo in enumerate(archivos_excel, 1):
        ruta_completa = INPUT_DIR / nombre_archivo
        t0_archivo    = time.perf_counter()

        logging.info("")
        logging.info("-" * 80)
        logging.info(f"[{i}/{total_archivos}] PROCESANDO: {nombre_archivo}  "
                     f"(tamano: {ruta_completa.stat().st_size / 1024:.1f} KB)")
        logging.info("-" * 80)

        try:
            # ── A. EXTRACCIÓN ────────────────────────────────────────────────
            t0 = time.perf_counter()
            extractor = ExcelExtractor(ruta_completa)
            df_estructura, df_datos = extractor.extraer_datos_y_estructura()
            nombre_tabla = extractor.nombre_tabla
            logging.info(f"[{nombre_tabla}] Extraccion completada en "
                         f"{time.perf_counter()-t0:.2f}s | "
                         f"{len(df_datos)} filas | {len(df_datos.columns)} cols")

            # Muestra de las primeras 3 filas para auditoria visual
            logging.debug(f"[{nombre_tabla}] Primeras filas del origen:\n{df_datos.head(3).to_string()}")

            # ── B. PERFILADO ─────────────────────────────────────────────────
            t0 = time.perf_counter()
            profiler = DataProfiler(df_datos, nombre_tabla)
            df_crudo, df_perfilado = profiler.perfilar_datos_crudos()
            logging.info(f"[{nombre_tabla}] Perfilado completado en {time.perf_counter()-t0:.2f}s")

            # ── C. CARGA + DERROTERO ─────────────────────────────────────────
            t0 = time.perf_counter()
            loader.crear_tabla(nombre_tabla, df_estructura)
            df_derrotero = loader.cargar_datos_con_derrotero(nombre_tabla, df_crudo)
            t_carga = time.perf_counter() - t0

            exitos = len(df_crudo) - len(df_derrotero)
            fallas = len(df_derrotero)
            g_exitos += exitos
            g_fallas += fallas

            velocidad = len(df_crudo) / t_carga if t_carga > 0 else 0
            logging.info(f"[{nombre_tabla}] Carga completada en {t_carga:.2f}s | "
                         f"~{velocidad:.0f} reg/s | OK {exitos} | FAIL {fallas}")

            # ── D. REPORTE ───────────────────────────────────────────────────
            t0 = time.perf_counter()
            generar_reporte_auditoria(nombre_tabla, df_perfilado, df_derrotero,
                                      exitos, fallas, OUTPUT_DIR)
            logging.info(f"[{nombre_tabla}] Reporte generado en {time.perf_counter()-t0:.2f}s")

            t_total_archivo = time.perf_counter() - t0_archivo
            logging.info(f"[{nombre_tabla}] ARCHIVO FINALIZADO en {_hms(t_total_archivo)} | "
                         f"OK {exitos} exitos | FAIL {fallas} fallas")

        except Exception as e:
            g_errores_criticos += 1
            logging.error(f"FALLO CRITICO procesando {nombre_archivo}: {e}")
            logging.error(traceback.format_exc())   # traceback completo al log
            continue

    # ── Cierre y resumen global ───────────────────────────────────────────────
    loader.cerrar()
    t_total = time.perf_counter() - t0_global

    logging.info("")
    logging.info("=" * 80)
    logging.info("RESUMEN GLOBAL DEL PIPELINE")
    logging.info("=" * 80)
    logging.info(f"Archivos procesados     : {total_archivos}")
    logging.info(f"Errores criticos (skip) : {g_errores_criticos}")
    logging.info(f"Inserciones exitosas    : {g_exitos}")
    logging.info(f"Inserciones fallidas    : {g_fallas}")
    total_reg = g_exitos + g_fallas
    if total_reg > 0:
        logging.info(f"Tasa de exito global    : {g_exitos/total_reg*100:.2f}%")
    logging.info(f"Tiempo total de ejecucion: {_hms(t_total)}")
    logging.info(f"Reportes de auditoria en : {OUTPUT_DIR}")
    logging.info(f"Log completo en          : {log_archivo}")
    logging.info("=" * 80)


if __name__ == "__main__":
    ejecutar_pipeline()