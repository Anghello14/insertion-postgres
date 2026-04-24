import os
import re
import psycopg2
from psycopg2 import sql
import logging
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# Clasificador de errores de Postgres
# ─────────────────────────────────────────────────────────────────────────────

_CATEGORIAS_ERROR = {
    'TIPO_DATO_INCOMPATIBLE': [
        'invalid input syntax',
        'invalid input value',
        'cannot cast',
        'operator does not exist',
    ],
    'OVERFLOW_NUMERICO': [
        'out of range',
        'numeric field overflow',
        'value too large',
        'integer out of range',
    ],
    'OVERFLOW_TEXTO': [
        'value too long',
        'character varying',
        'character(n)',
    ],
    'VIOLACION_NOT_NULL': [
        'null value in column',
        'violates not-null constraint',
    ],
    'VIOLACION_UNIQUE': [
        'duplicate key',
        'unique constraint',
        'unique violation',
    ],
    'VIOLACION_FK': [
        'foreign key constraint',
        'violates foreign key',
    ],
    'VIOLACION_CHECK': [
        'check constraint',
        'violates check',
    ],
    'ERROR_ENCODING': [
        'invalid byte sequence',
        'invalid character',
        'encoding',
    ],
    'ERROR_FECHA': [
        'date/time field value',
        'invalid value for',
        'out of range for type date',
        'timestamp',
    ],
}

def _categorizar_error(msg: str) -> str:
    msg_lower = msg.lower()
    for categoria, patrones in _CATEGORIAS_ERROR.items():
        for patron in patrones:
            if patron in msg_lower:
                return categoria
    return 'ERROR_DESCONOCIDO'


def _extraer_columna_del_error(msg: str) -> str:
    """
    Intenta extraer el nombre de columna mencionado en el error de Postgres.
    """
    # Patrones comunes: 'column "foo"', 'for column "foo"'
    match = re.search(r'column ["\']?(\w+)["\']?', msg, re.IGNORECASE)
    if match:
        return match.group(1)
    return 'N/A'


# ─────────────────────────────────────────────────────────────────────────────
# Clase principal
# ─────────────────────────────────────────────────────────────────────────────

class PostgresLoader:
    def __init__(self, host, database, user, password, port=5432):
        self.conn_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port
        }
        self.conn = None
        # Mapeo de tipos Oracle/Excel → Postgres
        self.mapa_tipos = {
            'VARCHAR2': 'TEXT',
            'VARCHAR': 'TEXT',
            'NVARCHAR': 'TEXT',
            'CHAR': 'TEXT',
            'CLOB': 'TEXT',
            'NUMBER': 'NUMERIC',
            'FLOAT': 'DOUBLE PRECISION',
            'INTEGER': 'INTEGER',
            'SMALLINT': 'SMALLINT',
            'BIGINT': 'BIGINT',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'BOOLEAN',
            'BLOB': 'BYTEA',
            'RAW': 'BYTEA',
        }

    # ── Conexión ─────────────────────────────────────────────────────────────

    def conectar(self):
        try:
            logging.info(f"Intentando conectar a Postgres | host={self.conn_params['host']} "
                         f"db={self.conn_params['database']} user={self.conn_params['user']} "
                         f"port={self.conn_params['port']}")
            self.conn = psycopg2.connect(**self.conn_params)
            self.conn.autocommit = False
            ver = self.conn.server_version
            logging.info(f">>> Conexion exitosa a PostgreSQL (Docker). Version del servidor: {ver}")
        except Exception as e:
            logging.error(f"Fallo critico conectando a Postgres: {e}")
            raise

    # ── Creación de tabla ────────────────────────────────────────────────────

    def crear_tabla(self, nombre_tabla, df_estructura):
        """
        Recrea la tabla en Postgres usando la definición de DETALLE_COLUMNAS.
        Loguea cada columna con su tipo mapeado.
        """
        cursor = self.conn.cursor()
        columnas_def = []
        columnas_log = []

        logging.info(f"[{nombre_tabla}] {'-'*50}")
        logging.info(f"[{nombre_tabla}] CREACION DE TABLA - {len(df_estructura)} columnas a definir")

        for _, fila in df_estructura.iterrows():
            nombre_col = str(fila['COLUMNA']).strip()
            tipo_crudo = str(fila['TIPO DE DATO']).upper().strip()

            tipo_pg = 'TEXT'   # fallback seguro
            tipo_origen = tipo_crudo
            for clave in self.mapa_tipos:
                if clave in tipo_crudo:
                    tipo_pg = self.mapa_tipos[clave]
                    break

            columnas_def.append(f'"{nombre_col}" {tipo_pg}')
            columnas_log.append((nombre_col, tipo_origen, tipo_pg))
            logging.info(f"[{nombre_tabla}]   COL '{nombre_col}' | origen={tipo_origen} -> postgres={tipo_pg}")

        query_drop   = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier(nombre_tabla.lower()))
        query_create = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(nombre_tabla.lower()),
            sql.SQL(', ').join(map(sql.SQL, columnas_def))
        )

        logging.info(f"[{nombre_tabla}] DDL: {query_create.as_string(cursor)}")

        try:
            cursor.execute(query_drop)
            logging.info(f"[{nombre_tabla}] DROP TABLE IF EXISTS ejecutado.")
            cursor.execute(query_create)
            self.conn.commit()
            logging.info(f"[{nombre_tabla}] Tabla '{nombre_tabla.lower()}' creada exitosamente en Postgres.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"[{nombre_tabla}] ERROR creando tabla: {e}")
            raise
        finally:
            cursor.close()

    # ── Carga con derrotero ──────────────────────────────────────────────────

    def cargar_datos_con_derrotero(self, nombre_tabla, df_datos):
        """
        Inserta registros uno a uno para capturar cada error individualmente.
        Retorna un DataFrame con el derrotero completo de fallas.
        """
        cursor = self.conn.cursor()
        nombre_tabla_pg = nombre_tabla.lower()
        columnas        = [f'"{col}"' for col in df_datos.columns]
        placeholders    = [sql.Placeholder()] * len(columnas)

        query_insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(nombre_tabla_pg),
            sql.SQL(', ').join(map(sql.SQL, columnas)),
            sql.SQL(', ').join(placeholders)
        )

        derrotero_errores = []
        exitos  = 0
        fallas  = 0
        total   = len(df_datos)
        LOG_INTERVALO = max(1, total // 10)   # log cada 10 % del total

        logging.info(f"[{nombre_tabla}] {'-'*50}")
        logging.info(f"[{nombre_tabla}] INICIO DE INSERCION | {total} registros | "
                     f"{len(df_datos.columns)} columnas")
        logging.info(f"[{nombre_tabla}] Columnas: {list(df_datos.columns)}")

        for i, fila in df_datos.iterrows():
            # NaN → None para psycopg2
            valores = []
            for v in fila.values:
                try:
                    valores.append(None if pd.isna(v) else v)
                except (TypeError, ValueError):
                    valores.append(v)

            try:
                cursor.execute(query_insert, valores)
                self.conn.commit()
                exitos += 1

                # Progreso periódico
                procesados = exitos + fallas
                if procesados % LOG_INTERVALO == 0:
                    pct = procesados / total * 100
                    logging.info(f"[{nombre_tabla}] Progreso: {procesados}/{total} "
                                 f"({pct:.1f}%) | OK {exitos} | FAIL {fallas}")

            except Exception as e:
                self.conn.rollback()
                fallas += 1
                error_msg  = str(e).strip()
                categoria  = _categorizar_error(error_msg)
                col_afect  = _extraer_columna_del_error(error_msg)

                # Construir dict del registro problemático
                fila_dict = dict(zip(df_datos.columns, valores))
                valor_col_afect = fila_dict.get(col_afect, 'N/A') if col_afect != 'N/A' else 'N/A'

                registro_error = {
                    'FILA_EXCEL'         : i + 2,          # +2: cabecera + base-0
                    'TABLA'              : nombre_tabla_pg,
                    'CATEGORIA_ERROR'    : categoria,
                    'COLUMNA_AFECTADA'   : col_afect,
                    'VALOR_COLUMNA_AFECT': str(valor_col_afect)[:200],
                    'MENSAJE_POSTGRES'   : error_msg[:500],
                    'DATOS_FILA'         : str(dict(list(fila_dict.items())[:10]))[:500],
                    'TIMESTAMP_ERROR'    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                derrotero_errores.append(registro_error)

                # Log con nivel según categoría
                nivel = logging.WARNING
                if categoria in ('OVERFLOW_TEXTO', 'OVERFLOW_NUMERICO', 'ERROR_ENCODING'):
                    nivel = logging.ERROR
                logging.log(nivel,
                    f"[{nombre_tabla}] FALLA fila={i+2} | cat={categoria} | "
                    f"col={col_afect} | val='{str(valor_col_afect)[:80]}' | {error_msg[:120]}"
                )

        # ── Resumen final ────────────────────────────────────────────────────
        tasa_ok = exitos / total * 100 if total > 0 else 0
        logging.info(f"[{nombre_tabla}] {'-'*50}")
        logging.info(f"[{nombre_tabla}] FIN INSERCION | Total={total} | "
                     f"OK Exitos={exitos} ({tasa_ok:.2f}%) | FAIL Fallas={fallas} "
                     f"({100-tasa_ok:.2f}%)")

        if derrotero_errores:
            df_err = pd.DataFrame(derrotero_errores)
            resumen_cats = df_err['CATEGORIA_ERROR'].value_counts().to_dict()
            logging.warning(f"[{nombre_tabla}] DISTRIBUCION DE ERRORES POR CATEGORIA: {resumen_cats}")
            resumen_cols = df_err['COLUMNA_AFECTADA'].value_counts().head(10).to_dict()
            logging.warning(f"[{nombre_tabla}] COLUMNAS MAS PROBLEMATICAS: {resumen_cols}")
        else:
            logging.info(f"[{nombre_tabla}] Sin errores de insercion - compatibilidad 100%.")

        cursor.close()
        return pd.DataFrame(derrotero_errores)

    # ── Cierre ───────────────────────────────────────────────────────────────

    def cerrar(self):
        if self.conn:
            self.conn.close()
            logging.info("Conexion a PostgreSQL cerrada.")