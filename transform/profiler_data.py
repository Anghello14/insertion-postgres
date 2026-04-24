import pandas as pd
import numpy as np
import logging
import re

class DataProfiler:
    def __init__(self, df, nombre_tabla):
        self.df = df
        self.nombre_tabla = nombre_tabla

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _clasificar_tipo_semantico(self, serie):
        """
        Intenta inferir el tipo semántico real de una columna a partir
        de su contenido, independientemente del dtype de pandas.
        """
        muestra = serie.dropna().astype(str).head(200)
        if muestra.empty:
            return "VACIO"

        patron_entero   = re.compile(r'^-?\d+$')
        patron_decimal  = re.compile(r'^-?\d+[\.,]\d+$')
        patron_fecha    = re.compile(
            r'^\d{4}[-/]\d{2}[-/]\d{2}|^\d{2}[-/]\d{2}[-/]\d{4}'
        )
        patron_datetime = re.compile(
            r'^\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}'
        )

        conteos = {
            'entero': muestra.apply(lambda x: bool(patron_entero.match(x))).sum(),
            'decimal': muestra.apply(lambda x: bool(patron_decimal.match(x))).sum(),
            'datetime': muestra.apply(lambda x: bool(patron_datetime.match(x))).sum(),
            'fecha': muestra.apply(lambda x: bool(patron_fecha.match(x))).sum(),
        }

        dominante = max(conteos, key=conteos.get)
        ratio = conteos[dominante] / len(muestra)

        if ratio >= 0.80:
            return dominante.upper()
        return "TEXTO"

    def _detectar_conflictos_potenciales(self, serie, dtype_pandas, tipo_semantico):
        """
        Devuelve lista de advertencias sobre posibles conflictos en Postgres.
        """
        alertas = []
        no_nulos = serie.dropna()

        # Overflow de VARCHAR común (255)
        if dtype_pandas == 'object':
            max_len = no_nulos.astype(str).str.len().max() if not no_nulos.empty else 0
            if max_len > 255:
                alertas.append(f"OVERFLOW_VARCHAR255: longitud max {max_len} > 255")
            if max_len > 4000:
                alertas.append(f"OVERFLOW_VARCHAR4000: longitud max {max_len} > 4000")

        # Valores mixtos (la columna tiene números y texto mezclados)
        if tipo_semantico == 'TEXTO' and serie.dtype == 'object':
            tiene_numeros = no_nulos.astype(str).str.match(r'^-?\d+[\.,]?\d*$').sum()
            if 0 < tiene_numeros < len(no_nulos) * 0.9:
                alertas.append(f"TIPO_MIXTO: {tiene_numeros} valores numericos en columna de texto")

        # Precision overflow en NUMERIC
        if tipo_semantico in ('ENTERO', 'DECIMAL'):
            try:
                numericos = pd.to_numeric(
                    no_nulos.astype(str).str.replace(',', '.', regex=False),
                    errors='coerce'
                ).dropna()
                if not numericos.empty:
                    if numericos.abs().max() > 9.99e37:
                        alertas.append("OVERFLOW_NUMERIC: valor excede rango NUMERIC de Postgres")
            except Exception:
                pass

        # Caracteres especiales / no-ASCII
        if serie.dtype == 'object':
            no_ascii = no_nulos.astype(str).apply(
                lambda x: any(ord(c) > 127 for c in x)
            ).sum()
            if no_ascii > 0:
                alertas.append(f"CHARS_NO_ASCII: {no_ascii} registros con caracteres especiales/unicode")

        # Nulos en columnas que podrían ser NOT NULL por nombre
        nombre = str(serie.name).upper()
        nulos = serie.isnull().sum()
        if nulos > 0 and any(k in nombre for k in ('ID', 'COD', 'CODIGO', 'KEY', 'PK')):
            alertas.append(f"NULOS_EN_CLAVE: {nulos} nulos en columna candidata a PK/NOT NULL")

        return alertas if alertas else ["SIN_ALERTAS"]

    # ------------------------------------------------------------------
    # Método principal
    # ------------------------------------------------------------------

    def perfilar_datos_crudos(self):
        """
        Perfilado profundo sin tocar los datos.
        Captura todo lo necesario para contrastar con errores de Postgres.
        Retorna: (df_original_sin_modificar, df_perfilado)
        """
        logging.info(f"[{self.nombre_tabla}] {'='*60}")
        logging.info(f"[{self.nombre_tabla}] INICIO PERFILADO DE DATOS CRUDOS")
        logging.info(f"[{self.nombre_tabla}] {'='*60}")
        logging.info(f"[{self.nombre_tabla}] Dimensiones: {len(self.df)} filas x {len(self.df.columns)} columnas")
        logging.info(f"[{self.nombre_tabla}] Columnas detectadas: {list(self.df.columns)}")
        logging.info(f"[{self.nombre_tabla}] Memoria estimada del DataFrame: {self.df.memory_usage(deep=True).sum() / 1024:.1f} KB")

        # Nulos totales a nivel tabla
        nulos_totales = self.df.isnull().sum().sum()
        logging.info(f"[{self.nombre_tabla}] Nulos totales en el dataset: {nulos_totales} "
                     f"({nulos_totales / max(self.df.size, 1) * 100:.2f}% del total de celdas)")

        reporte_perfilado = []
        alertas_globales = []

        for columna in self.df.columns:
            serie = self.df[columna]
            total_filas = len(serie)
            nulos = int(serie.isnull().sum())
            no_nulos = serie.dropna()
            pct_nulo = nulos / total_filas * 100 if total_filas > 0 else 0

            dtype_pandas = str(serie.dtype)
            tipo_semantico = self._clasificar_tipo_semantico(serie)

            # Longitudes (sobre representación string real)
            as_str = no_nulos.astype(str)
            longitudes = as_str.str.len()
            len_max  = int(longitudes.max())  if not longitudes.empty else 0
            len_min  = int(longitudes.min())  if not longitudes.empty else 0
            len_prom = round(float(longitudes.mean()), 2) if not longitudes.empty else 0

            # Valores únicos
            n_unicos = int(serie.nunique(dropna=True))
            pct_unicos = round(n_unicos / total_filas * 100, 2) if total_filas > 0 else 0

            # Columna completamente vacía
            todo_nulo = (nulos == total_filas)

            # Estadísticas numéricas (si aplica)
            val_min = val_max = val_media = val_std = None
            if tipo_semantico in ('ENTERO', 'DECIMAL') or dtype_pandas not in ('object', 'bool'):
                try:
                    numericos = pd.to_numeric(
                        no_nulos.astype(str).str.replace(',', '.', regex=False),
                        errors='coerce'
                    ).dropna()
                    if not numericos.empty:
                        val_min  = round(float(numericos.min()), 6)
                        val_max  = round(float(numericos.max()), 6)
                        val_media = round(float(numericos.mean()), 6)
                        val_std  = round(float(numericos.std()), 6)
                except Exception:
                    pass

            # Muestra de valores únicos (hasta 5)
            try:
                muestra_vals = str(no_nulos.drop_duplicates().head(5).tolist())[:300]
            except Exception:
                muestra_vals = "N/A"

            # Alertas / conflictos potenciales con Postgres
            alertas = self._detectar_conflictos_potenciales(serie, dtype_pandas, tipo_semantico)
            tiene_alertas = [a for a in alertas if a != "SIN_ALERTAS"]
            if tiene_alertas:
                alertas_globales.append((columna, tiene_alertas))

            perfil = {
                'COLUMNA'            : columna,
                'TIPO_PANDAS'        : dtype_pandas,
                'TIPO_SEMANTICO'     : tipo_semantico,
                'TOTAL_FILAS'        : total_filas,
                'NULOS'              : nulos,
                'PCT_NULO'           : f"{pct_nulo:.2f}%",
                'TODO_NULO'          : todo_nulo,
                'VALORES_UNICOS'     : n_unicos,
                'PCT_UNICOS'         : f"{pct_unicos:.2f}%",
                'LEN_MIN'            : len_min,
                'LEN_MAX'            : len_max,
                'LEN_PROMEDIO'       : len_prom,
                'VAL_MIN_NUMERICO'   : val_min,
                'VAL_MAX_NUMERICO'   : val_max,
                'VAL_MEDIA'          : val_media,
                'VAL_STD'            : val_std,
                'MUESTRA_VALORES'    : muestra_vals,
                'ALERTAS_POSTGRES'   : " | ".join(alertas),
            }
            reporte_perfilado.append(perfil)

            # Log por columna
            alerta_str = " | ".join(tiene_alertas) if tiene_alertas else "ninguna"
            logging.info(
                f"[{self.nombre_tabla}] COL '{columna}' | pandas={dtype_pandas} | "
                f"semantico={tipo_semantico} | nulos={nulos}({pct_nulo:.1f}%) | "
                f"unicos={n_unicos} | len[{len_min}-{len_max}] | alertas: {alerta_str}"
            )

        df_perfilado = pd.DataFrame(reporte_perfilado)

        # Resumen final de alertas
        logging.info(f"[{self.nombre_tabla}] {'='*60}")
        logging.info(f"[{self.nombre_tabla}] RESUMEN DE ALERTAS POTENCIALES PARA POSTGRES:")
        if alertas_globales:
            for col, alerts in alertas_globales:
                for a in alerts:
                    logging.warning(f"[{self.nombre_tabla}]   ALERTA [{col}] -> {a}")
        else:
            logging.info(f"[{self.nombre_tabla}]   Sin alertas previas detectadas en perfilado.")
        logging.info(f"[{self.nombre_tabla}] Perfilado completado. {len(self.df.columns)} columnas analizadas.")
        logging.info(f"[{self.nombre_tabla}] {'='*60}")

        # Devolvemos el DF original (sin tocar) y el reporte de metadatos
        return self.df, df_perfilado