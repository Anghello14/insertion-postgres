import pandas as pd
import logging
from pathlib import Path

class ExcelExtractor:
    def __init__(self, ruta_archivo):
        self.ruta = Path(ruta_archivo)
        self.nombre_tabla = self.ruta.stem # Toma el nombre del archivo sin extensión
        
    def extraer_datos_y_estructura(self):
        """
        Lee el Excel y extrae:
        1. La estructura de columnas de la pestaña 'DETALLE_COLUMNAS'.
        2. Los datos crudos de la pestaña 'DATA'.
        """
        logging.info(f"[{self.nombre_tabla}] >>> Iniciando extraccion del archivo: {self.ruta.name}")

        try:
            with pd.ExcelFile(self.ruta) as xls:
                # 1. Extraer Estructura (Metadatos para crear la tabla)
                if 'DETALLE_COLUMNAS' not in xls.sheet_names:
                    logging.error(f"[{self.nombre_tabla}] CRITICO: No existe la pestana 'DETALLE_COLUMNAS'.")
                    raise ValueError(f"Falta pestana DETALLE_COLUMNAS en {self.ruta.name}")
                
                df_estructura = pd.read_excel(xls, sheet_name='DETALLE_COLUMNAS')
                logging.info(f"[{self.nombre_tabla}] Estructura detectada: {len(df_estructura)} columnas definidas.")

                # 2. Extraer Datos Crudos
                # Validamos si la pestaña se llama 'DATA' o 'CLEAN' (según lo que vimos en los archivos)
                pestana_datos = 'DATA' if 'DATA' in xls.sheet_names else 'CLEAN'
                
                if pestana_datos not in xls.sheet_names:
                    logging.error(f"[{self.nombre_tabla}] CRITICO: No se encontro la pestana de datos ('DATA' o 'CLEAN').")
                    raise ValueError(f"No hay pestana de datos en {self.ruta.name}")

                df_datos = pd.read_excel(xls, sheet_name=pestana_datos)
                logging.info(f"[{self.nombre_tabla}] Carga inicial: {len(df_datos)} registros crudos encontrados en pestana '{pestana_datos}'.")

                # Log de auditoria de columnas
                columnas_archivo = df_datos.columns.tolist()
                logging.debug(f"[{self.nombre_tabla}] Columnas en pestana de datos: {columnas_archivo}")

                return df_estructura, df_datos

        except Exception as e:
            logging.error(f"[{self.nombre_tabla}] ERROR durante la lectura del Excel: {str(e)}")
            raise