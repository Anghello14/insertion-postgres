# insertion-postgres — Pipeline ETL de Observabilidad

> **Objetivo:** pipeline ETL cuyo propósito principal es **observar el comportamiento de datos crudos** provenientes de archivos `.xlsx` al intentar insertarlos en PostgreSQL. Los errores son el insumo principal del análisis (el «derrotero»).

---

## Estructura del proyecto

```
insertion-postgres/
├── extract/
│   ├── __init__.py
│   └── extractor.py       # Lee archivos .xlsx (datos crudos, sin limpieza)
├── transform/
│   ├── __init__.py
│   └── profiler.py        # Perfila tipos, longitudes, nulos, unicidad — sin modificar datos
├── load/
│   ├── __init__.py
│   └── loader.py          # Inserta en PostgreSQL y documenta cada error con detalle
├── data/                  # Coloque aquí los archivos .xlsx de origen
├── logs/                  # Se genera automáticamente; un .log por ejecución
├── config.py              # Carga .env y expone constantes del pipeline
├── logger_config.py       # Logger dual: consola + archivo
├── main.py                # Punto de entrada
├── generate_sample.py     # Genera data/sample_data.xlsx de prueba
├── requirements.txt
├── .env.example           # Plantilla de variables de entorno
└── README.md
```

---

## Requisitos

- Python **3.10+**
- PostgreSQL accesible en red (servidor externo o local)

---

## Configuración

### 1. Crear y activar el entorno virtual

```bash
python3 -m venv venv
source venv/bin/activate        # Linux / macOS
# venv\Scripts\activate         # Windows
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

```bash
cp .env.example .env
# Edite .env con su editor preferido
```

Contenido de `.env`:

| Variable      | Descripción                                        | Ejemplo      |
|---------------|----------------------------------------------------|--------------|
| `DB_HOST`     | Host del servidor PostgreSQL                       | `localhost`  |
| `DB_PORT`     | Puerto                                             | `5432`       |
| `DB_NAME`     | Nombre de la base de datos                         | `mi_db`      |
| `DB_USER`     | Usuario de PostgreSQL                              | `postgres`   |
| `DB_PASSWORD` | Contraseña                                         | `secreto`    |
| `DB_SCHEMA`   | Esquema destino                                    | `public`     |
| `DATA_DIR`    | Directorio de archivos `.xlsx` (relativo al repo)  | `data`       |
| `LOGS_DIR`    | Directorio de logs                                 | `logs`       |
| `BATCH_SIZE`  | Tamaño del lote de inserción                       | `100`        |

### 4. Colocar archivos .xlsx en `data/`

```bash
cp mis_datos.xlsx data/
# O genere el archivo de muestra:
python generate_sample.py
```

---

## Ejecución

```bash
python main.py
```

La ejecución produce:
- Salida detallada en **consola**
- Archivo `logs/etl_pipeline_YYYYMMDD_HHMMSS.log` con el mismo contenido

---

## Fases del pipeline

### EXTRACT
Lee cada hoja de cada `.xlsx` como un `DataFrame` con `dtype=object` (sin conversión de tipos), preservando los valores exactamente como están en el archivo.

### TRANSFORM (Perfilado)
Por cada columna registra:
- `pandas_dtype` — tipo inferido por pandas
- `tipos_por_valor` — distribución de tipos Python reales (str, int, float, bool, null…)
- `tiene_tipos_mixtos` — ¿hay más de un tipo en la columna?
- `nulos` / `porcentaje_nulos`
- `str_longitud_max` / `str_longitud_min` / `str_longitud_promedio`
- `caracteres_especiales_o_no_ascii`
- `valores_unicos` / `porcentaje_unicidad`
- `muestra_valores` — hasta 5 valores representativos

> **Sin limpieza:** los datos pasan tal cual al loader.

### LOAD
- Crea la tabla destino con todas las columnas como `TEXT` (acepta cualquier valor).
- Intenta inserción en **bulk** por lote con `execute_values`.
- Si el lote falla, hace **fallback fila a fila** para identificar el registro exacto.
- Por cada error captura:
  - Tipo de excepción Python (`IntegrityError`, `DataError`, `OperationalError`, etc.)
  - `pgcode` — código de error PostgreSQL (p. ej. `23505` = unique_violation)
  - `pgerror` — mensaje detallado del servidor
  - Diagnóstico de columna/tabla/constraint (cuando el driver lo expone)
  - Datos completos de la fila que falló
- Al final imprime un **resumen agrupado** de errores por tipo/código/columna.

---

## Logs

Cada ejecución genera un archivo de log nuevo con timestamp:

```
logs/etl_pipeline_20240115_143022.log
```

Formato de cada línea:

```
2024-01-15 14:30:22 | ERROR    | etl_pipeline | loader:198 | ERROR fila #3 (batch #1, offset 2) | Tipo: DataError | pgcode: 22001 | ...
```

---

## Análisis de errores

Los errores documentados permiten construir un derrotero que incluye:

| Dimensión              | Dónde buscarlo en el log          |
|------------------------|-----------------------------------|
| Tipo de error          | `Tipo: DataError`                 |
| Código PostgreSQL      | `pgcode: 22001`                   |
| Columna implicada      | `columna_implicada`               |
| Dato que causó el error| `fila_datos`                      |
| Lotes lentos           | `⚠ LOTE LENTO`                    |
| Tipos mixtos           | `⚠ TIPOS MIXTOS detectados`       |
| Alta nulidad           | `⚠ ALTA TASA DE NULOS`            |
