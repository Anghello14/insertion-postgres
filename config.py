"""
config.py
Centraliza la carga de configuración desde .env y define las constantes del pipeline.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables desde .env (si existe)
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


# ── Base de datos PostgreSQL ─────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# URL de conexión para SQLAlchemy
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── Directorios ──────────────────────────────────────────────────────────────
DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data")
LOGS_DIR = BASE_DIR / os.getenv("LOGS_DIR", "logs")

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ── Pipeline ─────────────────────────────────────────────────────────────────
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
