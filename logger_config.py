"""
logger_config.py
Configura un logger dual: salida a consola (stdout) y a archivo de log rotativo.
Cada ejecución del pipeline genera una entrada de log nueva con timestamp.
"""
import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(logs_dir: Path, name: str = "etl_pipeline") -> logging.Logger:
    """
    Crea y devuelve un logger configurado con dos handlers:
      - StreamHandler  → consola (stdout)
      - FileHandler    → archivo logs/<name>_YYYYMMDD_HHMMSS.log

    El formato incluye: timestamp, nivel, módulo, línea y mensaje.
    """
    logs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"{name}_{timestamp}.log"

    fmt = (
        "%(asctime)s | %(levelname)-8s | %(name)s | "
        "%(module)s:%(lineno)d | %(message)s"
    )
    date_fmt = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Evitar duplicar handlers si se llama más de una vez
    if logger.handlers:
        return logger

    # ── Consola ──────────────────────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # ── Archivo ──────────────────────────────────────────────────────────────
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Logger inicializado. Archivo de log: %s", log_file)
    return logger
