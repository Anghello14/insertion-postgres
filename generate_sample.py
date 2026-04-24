"""
Script auxiliar para generar un archivo sample_data.xlsx de prueba.
Ejecutar una sola vez: python generate_sample.py
"""
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Hoja 1: Clientes — mezcla intencional de tipos y valores problemáticos
clientes = pd.DataFrame({
    "id": [1, 2, 3, "CUATRO", None, 6, "siete"],
    "nombre": ["Ana García", "José López", None, "María O'Brien", "  ", "Pedro", 123],
    "email": [
        "ana@ejemplo.com",
        "jose@ejemplo",          # email inválido (sin TLD)
        "maria@ejemplo.com",
        None,
        "duplicado@ejemplo.com",
        "duplicado@ejemplo.com", # duplicado
        "pedro@ej.com",
    ],
    "edad": [25, 300, -5, 42, None, "cuarenta", 30],
    "saldo": [1500.50, -200.00, None, 9999999999.99, "N/A", 0.0, 100],
    "activo": [True, False, None, "SI", 1, "yes", True],
    "fecha_registro": [
        "2024-01-15",
        "15/02/2024",  # formato distinto
        "32/13/2024",  # fecha inválida
        None,
        "hoy",
        "2024-06-01",
        "2024-07-22",
    ],
    "notas": [
        "Cliente VIP ★",               # carácter especial Unicode
        "Pendiente revisión",
        None,
        "Texto muy largo: " + "x" * 300,  # valor muy largo
        "",
        "Normal",
        "Carácter raro: €£¥",          # caracteres no-ASCII (moneda)
    ],
})

# Hoja 2: Productos — con columnas numéricas mixtas
productos = pd.DataFrame({
    "codigo": ["P001", "P002", "P003", None, "P005"],
    "descripcion": ["Widget A", "Gadget B", None, "Cosa D", "Producto E"],
    "precio": [10.99, "veinte", None, -5.00, 999.99],
    "stock": [100, 0, 50, "N/D", None],
    "categoria": ["A", "B", "A", "C", "B"],
})

out_path = DATA_DIR / "sample_data.xlsx"
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    clientes.to_excel(writer, sheet_name="clientes", index=False)
    productos.to_excel(writer, sheet_name="productos", index=False)

print(f"Archivo generado: {out_path}")
