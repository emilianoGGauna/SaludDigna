#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
upload_to_sql.py

Carga archivos CSV y Excel desde la carpeta `data/` hacia Azure SQL Database.

Variables de entorno (archivo .env en la raíz):
  AZURE_SQL_SERVER   - Ej: sdigna.database.windows.net
  AZURE_SQL_DATABASE - Nombre de la BD (ej: SaludDigna)
  AZURE_SQL_USER     - Usuario SQL
  AZURE_SQL_PASSWORD - Password del usuario
  AZURE_SQL_DRIVER   - Opcional, por defecto '{ODBC Driver 17 for SQL Server}'

Dependencias:
  pandas, sqlalchemy, pyodbc, python-dotenv, openpyxl

Uso:
  python upload_to_sql.py
"""
import os
import glob
import pandas as pd
from dotenv import load_dotenv
import urllib
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

# 1. Cargar variables de entorno
load_dotenv()
server   = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
user     = os.getenv("AZURE_SQL_USER")
password = os.getenv("AZURE_SQL_PASSWORD")
driver   = os.getenv("AZURE_SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

# Verificar variables esenciales
if not all([server, database, user, password]):
    print("❌ Faltan variables de entorno. Define AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USER y AZURE_SQL_PASSWORD en .env")
    exit(1)

# 2. Crear engine SQLAlchemy
odbc_str = (
    f"DRIVER={driver};"
    f"SERVER={server};PORT=1433;"
    f"DATABASE={database};"
    f"UID={user};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
params = urllib.parse.quote_plus(odbc_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
inspector = inspect(engine)

# 3. Detectar archivos en data/
data_dir = os.path.join(os.getcwd(), "data")
patterns = ["*.csv", "*.xls", "*.xlsx"]
files = []
for pat in patterns:
    files.extend(glob.glob(os.path.join(data_dir, pat)))

if not files:
    print(f"No se encontraron archivos en {data_dir}")
    exit(0)

# 4. Función para cargar DataFrame según extensión
def load_dataframe(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path, engine="openpyxl")

# 5. Procesar cada archivo
def main():
    for path in files:
        base = os.path.basename(path)
        table_name = os.path.splitext(base)[0].replace(" ", "_")
        print(f"\n➡️ Procesando archivo '{base}' → tabla '{table_name}'...")

        # 5.1 Verificar existencia de tabla
        try:
            if inspector.has_table(table_name):
                print(f"   ⚠️ La tabla '{table_name}' ya existe. Se omite.")
                continue
        except SQLAlchemyError as e:
            print(f"   ❌ Error al comprobar existencia de tabla: {e}")
            continue

        # 5.2 Cargar datos al DataFrame
        try:
            df = load_dataframe(path)
        except Exception as e:
            print(f"   ❌ Error al leer el archivo: {e}")
            continue

        # 5.3 Crear tabla en SQL Server
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="fail",  # fallará si existe
                index=False
            )
            print(f"   ✅ Tabla '{table_name}' creada con {len(df)} filas.")
        except SQLAlchemyError as e:
            print(f"   ❌ Error al crear la tabla '{table_name}': {e}")

    print("\n🎉 ¡Proceso completado!")

if __name__ == "__main__":
    main()
