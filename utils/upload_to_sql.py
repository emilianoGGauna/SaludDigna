#!/usr/bin/env python3
# upload_to_sql.py — Versión que elimina todas las tablas antes de subir nuevas

import os
import glob
import pandas as pd
import urllib
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from SecretKeys import SecretKeys

# 1. Obtener secretos desde Azure Key Vault
sk = SecretKeys()
server = sk.get("SERVER")
database = sk.get("DATABASE")
user = sk.get("USER")
password = sk.get("PASSWORD")
driver = sk.get("DRIVER") or "{ODBC Driver 17 for SQL Server}"

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

# 3. Cargar archivos de la carpeta ./data
data_dir = os.path.join(os.getcwd(), "data")
patterns = ["*.csv", "*.xls", "*.xlsx"]
files = []
for pat in patterns:
    files.extend(glob.glob(os.path.join(data_dir, pat)))

if not files:
    print(f"❌ No se encontraron archivos en {data_dir}")
    exit(0)

# 4. Cargar archivos según extensión
def load_dataframe(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path, engine="openpyxl")

# 5. Eliminar todas las tablas de la base de datos
def delete_all_tables():
    print("⚠️ Eliminando todas las tablas existentes en la base de datos...")
    try:
        with engine.begin() as conn:
            tables = inspector.get_table_names()
            for tbl in tables:
                print(f"   🗑️ Eliminando tabla: {tbl}")
                conn.execute(text(f"DROP TABLE [{tbl}]"))
        print("✅ Todas las tablas fueron eliminadas.")
    except SQLAlchemyError as e:
        print(f"❌ Error al eliminar tablas: {e}")
        exit(1)

# 6. Proceso principal
def main():
    delete_all_tables()

    for path in files:
        base = os.path.basename(path)
        table_name = os.path.splitext(base)[0].replace(" ", "_")
        print(f"\n➡️ Subiendo archivo '{base}' → tabla '{table_name}'...")

        try:
            df = load_dataframe(path)
        except Exception as e:
            print(f"   ❌ Error al leer el archivo: {e}")
            continue

        try:
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="fail",
                index=False
            )
            print(f"   ✅ Tabla '{table_name}' creada con {len(df)} filas.")
        except SQLAlchemyError as e:
            print(f"   ❌ Error al crear la tabla '{table_name}': {e}")

    print("\n🎉 ¡Carga finalizada! Todas las tablas anteriores fueron eliminadas y se subieron los nuevos archivos.")

if __name__ == "__main__":
    main()
