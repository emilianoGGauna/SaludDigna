#!/usr/bin/env python3
# upload_to_sql.py — Elimina todas las tablas y sube nuevos archivos,
# filtrando las columnas de fecha y hora a partir del año 2000

import os
import glob
import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from SecretKeys import SecretKeys

# ===== CONFIGURACIÓN =====
MIN_FECHA = pd.Timestamp("2000-01-01")
DATA_DIR = os.path.join(os.getcwd(), "data")
ARCHIVOS_PERMITIDOS = ["*.csv", "*.xls", "*.xlsx"]
# Columnas con fecha/hora a filtrar
COLUMNS_TO_FILTER = [
    "Fecha",
    "Fecha tiempo de atencion",
    "Hora inicio de espera",
    "Hora fin de espera",
    "Hora inicio de atencion",
    "Hora fin de atencion",
    "Hora inicio de espera limpia",
    "Hora fin de espera limpia"
]

# ===== SECRETOS Y CONEXIÓN =====
sk = SecretKeys()
server   = sk.get("SERVER")
database = sk.get("DATABASE")
user     = sk.get("USER")
password = sk.get("PASSWORD")
driver   = sk.get("DRIVER") or "{ODBC Driver 17 for SQL Server}"

# Cadena ODBC (para pyodbc) y URL SQLAlchemy (para DDL)
odbc_str = (
    f"DRIVER={driver};"
    f"SERVER={server},1433;"
    f"DATABASE={database};"
    f"UID={user};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}",
    fast_executemany=True,
    pool_pre_ping=True
)
inspector = inspect(engine)

# ===== FUNCIONES AUXILIARES =====
def encontrar_archivos(directorio: str) -> list:
    """Lista archivos CSV/XLS/XLSX en el directorio dado."""
    archivos = []
    for patrón in ARCHIVOS_PERMITIDOS:
        archivos.extend(glob.glob(os.path.join(directorio, patrón)))
    return archivos


def parse_datetime_series(serie: pd.Series) -> pd.Series:
    """
    Convierte una Serie con valores tipo AAAAMMDD o ISO datetime a datetime64.
    """
    s = serie.astype(str)
    mask_num = s.str.match(r"^\d{8}$")
    out = pd.Series(pd.NaT, index=serie.index)
    # Procesar números AAAAMMDD
    if mask_num.any():
        out[mask_num] = pd.to_datetime(
            s[mask_num], format="%Y%m%d", errors="coerce"
        )
    # Procesar el resto (ISO, datetime)
    rest = ~mask_num
    if rest.any():
        out[rest] = pd.to_datetime(
            serie[rest], errors="coerce"
        )
    return out


def delete_all_tables():
    """Elimina todas las tablas existentes en la base de datos."""
    print("⚠️ Eliminando todas las tablas existentes...")
    try:
        with engine.connect() as conn:
            for tbl in inspector.get_table_names():
                print(f"   🗑️ Eliminando {tbl}")
                conn.execute(text(f"DROP TABLE [{tbl}];"))
    except SQLAlchemyError as e:
        print(f"❌ Error eliminando tablas: {e}")
        exit(1)
    print("✅ Todas las tablas fueron eliminadas.")


def crear_tabla(cursor, nombre: str, df: pd.DataFrame):
    """Crea la tabla en SQL Server con columnas basadas en los dtypes del DataFrame."""
    cols_sql = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            t = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            t = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            t = "BIT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            t = "DATETIME2"
        else:
            t = "NVARCHAR(MAX)"
        cols_sql.append(f"[{col}] {t}")
    ddl = f"CREATE TABLE [{nombre}] ({', '.join(cols_sql)});"
    cursor.execute(ddl)

# ===== PROCESO PRINCIPAL =====
def main():
    archivos = encontrar_archivos(DATA_DIR)
    if not archivos:
        print(f"❌ No se encontraron archivos en {DATA_DIR}")
        return

    delete_all_tables()

    for ruta in archivos:
        nombre = os.path.basename(ruta)
        tabla  = os.path.splitext(nombre)[0].replace(" ", "_")
        print(f"\n➡️ Procesando '{nombre}' → tabla '{tabla}'")

        # Cargar el DataFrame
        ext = os.path.splitext(ruta)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(ruta)
        else:
            df = pd.read_excel(ruta, engine="openpyxl")

        # Aplicar filtro en las columnas de fecha/hora definidas
        for col in COLUMNS_TO_FILTER:
            if col in df.columns:
                df[col] = parse_datetime_series(df[col])
                antes = len(df)
                df = df[df[col] >= MIN_FECHA]
                filt = antes - len(df)
                print(f"   🧹 Filtradas {filt} filas con '{col}' < {MIN_FECHA.date()}")

        # Conexión pyodbc para DDL y DML
        conn_py = pyodbc.connect(odbc_str)
        cursor = conn_py.cursor()
        try:
            cursor.fast_executemany = True
        except:
            pass

        # Crear tabla en BD
        try:
            crear_tabla(cursor, tabla, df)
        except Exception as e:
            print(f"   ❌ Error creando tabla '{tabla}': {e}")
            cursor.close(); conn_py.close()
            continue

        # Insertar datos
        cols         = [f"[{c}]" for c in df.columns]
        placeholders = ",".join("?" for _ in df.columns)
        sql_ins      = f"INSERT INTO [{tabla}] ({','.join(cols)}) VALUES ({placeholders})"
        data         = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]

        if not data:
            print(f"   ⚠️ No hay filas para insertar en '{tabla}', omitiendo.")
        else:
            try:
                cursor.executemany(sql_ins, data)
                conn_py.commit()
                print(f"   ✅ Insertadas {len(data)} filas en '{tabla}'")
            except Exception as e:
                print(f"   ❌ Error insertando en '{tabla}': {e}")
                conn_py.rollback()

        cursor.close()
        conn_py.close()

    print("\n🎉 ¡Carga finalizada!")

if __name__ == '__main__':
    main()