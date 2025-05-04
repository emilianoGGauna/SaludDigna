from sqlalchemy import create_engine, text
import os
import urllib
from dotenv import load_dotenv

# Carga .env
load_dotenv()
server   = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
user     = os.getenv("AZURE_SQL_USER")
pwd      = os.getenv("AZURE_SQL_PASSWORD")
driver   = os.getenv("AZURE_SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

# Conexión
odbc_str = (
    f"DRIVER={driver};"
    f"SERVER={server};PORT=1433;"
    f"DATABASE={database};UID={user};PWD={pwd};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}")

# Construye y ejecuta el DROP dinámico
with engine.begin() as conn:
    sql = """
    DECLARE @sql NVARCHAR(MAX)=N'';
    SELECT @sql += 'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name) + ';'
    FROM sys.tables
    WHERE name LIKE '%(1)%';
    EXEC sp_executesql @sql;
    """
    conn.execute(text(sql))
    print("Tablas con '(1)' eliminadas.")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import text
from utils.upload_to_sql import engine  # o de donde vengas tu engine

# 0) Conexión ya configurada en upload_to_sql.py
#    engine = create_engine(…)
print("🔴 Eliminando tabla duplicada 'DescripcionDatos' si existiera…")
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS dbo.DescripcionDatos;"))
print("✅ Tabla duplicada eliminada (si existía).")

# 1) Ahora sigue tu flujo de carga/EDA habitual:
#    upload_to_sql.main()
#    db = DatabaseEDA() …etc.
