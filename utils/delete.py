#!/usr/bin/env python3
# eliminar_todas_menos_dos.py

import logging
from sqlalchemy import create_engine, text
import urllib
from SecretKeys import SecretKeys  # Asegúrate de tener este módulo disponible

# Configura logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Obtiene credenciales desde Azure Key Vault
secrets = SecretKeys()
server = secrets.get("SERVER")
database = secrets.get("DATABASE")
user = secrets.get("USER")
password = secrets.get("PASSWORD")
driver = secrets.get("DRIVER") or "{ODBC Driver 17 for SQL Server}"

# Crea la cadena de conexión
odbc_str = (
    f"DRIVER={driver};"
    f"SERVER={server},1433;"
    f"DATABASE={database};"
    f"UID={user};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}")

# Tablas que quieres conservar
tablas_a_conservar = [
    "Datos_Sin_Outliers_con_sentido",
    "Datos_20Minutos_a_4Horas_con_sentido"
]

# Elimina todas las tablas excepto las que están en la lista de conservación
with engine.begin() as conn:
    sql = f"""
    DECLARE @sql NVARCHAR(MAX) = N'';
    SELECT @sql += 'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name) + ';'
    FROM sys.tables
    WHERE name NOT IN ({','.join(f"'{tbl}'" for tbl in tablas_a_conservar)});
    EXEC sp_executesql @sql;
    """
    conn.execute(text(sql))
    logging.info("✅ Se eliminaron todas las tablas excepto las especificadas.")
