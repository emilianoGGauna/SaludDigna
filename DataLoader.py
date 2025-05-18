# DataLoader.py
import logging
import re
import urllib
from typing import List, Optional

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from azure.core.exceptions import AzureError

from SecretKeys import SecretKeys

# Configura el logging
default_format = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=default_format)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(
        self,
        secret_prefix: str = "",
        connect_timeout: int = 30,
        min_fecha: int = 19800101
    ):
        self._secrets = SecretKeys()
        prefix = f"{secret_prefix}_" if secret_prefix else ""
        self.min_fecha = min_fecha

        # Verificar credenciales de Azure Key Vault
        try:
            self._secrets._credential.get_token("https://vault.azure.net/.default")
        except AzureError as err:
            logger.warning("La verificación de credenciales de Azure falló: %s", err)

        # Leer secretos
        self._server = self._secrets.get(prefix + "SERVER")
        self._database = self._secrets.get(prefix + "DATABASE")
        self._user = self._secrets.get(prefix + "USER")
        self._password = self._secrets.get(prefix + "PASSWORD")
        self._driver = self._secrets.get(prefix + "DRIVER") or "{ODBC Driver 17 for SQL Server}"
        self._timeout = connect_timeout

        # Motor para inspección de tablas
        self.engine: Engine = self._create_engine()
        self.inspector = inspect(self.engine)

    def _create_engine(self) -> Engine:
        odbc = (
            f"DRIVER={self._driver};"
            f"SERVER={self._server},1433;"
            f"DATABASE={self._database};"
            f"UID={self._user};PWD={self._password};"
            "Encrypt=yes;TrustServerCertificate=no;"
            f"Connection Timeout={self._timeout};"
        )
        params = urllib.parse.quote_plus(odbc)
        url = f"mssql+pyodbc:///?odbc_connect={params}"
        try:
            engine = create_engine(
                url,
                fast_executemany=True,
                pool_pre_ping=True,
            )
            with engine.connect():
                logger.info("Conectado a %s/%s", self._server, self._database)
            return engine
        except OperationalError as err:
            logger.exception("La conexión a la base de datos falló: %s", err)
            raise

    def list_tables(self) -> List[str]:
        """Lista todas las tablas de usuario, excluyendo temporales/limpias."""
        tables = self.inspector.get_table_names()
        return [t for t in tables if not re.search(r"_limpia_\d{8}_\d{6}$", t)]

    def load_table(
        self,
        table: str,
        nrows: Optional[int] = None,
        sample_frac: Optional[float] = None
    ) -> pd.DataFrame:
        """Carga datos de una tabla específica, aplica filtrado y preprocesamiento estándar."""
        # Construir consulta con filtro mínimo de fecha
        top_clause = f"TOP {nrows}" if nrows else ""
        query = (
            f"SELECT {top_clause} * "
            f"FROM [{table}] "
            f"WHERE Fecha >= {self.min_fecha}"
        )

        # Cadena ODBC para pyodbc
        odbc_str = (
            f"DRIVER={self._driver};"
            f"SERVER={self._server},1433;"
            f"DATABASE={self._database};"
            f"UID={self._user};PWD={self._password};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )

        # Conexión y carga
        conn = pyodbc.connect(odbc_str, timeout=self._timeout)
        try:
            df = pd.read_sql(query, con=conn)
        finally:
            conn.close()

        # Muestreo opcional
        if sample_frac and 0 < sample_frac < 1:
            df = df.sample(frac=sample_frac, random_state=42)

        # ----------------------------
        # Preprocesamiento estándar
        # ----------------------------
        # Convertir Fecha a datetime y aplicar filtro nuevamente como seguridad
        df['FechaDT'] = pd.to_datetime(df['Fecha'], format='%Y%m%d', errors='coerce')
        df = df[df['FechaDT'] >= pd.to_datetime(str(self.min_fecha), format='%Y%m%d')]
        df.dropna(subset=['FechaDT'], inplace=True)

        # Convertir horas de espera y atención
        df['InicioEsperaDT'] = pd.to_datetime(df['Hora inicio de espera limpia'], errors='coerce')
        df['InicioAtencionDT'] = pd.to_datetime(df['Hora inicio de atencion'], errors='coerce')
        df.dropna(subset=['InicioEsperaDT', 'InicioAtencionDT'], inplace=True)

        # Calcular total de tiempo y día de la semana
        df['TotalTiempo'] = df['Minutos de espera'] + df['Minutos de atencion']
        df['DiaSemana'] = df['InicioEsperaDT'].dt.day_name()

        return df.reset_index(drop=True)