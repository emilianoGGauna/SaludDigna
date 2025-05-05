#!/usr/bin/env python3
# db_eda_with_keyvault.py
# -*- coding: utf-8 -*-
"""
Enhanced DatabaseEDA using Azure Key Vault for all credentials via SecretKeys.

Features:
- Retrieves SQL connection settings from Key Vault secrets
- Full EDA diagnostics: missing values, unique counts, numeric summaries
- No local AZURE_SQL_* env needed (only KEY_VAULT_URL + AZURE_CLIENT_* for Key Vault)
- Thread-safe, lazy connections and memoized EDA reports
"""
import logging
import urllib
from functools import cached_property
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from azure.core.exceptions import AzureError
from SecretKeys import SecretKeys

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

class DatabaseEDA:
    """
    Performs basic EDA on all tables in an Azure SQL database, with credentials from Key Vault.
    Use generate_all_reports() to compute or cached_property to inspect metadata.
    """

    def __init__(
        self,
        secret_prefix: str = "",
        connect_timeout: int = 30,
        azure_scope: str = "https://vault.azure.net/.default",
    ):
        # Initialize Key Vault client and verify
        self._secrets = SecretKeys()
        try:
            self._secrets._credential.get_token(azure_scope)
        except AzureError as err:
            logger.warning("Azure credentials verification failed: %s", err)

        prefix = f"{secret_prefix}_" if secret_prefix else ""
        # Fetch secrets
        self._server = self._secrets.get(prefix + "SERVER")
        self._database = self._secrets.get(prefix + "DATABASE")
        self._user = self._secrets.get(prefix + "USER")
        self._password = self._secrets.get(prefix + "PASSWORD")
        self._driver = self._secrets.get(prefix + "DRIVER") or "{ODBC Driver 17 for SQL Server}"
        self._timeout = connect_timeout

    @cached_property
    def engine(self) -> Engine:
        """Create and cache SQLAlchemy engine."""
        odbc = (
            f"DRIVER={self._driver};"
            f"SERVER={self._server},{1433};"
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
                connect_args={"connect_timeout": self._timeout},
            )
            # test connection
            with engine.connect():
                logger.info("Connected to %s/%s", self._server, self._database)
            return engine
        except OperationalError as err:
            logger.exception("Error connecting to database: %s", err)
            raise

    @cached_property
    def inspector(self):
        """Cached SQLAlchemy inspector."""
        return inspect(self.engine)

    def list_tables(self) -> List[str]:
        """Return list of table names in the database."""
        return self.inspector.get_table_names()

    def load_table(
        self,
        table_name: str,
        sample_frac: Optional[float] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load full or sampled subset of a table.
        :param table_name: Table to load
        :param sample_frac: Fraction of rows to sample
        :param nrows: If set, uses TOP nrows
        """
        query = f"SELECT TOP {nrows} * FROM [{table_name}]" if nrows else f"SELECT * FROM [{table_name}]"
        df = pd.read_sql(query, self.engine)
        if sample_frac:
            df = df.sample(frac=sample_frac, random_state=42)
        return df.reset_index(drop=True)

    def _build_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Internal: build a single-table EDA report."""
        base = pd.DataFrame({
            "column": df.columns,
            "dtype": df.dtypes.values,
            "missing": df.isna().sum().values,
            "missing_pct": (df.isna().mean().values * 100).round(2),
            "n_unique": [df[col].nunique() for col in df.columns],
        })
        num_cols = df.select_dtypes(include="number").columns
        if num_cols.any():
            stats = df[num_cols].describe().T.rename(
                columns={"25%": "pct_25", "50%": "pct_50", "75%": "pct_75"}
            )[
                ["mean", "std", "min", "pct_25", "pct_50", "pct_75", "max"]
            ]
        else:
            stats = pd.DataFrame(columns=["mean", "std", "min", "pct_25", "pct_50", "pct_75", "max"])

        return base.merge(stats, left_on="column", right_index=True, how="left")

    def generate_all_reports(
        self,
        sample_frac: float = 1.0,
    ) -> Dict[str, pd.DataFrame]:
        """
        Runs EDA on each table; returns dict of DataFrames.
        :param sample_frac: fraction to sample each table before report
        """
        reports: Dict[str, pd.DataFrame] = {}
        for table in self.list_tables():
            try:
                logger.info("EDA for table: %s", table)
                df = self.load_table(table, sample_frac=sample_frac)
                reports[table] = self._build_report(df)
            except SQLAlchemyError as err:
                logger.error("Failed EDA on %s: %s", table, err)
        return reports

if __name__ == "__main__":
    eda = DatabaseEDA()
    all_reports = eda.generate_all_reports(sample_frac=0.1)
    for tbl, rpt in all_reports.items():
        print(f"\n=== {tbl} ===")
        print(rpt.to_string(index=False))
