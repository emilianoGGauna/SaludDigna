#!/usr/bin/env python3
# db_eda_with_keyvault.py
# -*- coding: utf-8 -*-
"""
Enhanced DatabaseEDA using Azure Key Vault for all credentials via SecretKeys.

Features:
- Retrieves SQL connection settings from Key Vault secrets
- Full EDA diagnostics: missing values, unique counts, numeric summaries
- No local AZURE_SQL_* env needed (only KEY_VAULT_URL + AZURE_CLIENT_* for Key Vault)
"""
import logging
import urllib
from typing import Optional, Dict, List

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from scipy import stats

from SecretKeys import SecretKeys  # Change import to match your filename

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class DatabaseEDA:
    """Performs basic EDA on all tables in an Azure SQL database, with credentials from Key Vault."""

    def __init__(self, secret_prefix: str = ""):
        # Initialize Key Vault client
        sk = SecretKeys()
        p = (secret_prefix + "_") if secret_prefix else ""

        # Retrieve connection secrets
        server   = sk.get(p + "SERVER")
        database = sk.get(p + "DATABASE")
        user     = sk.get(p + "USER")
        pwd      = sk.get(p + "PASSWORD")
        driver   = sk.get(p + "DRIVER") or "{ODBC Driver 17 for SQL Server}"

        # Build ODBC string
        odbc = (
            f"DRIVER={driver};"
            f"SERVER={server},1433;"
            f"DATABASE={database};"
            f"UID={user};PWD={pwd};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Connection Timeout=30;Login Timeout=10;"
        )
        params = urllib.parse.quote_plus(odbc)
        url = f"mssql+pyodbc:///?odbc_connect={params}"

        try:
            self.engine = create_engine(
                url, fast_executemany=True, pool_pre_ping=True,
                connect_args={"connect_timeout": 30}
            )
            # Test connection
            with self.engine.connect():
                pass
            self.inspector = inspect(self.engine)
            logger.info("Connected to Azure SQL database '%s' on '%s'", database, server)
        except OperationalError as e:
            logger.exception("Operational error connecting to DB: %s", e)
            raise
        except SQLAlchemyError as e:
            logger.exception("SQLAlchemy error: %s", e)
            raise

    def generate_all_reports(self, sample_frac: float = 1.0) -> Dict[str, pd.DataFrame]:
        """
        Runs a quick EDA on each table, returning a dict:
          { table_name: report_df }
        The report_df has columns:
          - column
          - dtype
          - missing       (count of nulls)
          - missing_pct   (percentage of nulls)
          - n_unique      (count of unique values)
          - mean, std, min, 25%, 50%, 75%, max (for numeric columns)
        """
        reports: Dict[str, pd.DataFrame] = {}

        for table in self.inspector.get_table_names():
            logger.info("Analyzing table: %s", table)
            df = pd.read_sql(f"SELECT * FROM [{table}]", self.engine)

            # sample if requested
            if 0 < sample_frac < 1:
                df = df.sample(frac=sample_frac, random_state=42).reset_index(drop=True)

            # base report
            report = pd.DataFrame({
                "column":    df.columns,
                "dtype":     df.dtypes.values,
                "missing":   df.isna().sum().values,
                "missing_pct": (df.isna().mean().values * 100).round(2),
                "n_unique":  [df[col].nunique() for col in df.columns],
            })

            # numeric summary
            numeric_cols = df.select_dtypes(include="number").columns
            if len(numeric_cols) > 0:
                num_summary = df[numeric_cols].describe().T
                # flatten the columns into a single-level
                num_summary = num_summary.rename(columns={
                    "25%": "pct_25", "50%": "pct_50", "75%": "pct_75"
                })
                num_summary = num_summary[["mean", "std", "min", "pct_25", "pct_50", "pct_75", "max"]]
            else:
                num_summary = pd.DataFrame(
                    columns=["mean", "std", "min", "pct_25", "pct_50", "pct_75", "max"]
                )

            # merge base report with numeric summary
            report = report.merge(
                num_summary,
                left_on="column",
                right_index=True,
                how="left"
            )

            reports[table] = report

        return reports

    def list_tables(self) -> List[str]:
        """Return list of table names in the database."""
        return self.inspector.get_table_names()

    def load_table(self, table_name: str, sample_frac: Optional[float] = None, nrows: Optional[int] = None) -> pd.DataFrame:
        """Load full or sampled subset of a table."""
        sql = f"SELECT * FROM [{table_name}]"
        if nrows:
            sql = f"SELECT TOP {nrows} * FROM [{table_name}]"
        df = pd.read_sql(sql, self.engine)
        if sample_frac:
            df = df.sample(frac=sample_frac, random_state=42).reset_index(drop=True)
        return df


if __name__ == "__main__":
    db = DatabaseEDA()
    reports = db.generate_all_reports(sample_frac=0.1)  # set <1.0 to sample
    for tbl, rpt in reports.items():
        print(f"\n=== EDA Report for {tbl} ===")
        # show base stats
        print(rpt[["column", "dtype", "missing", "missing_pct", "n_unique"]].to_string(index=False))
        # show numeric summary if present
        numeric_cols = ["mean", "std", "min", "pct_25", "pct_50", "pct_75", "max"]
        if rpt[numeric_cols].notna().any().any():
            print("\nNumeric summary:")
            print(rpt[["column"] + numeric_cols].dropna(subset=["mean"]).to_string(index=False))
