#!/usr/bin/env python3
# temporal_data_loader.py
# -*- coding: utf-8 -*-
"""
TemporalDataLoader — Streamlit-friendly loader & summary

- Conexión segura via DatabaseEDA
- Select, load, summarize
- Todos los alias SQL entre corchetes para evitar errores T-SQL
"""
from typing import Optional, Dict, Any

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import Engine

from DatabaseEDA import DatabaseEDA

class TemporalDataLoader:
    def __init__(self, secret_prefix: str = ""):
        self.db_eda = DatabaseEDA(secret_prefix=secret_prefix)
        self.engine: Engine = self.db_eda.engine
        self._table_list: Optional[list[str]] = None

    @property
    def tables(self) -> list[str]:
        if self._table_list is None:
            all_tables = self.db_eda.list_tables()
            import re
            self._table_list = [
                t for t in all_tables 
                if not re.search(r"_limpia_\d{8}_\d{6}$", t)
            ]
        return self._table_list

    def select_table(self) -> Optional[str]:
        if not self.tables:
            st.error("No hay tablas disponibles.")
            return None
        return st.selectbox("Selecciona tabla:", self.tables)

    def load_data(
        self,
        table: str,
        sample_frac: Optional[float] = None,
        nrows: Optional[int] = None
    ) -> pd.DataFrame:
        sql = (
            f"SELECT TOP {nrows} * FROM [{table}]"
            if nrows else
            f"SELECT * FROM [{table}]"
        )
        df = pd.read_sql(sql, self.engine)
        if sample_frac and 0 < sample_frac < 1:
            df = df.sample(frac=sample_frac, random_state=42)
        return df.reset_index(drop=True)

    def summarize_table(self, table: str) -> Dict[str, Any]:
        # Metadata
        meta = pd.read_sql(
            f"SELECT COLUMN_NAME, DATA_TYPE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_NAME = '{table}'",
            self.engine
        )
        types = meta.set_index("COLUMN_NAME").DATA_TYPE.str.lower()
        num_cols = types[types.str.contains(r"int|decimal|numeric|float|double")].index
        dt_cols  = types[types.str.contains(r"date|time|timestamp")].index

        # Construcción dinámica de expresiones
        exprs = []
        for c in num_cols:
            col = f"[{c}]"
            exprs += [
                f"MIN({col})   AS [{c}_min]",
                f"MAX({col})   AS [{c}_max]",
                f"AVG({col})   AS [{c}_avg]",
                f"STDEV({col}) AS [{c}_std]"
            ]
        for c in dt_cols:
            col = f"[{c}]"
            exprs += [
                f"MIN({col}) AS [{c}_min]",
                f"MAX({col}) AS [{c}_max]"
            ]

        sql_query = (
            f"SELECT {', '.join(exprs)} FROM [{table}]"
            if exprs else
            "SELECT 1 AS [dummy]"
        )
        stats_sql = pd.read_sql(sql_query, self.engine)

        # Muestreo para pandas
        sample = self.load_data(table, sample_frac=0.1, nrows=10000)
        corr   = sample.select_dtypes(include=[np.number]).corr()

        return {"stats_sql": stats_sql, "sample": sample, "corr": corr}

    def visualize_summary(self, table: str) -> None:
        s = self.summarize_table(table)

        st.subheader("1️⃣ Estadísticas SQL")
        st.dataframe(s["stats_sql"].T)

        st.subheader("2️⃣ df.info()")
        import io
        buf = io.StringIO()
        s["sample"].info(buf=buf, show_counts=True)
        st.text(buf.getvalue())

        num_desc = s["sample"].select_dtypes(include=[np.number]).describe().T
        if not num_desc.empty:
            st.subheader("3️⃣ Estadísticas numéricas")
            st.dataframe(num_desc)

        obj_desc = s["sample"].select_dtypes(include=["object","category"]).describe(include="all").T
        if not obj_desc.empty:
            st.subheader("4️⃣ Estadísticas categóricas")
            st.dataframe(obj_desc)

        st.subheader("5️⃣ Valores faltantes")
        miss = s["sample"].isna().sum().reset_index(name="missing")
        miss.columns = ["column", "missing"]
        st.dataframe(miss)

        if not s["corr"].empty:
            st.subheader("6️⃣ Correlación numérica")
            st.dataframe(s["corr"].round(3))
