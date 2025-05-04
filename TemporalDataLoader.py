from OutlierAnalyzer import OutlierAnalyzer
import streamlit as st
import pandas as pd
import numpy as np

class TemporalDataLoader:
    """Carga tablas, genera resumen SQL + Pandas y muestra solo tablas (sin gráficas)."""

    def __init__(self):
        self.analyzer = OutlierAnalyzer()
        self.engine   = self.analyzer.engine
        self.db       = self.analyzer.db

    # ────────────────────────────────────────────
    # Selección y carga de datos
    # ────────────────────────────────────────────
    def select_table(self) -> str:
        """Selector Streamlit que oculta tablas *_limpia_YYYYMMDD_HHMMSS."""
        import re
        visibles = [t for t in self.db.list_tables() if not re.search(r"_limpia_\d{8}_\d{6}$", t)]
        return st.selectbox("Selecciona tabla temporal:", visibles)

    def load_data(self, table: str, sample_frac: float | None = None, nrows: int | None = None) -> pd.DataFrame:
        sql = f"SELECT * FROM [{table}]" if not nrows else f"SELECT TOP {nrows} * FROM [{table}]"
        df = pd.read_sql(sql, self.engine)
        if sample_frac:
            df = df.sample(frac=sample_frac, random_state=42)
        return df.reset_index(drop=True)

    # ────────────────────────────────────────────
    # Resumen SQL + Pandas (sin gráficas)
    # ────────────────────────────────────────────
    def summarize_table(self, table: str) -> dict:
        meta = pd.read_sql(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}'", self.engine)
        types = meta.set_index("COLUMN_NAME").DATA_TYPE.str.lower()
        num_cols = types[types.str.contains(r"int|decimal|numeric|float|double")].index.tolist()
        dt_cols  = types[types.str.contains(r"date|time|timestamp")].index.tolist()

        # Stats via SQL
        exprs = []
        for c in num_cols:
            exprs += [f"MIN([{c}]) AS {c}_min", f"MAX([{c}]) AS {c}_max", f"AVG([{c}]) AS {c}_avg", f"STDEV([{c}]) AS {c}_std"]
        for c in dt_cols:
            exprs += [f"MIN([{c}]) AS {c}_min", f"MAX([{c}]) AS {c}_max"]
        stats_sql = pd.read_sql(f"SELECT {', '.join(exprs)} FROM [{table}]" if exprs else "SELECT 1 AS dummy", self.engine)

        # Sample Pandas
        df_sample = self.load_data(table, sample_frac=0.1, nrows=10_000)
        corr = df_sample.select_dtypes(include=[np.number]).corr()

        return {
            "stats_sql": stats_sql,
            "sample":    df_sample,
            "corr":      corr
        }

    # ────────────────────────────────────────────
    # Visualización: solo tablas y texto
    # ────────────────────────────────────────────
    def visualize_summary(self, table: str):
        s = self.summarize_table(table)

        # 1. Stats SQL
        st.subheader("1️⃣ Estadísticas SQL (numéricas & datetime)")
        st.dataframe(s["stats_sql"].T)

                # 2. df.info()
        st.subheader("2️⃣ df.info()")
        import io
        buf = io.StringIO()
        s["sample"].info(buf=buf)
        st.text(buf.getvalue())

        # 3. describe() numérico describe() numérico
        num_desc = s["sample"].select_dtypes(include=[np.number]).describe().T
        if not num_desc.empty:
            st.subheader("3️⃣ describe() numérico")
            st.dataframe(num_desc)

        # 4. describe(include='object')
        obj_desc = s["sample"].select_dtypes(include=['object','category']).describe(include='all').T
        if not obj_desc.empty:
            st.subheader("4️⃣ describe() categórico")
            st.dataframe(obj_desc)

        # 5. Missing por columna
        st.subheader("5️⃣ Valores faltantes por columna")
        miss = s["sample"].isna().sum().reset_index(name="missing")
        miss.columns = ["column", "missing"]
        st.dataframe(miss)

        # 6. Correlación (tabla)
        if not s["corr"].empty:
            st.subheader("6️⃣ Matriz de correlación")
            st.dataframe(s["corr"].round(3))
