import streamlit as st
import pandas as pd
import io
from DatabaseEDA import DatabaseEDA
import plotly.express as px

class TemporalReport:
    """Genera un reporte final más amigable, usando tabs y expanders."""

    def show_summary(self, original_df: pd.DataFrame, cleaned_df: pd.DataFrame, log: list[str]) -> None:
        st.subheader("📊 Reporte Final de Limpieza")

        # ──────────────── SECCIÓN: LOG Y MÉTRICAS ────────────────
        with st.expander("🔍 Ver log de limpieza y métricas generales", expanded=True):
            st.markdown("**Log de limpieza**")
            for entry in log:
                st.markdown(f"- {entry}")
            st.markdown("---")

            orig_rows = len(original_df)
            clean_rows = len(cleaned_df)
            orig_nulls = int(original_df.isna().sum().sum())
            clean_nulls = int(cleaned_df.isna().sum().sum())
            removed_rows = orig_rows - clean_rows
            removed_nulls = orig_nulls - clean_nulls

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Filas originales", orig_rows)
            c2.metric("Filas limpias", clean_rows, delta=f"-{removed_rows}")
            c3.metric("Nulos originales", orig_nulls)
            c4.metric("Nulos restantes", clean_nulls, delta=f"-{removed_nulls}")

        # ──────────────── PESTAÑAS PARA SECCIONES ────────────────
        tabs = st.tabs([
            "Estructura & Descripción",
            "Distribuciones",
            "Correlación",
            "Series Temporales"
        ])

        # Tab 1: df.info y describe
        with tabs[0]:
            st.subheader("📝 Estructura (df.info)")
            buf = io.StringIO()
            cleaned_df.info(buf=buf, show_counts=True)
            st.text(buf.getvalue())

            st.markdown("---")
            st.subheader("🔢 Describe numérico")
            st.dataframe(cleaned_df.select_dtypes("number").describe().T)

            st.markdown("---")
            st.subheader("🔤 Value counts categórico")
            for col in cleaned_df.select_dtypes(["object","category"]):
                with st.expander(f"{col} (ver valores)"):
                    vc = cleaned_df[col].value_counts(dropna=False).to_frame("count")
                    st.dataframe(vc)

        # Tab 2: Distribuciones categóricas
        with tabs[1]:
            st.subheader("📊 Top 5 categórico")
            for col in cleaned_df.select_dtypes(["object","category"]):
                data = (
                    cleaned_df[col]
                    .value_counts(dropna=False)
                    .nlargest(5)
                    .reset_index()
                )
                data.columns = [col, f"{col}_count"]
                fig = px.bar(
                    data,
                    x=col,
                    y=f"{col}_count",
                    labels={col: col, f"{col}_count": "count"},
                    title=f"Top 5 de {col}"
                )
                st.plotly_chart(fig, use_container_width=True)

        # Tab 3: Correlación
        with tabs[2]:
            st.subheader("🔗 Mapa de correlación")
            df_corr = cleaned_df.copy()
            for c in df_corr.select_dtypes("datetime"):
                df_corr[c] = df_corr[c].astype("int64")
            for c in df_corr.select_dtypes(["object","category"]):
                df_corr[c] = df_corr[c].astype("category").cat.codes
            corr = df_corr.corr()
            fig = px.imshow(corr, color_continuous_scale="RdBu", zmin=-1, zmax=1)
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)

        # Tab 4: Series Temporales
        with tabs[3]:
            st.subheader("📈 Series temporales")
            potential = list(cleaned_df.select_dtypes("datetime").columns)
            for col in cleaned_df.select_dtypes("object"):
                parsed = pd.to_datetime(cleaned_df[col], errors="coerce")
                if parsed.notna().mean() > 0.8:
                    potential.append(col)
                    cleaned_df[col] = parsed

            if not potential:
                st.info("No hay columnas de fecha para series temporales.")
            else:
                for dt_col in potential:
                    st.markdown(f"**{dt_col}**")
                    df_ts = (
                        cleaned_df
                        .set_index(dt_col)
                        .resample("D")
                        .size()
                        .reset_index(name="count")
                    )
                    fig = px.line(df_ts, x=dt_col, y="count")
                    st.plotly_chart(fig, use_container_width=True)

        # ──────────────── BOTÓN SUBIDA ────────────────
        st.markdown("---")
        if st.button("📤 Subir datos limpios a SQL"):
            base = st.session_state.get("table", "tabla")
            target = f"{base}_limpia"
            db = DatabaseEDA()
            try:
                db.upload_to_sql(
                    df=cleaned_df,
                    table_name=target,
                    if_exists="replace"
                )
                st.success(f"✅ Datos subidos a '{target}'.")
            except Exception as e:
                st.error(f"❌ Error al subir datos: {e}")
