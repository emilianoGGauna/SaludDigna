#!/usr/bin/env python3
# temporal_report.py
# -*- coding: utf-8 -*-
"""
TemporalReport — Visualización interactiva avanzada con Streamlit y Plotly
Actualización: cada gráfico con color exclusivo y único, rotando paleta.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List, Optional

# Configuración global de Plotly
px.defaults.template = "plotly_white"
px.defaults.width = None
px.defaults.height = 400

# Paleta de colores personalizada
CUSTOM_COLOR_SEQ = px.colors.qualitative.Plotly

class TemporalReport:
    """
    Genera un reporte visual donde cada gráfico usa un color distinto.
    """
    def show_summary(
        self,
        original_df: pd.DataFrame,
        cleaned_df: pd.DataFrame,
        cleaning_log: List[str],
        group_by: Optional[List[str]] = None
    ) -> None:
        st.subheader("📊 Reporte Final de Visualización")
        color_idx = 0

        # 1️⃣ Log y métricas generales (color fijo)
        with st.expander("🔍 Log y Métricas Generales", expanded=True):
            st.markdown("**Log de limpieza**")
            for entry in cleaning_log:
                st.markdown(f"- {entry}")
            st.markdown("---")
            cols = st.columns(4)
            cols[0].metric("Filas Originales", len(original_df))
            cols[1].metric("Filas Limpias", len(cleaned_df), delta=len(cleaned_df)-len(original_df))
            cols[2].metric("Valores Nulos Originales", int(original_df.isna().sum().sum()))
            cols[3].metric("Valores Nulos Restantes", int(cleaned_df.isna().sum().sum()), delta=int(cleaned_df.isna().sum().sum()-original_df.isna().sum().sum()))

        # 2️⃣ Análisis por Grupos con color rotativo
        if group_by:
            st.header("🎨 Distribuciones por Grupos")
            num_cols = cleaned_df.select_dtypes(include='number').columns.tolist()
            for grp in group_by:
                if grp in cleaned_df.columns and num_cols:
                    ycol = num_cols[0]
                    with st.expander(f"Ver violín de {ycol} por {grp}", expanded=False):
                        color = CUSTOM_COLOR_SEQ[color_idx % len(CUSTOM_COLOR_SEQ)]
                        fig = px.violin(
                            cleaned_df,
                            x=grp,
                            y=ycol,
                            color=grp,
                            box=True,
                            points='all',
                            color_discrete_sequence=[color],
                            title=f"Distribución de {ycol} por {grp}"
                        )
                        fig.update_traces(side='positive', spanmode='hard')
                        st.plotly_chart(fig, use_container_width=True)
                        color_idx += 1

        # 3️⃣ Distribuciones Numéricas enriquecidas
        num_cols = cleaned_df.select_dtypes(include='number').columns.tolist()
        if num_cols:
            st.header("📈 Distribuciones Numéricas Enriquecidas")
            for col in num_cols:
                with st.expander(f"Ver distribución de {col}", expanded=False):
                    color = CUSTOM_COLOR_SEQ[color_idx % len(CUSTOM_COLOR_SEQ)]
                    fig = px.histogram(
                        cleaned_df,
                        x=col,
                        nbins=30,
                        histnorm='probability density',
                        marginal='rug',
                        color_discrete_sequence=[color],
                        title=f"Histograma y KDE de {col}"
                    )
                    fig.update_traces(opacity=0.7)
                    st.plotly_chart(fig, use_container_width=True)
                    color_idx += 1

        # 4️⃣ Gráficos Apilados y de Área
        if group_by and num_cols:
            st.header("📊 Barras Apiladas y Área")
            grp = group_by[0]
            metric = num_cols[0]
            df_agg = cleaned_df.groupby(grp)[metric].mean().reset_index()
            with st.expander(f"Barras apiladas de {metric} por {grp}", expanded=False):
                color = CUSTOM_COLOR_SEQ[color_idx % len(CUSTOM_COLOR_SEQ)]
                bar = px.bar(
                    df_agg,
                    x=grp,
                    y=metric,
                    color=grp,
                    color_discrete_sequence=[color],
                    title=f"Promedio de {metric} por {grp}"
                )
                st.plotly_chart(bar, use_container_width=True)
                color_idx += 1
            with st.expander(f"Área acumulada de {metric}", expanded=False):
                df_area = df_agg.sort_values(grp)
                color = CUSTOM_COLOR_SEQ[color_idx % len(CUSTOM_COLOR_SEQ)]
                area = px.area(
                    df_area,
                    x=grp,
                    y=metric,
                    color_discrete_sequence=[color],
                    title=f"Área de promedio de {metric}"
                )
                st.plotly_chart(area, use_container_width=True)
                color_idx += 1

        # 5️⃣ Scatter matrix multicolor
        if len(num_cols) > 1:
            st.header("🔍 Matriz de Dispersión")
            fig_matrix = px.scatter_matrix(
                cleaned_df,
                dimensions=num_cols,
                color=group_by[0] if group_by else None,
                color_discrete_sequence=CUSTOM_COLOR_SEQ,
                title="Scatter matrix de variables numéricas"
            )
            fig_matrix.update_traces(diagonal_visible=False)
            st.plotly_chart(fig_matrix, use_container_width=True)
            color_idx += 1

        # 6️⃣ Series Temporales con color individual
        st.header("🕒 Series Temporales")
        date_cols = list(cleaned_df.select_dtypes(include=['datetime']).columns)
        for col in cleaned_df.select_dtypes(include=['object']):
            parsed = pd.to_datetime(cleaned_df[col], errors='coerce')
            if parsed.notna().mean() > 0.8:
                cleaned_df[col] = parsed
                date_cols.append(col)
        for col in date_cols:
            with st.expander(f"Serie diaria de {col}", expanded=False):
                df_ts = cleaned_df.dropna(subset=[col]).set_index(col).resample('D').size().reset_index(name='Registros')
                color = CUSTOM_COLOR_SEQ[color_idx % len(CUSTOM_COLOR_SEQ)]
                line = px.line(
                    df_ts,
                    x=col,
                    y='Registros',
                    markers=True,
                    color_discrete_sequence=[color],
                    title=f"Serie diaria de registros por {col}"
                )
                st.plotly_chart(line, use_container_width=True)
                color_idx += 1

        # 7️⃣ Matriz de Correlación con paleta divergente
        st.header("🔗 Matriz de Correlación")
        corr = cleaned_df.select_dtypes(include='number').corr()
        heat = px.imshow(
            corr,
            text_auto=True,
            color_continuous_scale=px.colors.diverging.Picnic,
            title="Heatmap de Correlación"
        )
        st.plotly_chart(heat, use_container_width=True)
        color_idx += 1
