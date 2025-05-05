#!/usr/bin/env python3
# app.py
# -*- coding: utf-8 -*-
import streamlit as st
from sqlalchemy.exc import OperationalError

# IMPORT CORREGIDO: coincide con el nombre de fichero
from TemporalDataLoader import TemporalDataLoader
from TemporalReport import TemporalReport
import pandas as pd

# ---------- CONFIGURACIÓN DE PÁGINA ---------- #
st.set_page_config(
    page_title="Temporal Data Analyzer",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.title("🧪 Temporal Data Analyzer")
st.markdown("Flujo: Selección de tabla → Análisis visual")

# ---------- ESTADO ---------- #
if "step" not in st.session_state:
    st.session_state.step = 1
for key in ("raw_df", "table_name"):
    st.session_state.setdefault(key, None)

# ---------- CARGAR COMPONENTES ---------- #
@st.cache_resource(ttl=600)
def get_loader():
    return TemporalDataLoader()

try:
    with st.spinner("🔌 Conectando a la base de datos..."):
        loader = get_loader()
    st.sidebar.success("Conexión establecida ✅")
except OperationalError as e:
    st.sidebar.error(f"Error de conexión: {e}")
    st.stop()

reporter = TemporalReport()

# ---------- BARRA LATERAL ---------- #
st.sidebar.header("Navegación")
steps = ["Selección & Carga", "Análisis Visual"]
choice = st.sidebar.radio("Paso actual:", steps, index=st.session_state.step - 1)
st.session_state.step = steps.index(choice) + 1

if st.sidebar.button("🔄 Reiniciar flujo"):
    for k in ("step", "raw_df", "table_name"):
        st.session_state[k] = None
    st.rerun()

col_prev, col_next = st.columns([1,1])
def navigate(prev, nxt):
    if col_prev.button("⬅️ Anterior"):
        st.session_state.step = prev
        st.rerun()
    if col_next.button("➡️ Siguiente"):
        st.session_state.step = nxt
        st.rerun()

# ---------- STEP 1: Selección & Carga ---------- #
if st.session_state.step == 1:
    st.header("1️⃣ Selección & Carga de Datos")
    table = loader.select_table()
    if table:
        st.session_state.table_name = table
        if st.button("📥 Cargar datos"):
            with st.spinner("Cargando datos..."):
                df = loader.load_data(table, sample_frac=None, nrows=None)
                st.session_state.raw_df = df
                st.success(f"'{table}' cargada: {len(df)} filas")

    if st.session_state.raw_df is not None:
        st.subheader("Vista previa de datos")
        st.dataframe(st.session_state.raw_df.head(), use_container_width=True)
        st.subheader("📊 Resumen estadístico")
        loader.visualize_summary(st.session_state.table_name)

    navigate(1, 2)

# ---------- STEP 2: Análisis Visual ---------- #
elif st.session_state.step == 2:
    st.header("2️⃣ Análisis Visual Completo")
    if st.session_state.raw_df is None:
        st.warning("Carga primero los datos en el paso 1.")
    else:
        reporter.show_summary(
            original_df=st.session_state.raw_df,
            cleaned_df=st.session_state.raw_df,
            cleaning_log=[]
        )
    navigate(2, 2)
