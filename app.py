# app.py
# -*- coding: utf-8 -*-
"""
Temporal Data Analyzer – Streamlit UI (Enhanced Flow)
----------------------------------------------------
Flujo optimizado:
1️⃣ Conectar & Seleccionar tabla
2️⃣ Carga + EDA (gráficas incluidas)
3️⃣ Visualizar outliers
4️⃣ Limpiar datos
5️⃣ Reporte final + Subida
"""

import streamlit as st
from sqlalchemy.exc import OperationalError
from TemporalDataLoader import TemporalDataLoader
from TemporalOutlierVisualizer import TemporalOutlierVisualizer
from TemporalCleaner import TemporalCleaner
from TemporalReport import TemporalReport

# ---------- CONFIGURACIÓN ---------- #
st.set_page_config(page_title="Temporal Data Analyzer", layout="wide")
st.title("🕒 Temporal Data Analyzer")
st.markdown("Sigue el flujo paso a paso: conexión, EDA, outliers, limpieza y reporte final.")

# ---------- STATE ---------- #
if "step" not in st.session_state:
    st.session_state.step = 1
for key in ("raw_df","cleaned_df","cleaning_log","table_name"):
    st.session_state.setdefault(key, None)

# ---------- INICIALIZAR CONEXIÓN ---------- #
@st.cache_resource(ttl=600)
def get_loader():
    return TemporalDataLoader()

try:
    with st.spinner("Verificando conexión a la base de datos…"):
        loader = get_loader()
    st.success("Conexión exitosa 🎉")
except OperationalError as e:
    st.error(f"Error de conexión: {e}")
    st.stop()

visualizer = TemporalOutlierVisualizer()
cleaner    = TemporalCleaner()
reporter   = TemporalReport()

# ---------- NAVEGACIÓN & PROGRESO ---------- #
steps = ["Selección & EDA","Outliers","Limpieza","Reporte"]
choice = st.sidebar.radio("Paso", steps, index=st.session_state.step-1)
st.session_state.step = steps.index(choice) + 1
st.progress(int((st.session_state.step-1)/(len(steps)-1)*100))

def nav(prev_step, next_step):
    c1, c2 = st.columns([1,1])
    if c1.button("⬅️ Anterior"):
        st.session_state.step = prev_step
        st.rerun()
    if c2.button("Siguiente ➡️"):
        st.session_state.step = next_step
        st.rerun()

# ══════════════════════════ #
# STEP 1 – SELECCIÓN & EDA    #
# ══════════════════════════ #
if st.session_state.step == 1:
    st.header("1️⃣ Selección de Tabla & EDA")
    table = loader.select_table()
    if table and st.button("Cargar datos y generar EDA"):
        with st.spinner("Generando EDA..."):
            df = loader.load_data(table)
            st.session_state.raw_df = df
            st.session_state.table_name = table
            st.success(f"Tabla '{table}' cargada ({len(df)} filas).")
    if st.session_state.raw_df is not None:
        st.subheader("Vista previa")
        st.dataframe(st.session_state.raw_df.head(), use_container_width=True)
        st.subheader("📊 EDA completo")
        loader.visualize_summary(st.session_state.table_name)
    nav(1, 2)

# ══════════════════════════ #
# STEP 2 – OUTLIERS          #
# ══════════════════════════ #
elif st.session_state.step == 2:
    st.header("2️⃣ Visualizar Outliers")
    if st.session_state.raw_df is None:
        st.warning("Carga primero una tabla en el paso 1."); st.stop()
    visualizer.plot_all_variable_outliers(st.session_state.raw_df)
    nav(1, 3)

# ══════════════════════════ #
# STEP 3 – LIMPIEZA          #
# ══════════════════════════ #
elif st.session_state.step == 3:
    st.header("3️⃣ Limpiar Datos")
    if st.session_state.raw_df is None:
        st.warning("Carga primero una tabla en el paso 1."); st.stop()
    if st.button("Aplicar limpieza"):
        with st.spinner("Limpiando datos..."):
            cleaned, log = cleaner.clean_data(st.session_state.raw_df)
            st.session_state.cleaned_df = cleaned
            st.session_state.cleaning_log = log
            st.success("Limpieza completada ✅")
    if st.session_state.cleaned_df is not None:
        st.subheader("Previa de datos limpios")
        st.dataframe(st.session_state.cleaned_df.head(), use_container_width=True)
    nav(2, 4)

# ══════════════════════════ #
# STEP 4 – REPORTE FINAL     #
# ══════════════════════════ #
elif st.session_state.step == 4:
    st.header("4️⃣ Reporte Final")
    if st.session_state.cleaned_df is None:
        st.warning("Primero ejecuta la limpieza en el paso 3."); st.stop()
    reporter.show_summary(
        st.session_state.raw_df,
        st.session_state.cleaned_df,
        st.session_state.cleaning_log
    )
    nav(3, 4)

# ══════════════════════════ #
# REINICIAR FLUJO           #
# ══════════════════════════ #
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Reiniciar Flujo"):
    for k in ("step","raw_df","cleaned_df","cleaning_log","table_name"):
        st.session_state[k] = None
    st.rerun()
