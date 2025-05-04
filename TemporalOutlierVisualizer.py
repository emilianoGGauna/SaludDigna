import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from OutlierAnalyzer import OutlierAnalyzer

class TemporalOutlierVisualizer:
    def __init__(self):
        # Configuración de outliers en sidebar
        st.sidebar.subheader("Configuración de Outliers")
        self.method = st.sidebar.selectbox("Método de detección:", ['iqr', 'zscore'], index=0)
        if self.method == 'iqr':
            self.k = st.sidebar.slider("Multiplicador IQR (k):", 1.0, 3.0, 1.5, 0.1)
            self.threshold = None
        else:
            self.threshold = st.sidebar.slider("Umbral Z-score:", 2.0, 5.0, 3.0, 0.1)
            self.k = None
        self.color_map = {'Normal': 'royalblue', 'Outlier': 'firebrick'}
        self.analyzer = OutlierAnalyzer()

    def plot_all_variable_outliers(self, df: pd.DataFrame):
        st.subheader("Detección de Outliers por Variable")
        for col in df.columns:
            col_lower = col.lower()
            # Omitir cualquier columna que contenga 'id'
            if 'id' in col_lower:
                continue

            if any(keyword in col_lower for keyword in ['fecha', 'hora', 'minuto']):
                self._plot_datetime(df, col)
            else:
                dtype = df[col].dtype
                if np.issubdtype(dtype, np.number):
                    self._plot_numeric(df, col)
                else:
                    self._plot_categorical(df, col)

    def _ensure_legend(self, fig, states):
        # Asegura que tanto 'Normal' como 'Outlier' estén en la leyenda
        for state in ['Normal', 'Outlier']:
            if state not in states:
                fig.add_trace(
                    go.Scatter(
                        x=[None], y=[None], mode='markers',
                        marker=dict(color=self.color_map[state]),
                        name=state,
                        showlegend=True
                    )
                )

    def _parse_datetime(self, series: pd.Series):
        """
        Intenta parsear una serie a datetime. Si falla, intenta formatos de hora (HH:MM o HH:MM:SS).
        Devuelve la serie datetime o una serie vacía si no hubo éxitos.
        """
        ser = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
        if ser.dropna().empty:
            # Intentar formato time-only
            for fmt in ["%H:%M:%S", "%H:%M"]:
                try:
                    ser = pd.to_datetime(series, format=fmt, errors='coerce')
                    if not ser.dropna().empty:
                        break
                except Exception:
                    continue
        return ser

    def _plot_datetime(self, df: pd.DataFrame, col: str):
        st.markdown(f"### {col} (Temporal)")
        ser = self._parse_datetime(df[col])
        valid = ser.dropna()
        if valid.empty:
            st.warning(f"No se pudieron parsear valores de {col} como datetime ni como hora.")
            return

        # Detección de outliers sobre timestamp numérico
        ts_series = pd.Series(valid.values.astype('int64'), index=valid.index, name=col)
        mask_ts = self.analyzer.detect_outliers(
            ts_series, method=self.method, k=self.k or 1.5, threshold=self.threshold or 3.0
        )
        df_dt = pd.DataFrame({
            'timestamp': valid,
            'Estado': np.where(mask_ts.loc[valid.index], 'Outlier', 'Normal')
        }, index=valid.index)
        states = df_dt['Estado'].unique().tolist()

        # Scatter temporal con outliers resaltados
        fig0 = px.scatter(
            df_dt, x='timestamp', y=[0]*len(df_dt), color='Estado',
            color_discrete_map=self.color_map,
            title=f"Distribución de {col} con Outliers resaltados",
            labels={'timestamp':'Tiempo','y':''}
        )
        fig0.update_yaxes(visible=False)
        self._ensure_legend(fig0, states)
        st.plotly_chart(fig0, use_container_width=True)

        # Determinar si es time-only (año 1900) o fecha completa
        years = valid.dt.year.unique()
        if len(years) == 1 and years[0] == 1900:
            # Tratamiento como hora del día
            df_time = valid.dt.hour.value_counts().sort_index().reset_index()
            df_time.columns = ['Hora', 'Conteo']
            fig = px.bar(
                df_time, x='Hora', y='Conteo',
                title=f"Distribución por hora de {col}",
                labels={'Hora':'Hora del día','Conteo':'Registros'}
            )
            self._ensure_legend(fig, states)
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Tratamiento como serie de fechas completas
            df_real = df_dt[df_dt['timestamp'].dt.year != 1900]
            df_day = df_real['timestamp'].dt.floor('D').value_counts().sort_index().reset_index()
            df_day.columns = ['Fecha', 'Conteo']
            fig = px.line(
                df_day, x='Fecha', y='Conteo', markers=True,
                title=f"Serie diaria de {col}",
                labels={'Fecha':'Fecha','Conteo':'Registros'}
            )
            st.plotly_chart(fig, use_container_width=True)

    def _plot_numeric(self, df: pd.DataFrame, col: str):
        st.markdown(f"### {col} (Numérica)")
        mask = self.analyzer.detect_outliers(
            df[col], method=self.method, k=self.k or 1.5, threshold=self.threshold or 3.0
        )
        df_vis = df[[col]].copy()
        df_vis['Estado'] = np.where(mask, 'Outlier', 'Normal')
        states = df_vis['Estado'].unique().tolist()

        # Violín con caja
        fig1 = px.violin(
            df_vis, y=col, color='Estado', box=True, points='outliers',
            color_discrete_map=self.color_map,
            title=f"Violín de {col} con Outliers"
        )
        fig1.update_traces(
            hovertemplate="<b>%{y}</b><br>Estado: %{customdata}",
            customdata=df_vis['Estado']
        )
        self._ensure_legend(fig1, states)
        st.plotly_chart(fig1, use_container_width=True)

        # Boxplot separado
        fig2 = px.box(
            df_vis, y=col, color='Estado',
            color_discrete_map=self.color_map,
            title=f"Boxplot de {col} con Outliers"
        )
        fig2.update_layout(yaxis_title=col)
        self._ensure_legend(fig2, states)
        st.plotly_chart(fig2, use_container_width=True)

    def _plot_categorical(self, df: pd.DataFrame, col: str):
        st.markdown(f"### {col} (Categórica)")
        df_counts = df[col].value_counts(dropna=False).reset_index()
        df_counts.columns = [col, 'count']
        top_n = st.sidebar.slider(f"Top N para {col}", 3, 20, 10)
        df_top = df_counts.head(top_n)
        fig = px.bar(
            df_top, x=col, y='count',
            title=f"Top {top_n} categorías en {col}",
            color='count', color_continuous_scale='aggrnyl'
        )
        fig.update_layout(xaxis_title=col, yaxis_title='Conteo')
        st.plotly_chart(fig, use_container_width=True)