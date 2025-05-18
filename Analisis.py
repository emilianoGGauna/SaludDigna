import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# =========================================
#  Estilo global: 'Old Money' vintage pastel palette
# =========================================
BACKGROUND_COLOR = "#FAF8F0"  # Hueso muy claro
PLOT_BG_COLOR    = "#FAF8F0"
FONT_COLOR       = "#1B3B36"  # Dark Emerald

PALETTE = [
    "#597D72",  # Sage oscuro
    "#B59F7B",  # Beige tostado
    "#C8B1A3",  # Crema suave con matiz rosado
    "#8F8F8F",  # Gris elegante
    "#5E6F63",  # Verde grisáceo
    "#A57C76",  # Taupe intenso
    "#BAA892",  # Arena café más profundo
]


px.defaults.template                = "simple_white"
px.defaults.color_discrete_sequence = PALETTE
px.defaults.color_continuous_scale  = PALETTE

BASE_LAYOUT = dict(
    paper_bgcolor=BACKGROUND_COLOR,
    plot_bgcolor=PLOT_BG_COLOR,
    font=dict(family="Garamond, serif", color=FONT_COLOR, size=14),
    margin=dict(t=100, b=60, l=60, r=40)
)

# =========================================
#  Panel combinado de los 4 primeros gráficos
# =========================================
def plot_combined_panels(df, metrics, category_col='Sucursal', title="Paneles Combinados Extendidos"):
    # Verificación de columnas necesarias
    required_cols = ['PacienteFechaNacimiento', 'Fecha', 'Minutos de espera',
                     'Minutos de atencion', 'TotalTiempo', 'Cumple_20min']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Falta la columna requerida: {col}")

    # Limpieza y formatos
    df['PacienteFechaNacimiento'] = pd.to_datetime(df['PacienteFechaNacimiento'], errors='coerce')
    df['FechaDT'] = pd.to_datetime(df['Fecha'], format='%Y%m%d', errors='coerce')
    df = df.dropna(subset=['PacienteFechaNacimiento', 'FechaDT'])

    # Cálculo de edad
    df['Edad'] = df['PacienteFechaNacimiento'].apply(lambda x: datetime.now().year - x.year)

    # Clasificación por edad
    def clasificar_edad(edad):
        if edad < 18:
            return "Niño/a"
        elif edad < 40:
            return "Joven"
        elif edad < 65:
            return "Adulto"
        return "Adulto mayor"

    # Validación de métricas numéricas
    valid_metrics = [col for col in metrics if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    if len(valid_metrics) < 2:
        raise ValueError("Se requieren al menos 2 métricas numéricas válidas para el mapa de correlaciones.")

    # Crear figura
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=[
            "Distribución de Tiempo Total",
            "Espera vs Atención",
            "Serie Temporal de Atención",
            "Mapa de Correlaciones",
            "Media Diaria por Grupo de Edad",
            "% Cumplimiento < 20 minutos"
        ]
    )

    # Procesar cada categoría
    cats = df[category_col].dropna().unique()
    for i, cat in enumerate(cats):
        visible = (i == 0)
        color = PALETTE[i % len(PALETTE)]
        sub = df[df[category_col] == cat].copy()

        # === Panel 1: Violin plot del tiempo total ===
        fig.add_trace(go.Violin(
            y=sub['TotalTiempo'], name=cat,
            box_visible=True, meanline_visible=True,
            line_color=color, fillcolor=color,
            opacity=0.6, points='all', jitter=0.2,
            marker=dict(size=3, opacity=0.5),
            visible=visible
        ), row=1, col=1)

        # === Panel 2: Scatter + tendencia ===
        fig.add_trace(go.Scatter(
            x=sub['Minutos de espera'], y=sub['Minutos de atencion'],
            mode='markers', name=f"{cat} puntos",
            marker=dict(size=5, color=color, opacity=0.6),
            visible=visible
        ), row=1, col=2)

        try:
            trend = px.scatter(sub, x='Minutos de espera', y='Minutos de atencion', trendline='ols').data[1]
            trend.line.color = color
            trend.name = f"{cat} tendencia"
            trend.visible = visible
            fig.add_trace(trend, row=1, col=2)
        except Exception:
            pass  # Si no hay suficientes datos para OLS

        # === Panel 3: Serie temporal de atención ===
        daily = sub.groupby('FechaDT')['Minutos de atencion'].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=daily['FechaDT'],
            y=daily['Minutos de atencion'],
            mode='lines+markers',
            name=cat,
            line=dict(
                color=color,  # Color diferente
                width=3,             # Línea más gruesa
                dash='solid'           # Línea punteada
            ),
            marker=dict(
                size=7,              # Marcadores más grandes
                symbol='circle'     # Estilo diferente: 'circle', 'square', 'diamond', etc.
            ),
            visible=visible
        ), row=2, col=1)
        # === Panel 4: Mapa de correlaciones ===
        corr = sub[valid_metrics].corr().fillna(0)
        fig.add_trace(go.Heatmap(
            z=corr.values, x=valid_metrics, y=valid_metrics,
            zmin=-1, zmax=1,
            colorscale=PALETTE,
            showscale=False,
            visible=visible
        ), row=2, col=2)

        # === Panel 5: Promedio diario por grupo de edad ===
        sub['GrupoEdad'] = sub['Edad'].apply(clasificar_edad)
        daily_counts = sub.groupby(['FechaDT', 'GrupoEdad']).size().reset_index(name='Conteo')
        avg_daily = daily_counts.groupby('GrupoEdad')['Conteo'].mean().reindex(
            ['Niño/a', 'Joven', 'Adulto', 'Adulto mayor']
        ).fillna(0)

        fig.add_trace(go.Bar(
            x=avg_daily.index, y=avg_daily.values,
            name=f"{cat} - Promedio diario",
            marker=dict(color=color),
            visible=visible
        ), row=3, col=1)

        # === Panel 6: % cumplimiento < 20min ===
        cumplimiento = sub.groupby('FechaDT')['Cumple_20min'].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=cumplimiento['FechaDT'],
            y=cumplimiento['Cumple_20min'] * 100,
            mode='lines+markers',
            name='% Cumple',
            line=dict(
                color=color,     # Cambia a tu color deseado
                width=3,                # Grosor de la línea
                dash='solid'          # Estilo: puedes probar otros
            ),
            visible=visible,
            marker=dict(
                size=7,              # Marcadores más grandes
                symbol='square'     # Estilo diferente: 'circle', 'square', 'diamond', etc.
            )
        ), row=3, col=2)

    # === Menú desplegable por categoría ===
    n_traces_per_cat = len(fig.data) // len(cats)
    buttons = []
    for i, cat in enumerate(cats):
        visibility = [False] * len(fig.data)
        for j in range(n_traces_per_cat):
            visibility[i * n_traces_per_cat + j] = True
        buttons.append(dict(
            label=cat,
            method="update",
            args=[{"visible": visibility},
                  {"title": f"{title} — {cat}"}]
        ))

    # === Layout final ===
    fig.update_layout(
        BASE_LAYOUT,
        title=f"{title} — {cats[0]}",
        height=1300,
        showlegend=False,
        updatemenus=[dict(
            buttons=buttons,
            direction="down",
            x=0.5, xanchor="center",
            y=1.18, yanchor="top"
        )]
    )

    return fig


# =========================================
#  Funciones adicionales de visualización
# =========================================

def plot_histogram_density(df, metric, title, bins=40):
    fig = px.histogram(
        df, x=metric, nbins=bins,
        histnorm='density', marginal='rug',
        title=title, labels={metric: metric},
        color_discrete_sequence=[PALETTE[3]]
    )
    fig.update_traces(marker_line_color=FONT_COLOR, marker_line_width=1)
    fig.update_layout(BASE_LAYOUT)
    return fig

def plot_facet_histogram(df, metric, facet_col, title, wrap=3):
    fig = px.histogram(
        df, x=metric, facet_col=facet_col, facet_col_wrap=wrap,
        title=title, labels={metric: metric},
        color_discrete_sequence=[PALETTE[2]]
    )
    fig.update_traces(marker_line_color=FONT_COLOR, marker_line_width=1)
    fig.update_layout(BASE_LAYOUT)
    return fig

def plot_demand_heatmap(df, date_col, category_col, title):
    df['Hora'] = df[date_col].dt.hour
    daily = df.groupby([df[date_col].dt.date, 'Hora', category_col]).size().reset_index(name='Count')
    pivot = daily.groupby(['Hora', category_col])['Count'].mean().unstack(fill_value=0)
    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=pivot.columns, y=pivot.index,
        colorscale=PALETTE,
        colorbar=dict(title='Pacientes / día')
    ))
    fig.update_layout(
        BASE_LAYOUT,
        title=title,
        xaxis_title=category_col,
        yaxis_title='Hora del día'
    )
    return fig

def plot_avg_demand_line(df, date_col, category_col, title):
    df['Hora'] = df[date_col].dt.hour
    df['Fecha'] = df[date_col].dt.date

    # Conteo diario por hora y sucursal
    daily_counts = df.groupby([category_col, 'Fecha', 'Hora']).size().reset_index(name='Count')

    # Promedio diario por hora y sucursal
    avg_counts = daily_counts.groupby([category_col, 'Hora'])['Count'].mean().reset_index(name='Avg')

    fig = px.line(
        avg_counts, x='Hora', y='Avg', color=category_col,
        title=title,
        labels={'Hora': 'Hora del día', 'Avg': 'Pacientes / día'}
    )
    fig.update_traces(line=dict(width=3))
    fig.update_layout(BASE_LAYOUT)
    return fig


def plot_bar_avg_total_time(df):
    avg = df.groupby('Sucursal')['TotalTiempo'].mean().reset_index()
    fig = px.bar(
        avg, x='Sucursal', y='TotalTiempo',
        title="Tiempo Total Promedio por Sucursal",
        labels={'TotalTiempo': 'Minutos Promedio'},
        color='Sucursal'
    )
    fig.update_layout(BASE_LAYOUT, showlegend=False)
    return fig

def plot_stacked_area_daily_counts(df):
    daily = df.groupby(['FechaDT', 'Sucursal']).size().reset_index(name='Count')
    fig = px.area(
        daily, x='FechaDT', y='Count', color='Sucursal',
        title="Pacientes Diarios por Sucursal (Área Apilada)",
        labels={'FechaDT': 'Fecha', 'Count': '# Pacientes'}
    )
    fig.update_layout(BASE_LAYOUT)
    return fig
