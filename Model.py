# model_plots.py

import pandas as pd
import numpy as np
from scipy.optimize import milp, LinearConstraint, Bounds
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from DataLoader import DataLoader

# Paleta de colores y estilos
PALETTE = ['#597D72', '#B59F7B', '#C8B1A3']
BG_COLOR = '#FAF8F0'
FONT_COLOR = '#1B3B36'

def load_and_preprocess():
    loader = DataLoader()
    tables = loader.list_tables()
    if not tables:
        raise ValueError('No hay datos disponibles')
    df = loader.load_table(tables[0])
    df.columns = df.columns.str.upper().str.replace(' ', '_')
    df['FECHA'] = pd.to_datetime(df.get('FECHA'), errors='coerce')
    hora_col = [c for c in df.columns if 'HORA_INICIO' in c][0]
    df['HORA'] = pd.to_datetime(df[hora_col], errors='coerce').dt.hour.fillna(0).astype(int)
    return df


def optimize_staff(demand, full_shifts, part_shifts, cost_full, cost_part, capacity):
    """
    Optimiza la asignación de empleados full-time y part-time.
    Asegura cobertura mínima (>= demanda) y al menos un empleado en cada hora.
    """
    I, J = len(full_shifts), len(part_shifts)
    # Costos por turno
    c = np.concatenate([np.full(I, cost_full), np.full(J, cost_part)])
    hours = demand.index.tolist()
    T = len(hours)

    # Matriz de capacidad (A) y presencia (B)
    A = np.zeros((T, I + J))
    B = np.zeros((T, I + J))
    for i, hrs in enumerate(full_shifts.values()):
        for t_idx, t in enumerate(hours):
            if t in hrs:
                A[t_idx, i] = capacity  # cobertura en pacientes
                B[t_idx, i] = 1         # presencia de empleado
    for j, hrs in enumerate(part_shifts.values(), start=I):
        for t_idx, t in enumerate(hours):
            if t in hrs:
                A[t_idx, j] = capacity
                B[t_idx, j] = 1

    # Restricción: A * x >= demanda
    cons_capacity = LinearConstraint(-A, -np.inf, -demand.values)
    # Restricción: B * x >= 1 (al menos un empleado cada hora)
    cons_min_presence = LinearConstraint(-B, -np.inf, -np.ones(T))

    # Resolver MILP
    res = milp(
        c=c,
        constraints=[cons_capacity, cons_min_presence],
        bounds=Bounds(0, np.inf),
        integrality=np.ones(I + J, int)
    )

    x = np.round(res.x).astype(int)
    full_sol = dict(zip(full_shifts.keys(), x[:I]))
    part_sol = dict(zip(part_shifts.keys(), x[I:]))
    return full_sol, part_sol


def build_figure(df, cost_full, cost_part, capacity):
    branches = sorted(df['SUCURSAL'].unique())
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=('Empleados vs Pacientes', 'Costos por Hora')
    )

    all_vis = []
    for branch in branches:
        ndf = df[df['SUCURSAL'] == branch]
        daily = ndf.groupby([ndf['FECHA'].dt.date.rename('DIA'), 'HORA']) \
                   .size().reset_index(name='CNT')
        avg = daily.groupby('HORA')['CNT'] \
                   .mean().reindex(range(6, 19), fill_value=0)

        full_shifts = {f'FT_{h}': list(range(h, h + 8)) for h in range(6, 13)}
        part_shifts = {f'PT_{h}': list(range(h, h + 4)) for h in range(6, 16)}

        full_sol, part_sol = optimize_staff(
            avg, full_shifts, part_shifts,
            cost_full, cost_part, capacity
        )

        cov_ft = pd.Series(0, index=avg.index)
        cov_pt = pd.Series(0, index=avg.index)
        cost_ft = pd.Series(0.0, index=avg.index)
        cost_pt = pd.Series(0.0, index=avg.index)

        for tid, n in full_sol.items():
            for h in full_shifts[tid]:
                if h in cov_ft.index:
                    cov_ft[h] += n * capacity
                    cost_ft[h] += n * cost_full
        for tid, n in part_sol.items():
            for h in part_shifts[tid]:
                if h in cov_pt.index:
                    cov_pt[h] += n * capacity
                    cost_pt[h] += n * cost_part

        # Agregar trazas al gráfico
        fig.add_trace(
            go.Bar(x=avg.index, y=cov_ft / capacity, name='FT Empleados',
                   marker_color=PALETTE[0], visible=False), row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=avg.index, y=cov_pt / capacity, name='PT Empleados',
                   marker_color=PALETTE[1], visible=False), row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=avg.index, y=avg, mode='lines+markers',
                       name='Pacientes', marker_color=PALETTE[2], visible=False),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(x=avg.index, y=cost_ft, mode='lines+markers',
                       name='Costo FT', marker_color=PALETTE[0], visible=False),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=avg.index, y=cost_pt, mode='lines+markers',
                       name='Costo PT', marker_color=PALETTE[1], visible=False),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=avg.index, y=cost_ft + cost_pt, mode='lines+markers',
                       name='Costo Total', marker_color=PALETTE[2], visible=False),
            row=2, col=1
        )

        all_vis.extend([True] * 6 if branch == branches[0] else [False] * 6)

    # Configurar visibilidad inicial
    for i, trace in enumerate(fig.data):
        trace.visible = all_vis[i]

    # Botones para cambiar sucursal
    buttons = []
    for bi, branch in enumerate(branches):
        vis = [False] * len(fig.data)
        for j in range(bi * 6, bi * 6 + 6):
            vis[j] = True
        buttons.append(dict(
            label=branch,
            method='update',
            args=[{'visible': vis}, {'title.text': f'Demanda & Optimización — {branch}'}]
        ))

    fig.update_layout(
        updatemenus=[dict(active=0, x=0.1, y=1.15,
                          xanchor='left', yanchor='top', buttons=buttons)],
        plot_bgcolor=BG_COLOR, paper_bgcolor=BG_COLOR,
        font_color=FONT_COLOR, height=800
    )
    fig.update_xaxes(title_text='Hora')
    fig.update_yaxes(title_text='Número', row=1, col=1)
    fig.update_yaxes(title_text='Costo (moneda/hora)', row=2, col=1)

    return fig