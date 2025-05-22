# model_plots.py

import pandas as pd
import numpy as np
from scipy.optimize import milp, LinearConstraint, Bounds
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from DataLoader import DataLoader
import pulp

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


def optimize_staff(demand, full_shifts, part_shifts, A1=100, A2=80, C=12, nmax=5):
    hours = demand.index.tolist() #Listo
    n_hours = len(hours)

    prob = pulp.LpProblem("StaffSchedulingWithCustomShifts", pulp.LpMinimize)

    # Decision variables
    v1 = {s: pulp.LpVariable(f"{s}", lowBound=0, cat="Integer") for s in full_shifts}
    v2 = {s: pulp.LpVariable(f"{s}", lowBound=0, cat="Integer") for s in part_shifts}
    l = {h: pulp.LpVariable(f"leak_{h}", lowBound=0, cat="Continuous") for h in hours}

    # Objective: Minimize total cost
    prob += (
        pulp.lpSum([A1 * v1[s] for s in full_shifts]) +
        pulp.lpSum([A2 * v2[s] for s in part_shifts])
    ), "TotalCost"

    for t, h in enumerate(hours):
        # Calculate coverage for hour h
        ft_coverage = pulp.lpSum([v1[s] for s, covered in full_shifts.items() if h in covered])
        pt_coverage = pulp.lpSum([v2[s] for s, covered in part_shifts.items() if h in covered])
        total_coverage = ft_coverage + pt_coverage

        # Constraint: max concurrent staff
        prob += total_coverage <= nmax, f"MaxStaff_{h}"

        # Constraint: leakage limited to 30% of demand
        prob += l[h] <= 0.3 * demand[h], f"LeakageLimit_{h}"

        # Flow constraint
        if t == 0:
            prob += l[h] == 0, f"LeakageStart_{h}"
            prob += C * total_coverage + l[h] >= demand[h], f"DemandFlow_{h}"
        else:
            prev_h = hours[t - 1]
            prob += C * total_coverage + l[h] >= demand[h] + l[prev_h], f"DemandFlow_{h}"

        # End leakage should be zero
        if t == len(hours) - 1:
            prob += l[h] == 0, f"LeakageEnd_{h}"

        # Optional: Ensure wait time is controlled
        prob += 10 * C * total_coverage >= demand[h], f"WaitTime_{h}"

    # Solve
    solver = pulp.PULP_CBC_CMD(msg=False)
    result_status = prob.solve(solver)
    status = pulp.LpStatus[prob.status]

    # Extract solution
    if status == 'Optimal':
        full_sol = {k: int(v1[k].varValue or 0) for k in v1}
        part_sol = {k: int(v2[k].varValue or 0) for k in v2}
    else:
        full_sol = {}
        part_sol = {}

    return full_sol, part_sol, None if status == 'Optimal' else status

def build_figure(df, cost_full, cost_part, capacity, max_ventanillas):
    branches = sorted(df['SUCURSAL'].unique())
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        subplot_titles=('Empleados vs Pacientes', 'Costos por Hora', 'Tabla de empleados por hora'),
        row_heights=[0.33, 0.33, 0.34],
        vertical_spacing=0.1,
        specs=[[{}], [{}], [{'type': 'table'}]]
    )

    all_vis = []
    for branch in branches:
        ndf = df[df['SUCURSAL'] == branch]
        daily = ndf.groupby([ndf['FECHA'].dt.date.rename('DIA'), 'HORA']) \
                   .size().reset_index(name='CNT')
 
        avg = daily.groupby('HORA')['CNT'] \
           .mean().reindex(range(6, 20), fill_value=0)

        min_hour = avg.index.min()
        max_hour = avg.index.max() + 1


        full_shifts = {f'FT_{h}': list(range(h, h + 8)) for h in range(min_hour, max_hour-7)}
        part_shifts = {f'PT_{h}': list(range(h, h + 4)) for h in range(min_hour, max_hour-3)}


        full_sol, part_sol, warning_msg  = optimize_staff(
            avg, full_shifts, part_shifts,
            cost_full, cost_part, capacity,
            max_ventanillas
        )
        if full_sol is None or part_sol is None:
            # Mostrar advertencia o agregar una traza vacía para que no falle
            print(warning_msg)
            # Puedes crear series vacías para evitar errores más adelante
            cov_ft = pd.Series(0, index=avg.index)
            cov_pt = pd.Series(0, index=avg.index)
            cost_ft = pd.Series(0.0, index=avg.index)
            cost_pt = pd.Series(0.0, index=avg.index)
        else:
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

        empleados_por_hora = pd.DataFrame({
            'Hora': avg.index,
            'FT Empleados': (cov_ft / capacity).astype(int),
            'PT Empleados': (cov_pt / capacity).astype(int),
            'Total Empleados': ((cov_ft + cov_pt) / capacity).astype(int)
        })

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

        all_vis.extend([True] * 7 if branch == branches[0] else [False] * 7)

        fig.add_trace(
            go.Table(
                header=dict(
                    values=list(empleados_por_hora.columns),
                    fill_color=PALETTE[0],
                    font=dict(color='white'),
                    align='center'
                ),
                cells=dict(
                    values=[empleados_por_hora[col].tolist() for col in empleados_por_hora.columns],
                    fill_color=BG_COLOR,
                    align='center'
                ),
                visible=(branch == branches[0])
            ),
            row=3, col=1
        )


    # Configurar visibilidad inicial
    for i, trace in enumerate(fig.data):
        trace.visible = all_vis[i]

    # Botones para cambiar sucursal
    buttons = []
    for bi, branch in enumerate(branches):
        vis = [False] * len(fig.data)
        for j in range(bi * 7, bi * 7 + 7):
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