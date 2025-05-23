# app.py
from flask import Flask, render_template
import pandas as pd
from DataLoader import DataLoader
from Analisis import (
    plot_combined_panels,
    plot_histogram_density,
    plot_facet_histogram,
    plot_demand_heatmap,
    plot_avg_demand_line,
    plot_bar_avg_total_time,
    plot_stacked_area_daily_counts
)
from Model import load_and_preprocess, build_figure
from flask import request
import plotly.io as pio

app = Flask(__name__)

loader = DataLoader()
tables = loader.list_tables()

if not tables:
    import sys
    app.logger.error("No hay tablas disponibles en la base de datos.")
    sys.exit(1)

table = tables[0]
df = loader.load_table(table).copy()

# Preprocessing
df['FechaDT'] = pd.to_datetime(df['Fecha'], format='%Y%m%d', errors='coerce')
df['InicioEsperaDT'] = pd.to_datetime(df['Hora inicio de espera limpia'], errors='coerce')
df['InicioAtencionDT'] = pd.to_datetime(df['Hora inicio de atencion'], errors='coerce')

df.dropna(subset=['InicioEsperaDT', 'InicioAtencionDT'], inplace=True)

df['TotalTiempo'] = df['Minutos de espera'] + df['Minutos de atencion']
df['DiaSemana'] = df['InicioEsperaDT'].dt.day_name()

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/plots")
def render_all_plots():
    plots = {
        "combined_panels": plot_combined_panels(df, ['Minutos de espera', 'Minutos de atencion', 'TotalTiempo']).to_html(full_html=False),
        "histogram_density": plot_histogram_density(df, 'TotalTiempo', 'Densidad de Tiempo Total').to_html(full_html=False),
        "facet_histogram": plot_facet_histogram(df, 'Minutos de espera', 'DiaSemana', 'Espera por Día de Semana').to_html(full_html=False),
        "heatmap": plot_demand_heatmap(df, 'InicioEsperaDT', 'Sucursal', 'Demanda Promedio por Hora y Sucursal').to_html(full_html=False),
        "avg_demand_line": plot_avg_demand_line(df, 'InicioEsperaDT', 'Sucursal', 'Demanda Promedio por Hora y Sucursal').to_html(full_html=False),
        "bar_avg_total_time": plot_bar_avg_total_time(df).to_html(full_html=False),
        "stacked_area": plot_stacked_area_daily_counts(df).to_html(full_html=False)
    }
    return render_template("plots.html", plots=plots)

@app.route('/proposal', methods=['GET', 'POST'])
def proposal():
    # Valores por defecto
    t_cost_full = 150.0
    t_cost_part = 90.0
    t_capacity = 10

    if request.method == 'POST':
        t_cost_full = float(request.form.get('t_cost_full', 150.0))
        t_cost_part = float(request.form.get('t_cost_part', 90.0))
        t_capacity = int(request.form.get('t_capacity', 10))

    try:
        df_model = load_and_preprocess()
    except ValueError as e:
        return f"<h2>Error en la carga de datos: {str(e)}</h2>", 500

    fig = build_figure(df_model, t_cost_full, t_cost_part, t_capacity)
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('proposal.html',
                           plot_html=plot_html,
                           t_cost_full=t_cost_full,
                           t_cost_part=t_cost_part,
                           t_capacity=t_capacity)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
