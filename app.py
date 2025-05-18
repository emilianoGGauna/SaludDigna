# app.py
from flask import Flask, render_template
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

app = Flask(__name__)

# Instanciar DataLoader y precargar datos con filtrado y preprocesamiento
loader = DataLoader(min_fecha=19800101)
tables = loader.list_tables()
if not tables:
    app.logger.error("No hay tablas disponibles en la base de datos.")
    raise SystemExit(1)

df = loader.load_table(tables[0])

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/plots")
def render_all_plots():
    plots = {
        "combined_panels": plot_combined_panels(
            df, ['Minutos de espera', 'Minutos de atencion', 'TotalTiempo']
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "histogram_density": plot_histogram_density(
            df, 'TotalTiempo', 'Densidad de Tiempo Total'
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "facet_histogram": plot_facet_histogram(
            df, 'Minutos de espera', 'DiaSemana', 'Espera por Día de Semana'
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "heatmap": plot_demand_heatmap(
            df, 'InicioEsperaDT', 'Sucursal', 'Demanda Promedio por Hora y Sucursal'
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "avg_demand_line": plot_avg_demand_line(
            df, 'InicioEsperaDT', 'Sucursal', 'Demanda Promedio por Hora y Sucursal'
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "stacked_area": plot_stacked_area_daily_counts(
            df
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True}),

        "bar_avg_total_time": plot_bar_avg_total_time(
            df
        ).to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
    }
    return render_template("plots.html", plots=plots)

if __name__ == "__main__":
    # Para entorno de desarrollo
    app.run(debug=True, host="0.0.0.0", port=5000)
