import dash
from dash import dcc, html, Input, Output
from extract import extract_data
from transform import transform_data
from load import load_data
from pyspark.sql import SparkSession
import pandas as pd
from pysus.online_data.SIH import SIH

spark = SparkSession.builder.appName("SIHDashboard").getOrCreate()

app = dash.Dash(__name__)

states = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT",
    "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR",
    "SC", "SP", "SE", "TO"
]
years = list(range(2008, 2025))  # Adjust based on data availability

sih = SIH().load()
group_options = sih.groups  # Dynamically loaded group options

app.layout = html.Div([
    html.H1("SIH ETL Dashboard"),
    dcc.Dropdown(id="group-dropdown", options=[{"label": g, "value": g} for g in group_options], placeholder="Select Group"),
    dcc.Dropdown(id="state-dropdown", options=[{"label": s, "value": s} for s in states], placeholder="Select State"),
    dcc.Dropdown(id="year-dropdown", options=[{"label": y, "value": y} for y in years], placeholder="Select Year"),
    dcc.Graph(id="sih-graph")
])

@app.callback(
    Output("sih-graph", "figure"),
    Input("group-dropdown", "value"),
    Input("state-dropdown", "value"),
    Input("year-dropdown", "value")
)
def update_graph(group, state, year):
    if not group or not state or not year:
        return {"data": [], "layout": {"title": "Please select all filters"}}

    sdf = extract_data(group, state, year)
    result_sdf = transform_data(sdf)
    result_df = result_sdf.toPandas()

    fig = {
        "data": [
            {
                "x": result_df["PROC_REA"],
                "y": result_df["count"],
                "type": "bar"
            }
        ],
        "layout": {"title": f"Procedures in {state} - {year} ({group})"}
    }
    return fig

def view(df):
    app.run(debug=True)
