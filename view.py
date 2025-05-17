from dash import Dash, dcc, html, Input, Output
from scipy.stats import ttest_1samp
import plotly.express as px
import dash_bootstrap_components as dbc

app = Dash(__name__, external_stylesheets=[dbc.themes.COSMO])

def view(df):
    @app.callback(
        Output("graph", "figure"),
        Input("sex_checklist", "value"),
        Input("age_checklist", "value"),
        Input("dropdown", "value"))
    def update_line_chart(sex_options, age_options, graph_option):
        mask = df.SEX.isin(sex_options)
        mask_2 = df.AGE.isin(age_options)
        df_aux = df[mask]
        df_copy = df_aux[mask_2]
        mean = round(df_copy[graph_option].mean(), 2)
        std = round(df_copy[graph_option].std(), 2)
        t_test = ttest_1samp(df_copy[graph_option], 5)
        padding = df_copy[graph_option].max() * 0.1

        fig = px.line(df_copy, y=graph_option, symbol='NAME', color='NAME')
        fig.add_hline(mean, annotation_text="Mean ("+str(mean)+")",
                      annotation_position="bottom right")
        fig.add_annotation(text="T-Statistic ("+str(round(t_test.statistic, 2))+")",
                           showarrow=False, x=0.5, y=df_copy[graph_option].min()-(padding*0.75))
        fig.add_annotation(text="P Value ("+str(t_test.pvalue)+")",
                           showarrow=False, x=6.5, y=df_copy[graph_option].min()-(padding*0.75))
        fig.add_hrect(y0=mean+std, y1=mean-std, line_width=0, fillcolor="red", opacity=0.2,
                      annotation_text="STD ("+str(std)+")", annotation_position="top right")
        fig.update_yaxes(range=[df_copy[graph_option].min()-padding, df_copy[graph_option].max()+padding])
        return fig

    app.layout = html.Div([
        html.H1('Microgravity Simulation'),
        html.H4('Filters'),
        html.Div([
            html.Div([
                html.Strong('Sex'),
                dcc.Checklist(
                    id="sex_checklist",
                    options=sorted(df['SEX'].unique()),
                    value=sorted(df['SEX'].unique())
                ),
            ], className='sex'),
            html.Div([
                html.Strong('Age'),
                dcc.Checklist(
                    id="age_checklist",
                    options=df['AGE'].unique(),
                    value=df['AGE'].unique()
                ),
            ], className='age')
        ]),
        html.H4('Graphs'),
        dcc.Dropdown(id='dropdown', options=[
            {'label': 'Z0 (Basic Thoracic Impedance)', 'value': 'Z0'},
            {'label': 'MABP (Mean Arterial Blood Pressure)', 'value': 'MABP'},
            {'label': 'PS (Systolic Blood Pressure)', 'value': 'PS'},
            {'label': 'PD (Diastolic Blood Pressure)', 'value': 'PD'},
            {'label': 'HR (Heart Rate)', 'value': 'HR'},
            {'label': 'RPP (Peripherical Al Resistance)', 'value': 'RPP'},
            {'label': 'SV (Stroke Volume)', 'value': 'SV'},
            {'label': 'SVI (Stroke Volume Index)', 'value': 'SVI'},
            {'label': 'CO (Cardiac Output)', 'value': 'CO'},
            {'label': 'CI (Cardiac Index)', 'value': 'CI'},
            {'label': 'SVR (Systemic Vascular Resistance)', 'value': 'SVR'},
            {'label': 'SVRI (Systemic Vascular Resistance Index)',
             'value': 'SVRI'},
            {'label': 'LVSERI', 'value': 'LVSERI'},
            {'label': 'S', 'value': 'S'},
            {'label': 'P/V (Pressure/Volume)', 'value': 'P/V'},
            {'label': 'VET', 'value': 'VET'},
            {'label': 'VETI', 'value': 'VETI'},
            {'label': 'PEP (Preejection Period)', 'value': 'PEP'},
            {'label': 'PEPI (Preejection Period Index)', 'value': 'PEPI'},
            {'label': 'QS2 (Time interval between the Q wave of the ECG and the closure of the aortic valve)', 'value': 'QS2'},
            {'label': 'QS2I (Index of the time interval between the Q wave of the ECG and the closure of the aortic valve)', 'value': 'QS2I'},
            {'label': 'A2/A1', 'value': 'A2/A1'},
            {'label': 'D/S', 'value': 'DS'},
            {'label': 'RZ (Time (in ms) between the ECG R peak and the dZ/dt Z peak)', 'value': 'RZ'},
            {'label': 'RR (Standard deviation of RR intervals)',
             'value': 'RR'},
            {'label': 'SDRR', 'value': 'SDRR'},
            {'label': 'SDDRR', 'value': 'SDDRR'},
            {'label': 'ACI (Acute Cardiac Ischemia)', 'value': 'ACI'},
            {'label': 'Heath', 'value': 'HEATH'},
        ],
            placeholder='Clique aqui para selecionar here to select a variable'
        ),
        dcc.Graph(id="graph")
    ])

    app.run_server(debug=True)