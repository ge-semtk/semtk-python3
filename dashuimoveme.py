# Template Dash UI
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import semtk3
from dash.dependencies import Input, Output

external_stylesheets = []

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

semtk3.set_host("http://vesuvius-test")
semtk3.override_ports(utility_port="5900")
fig_default = semtk3.select_plot_by_id("demoNodegroupPlotting-JWWtemp")

app.layout = html.Div(children=[
    
    html.H1(children='Template UI...'),
    html.Br(),

    dcc.Graph(
        id='figure1',
        figure=fig_default
    ),
    
    html.Label('Select a nodegroup:'),
    html.Br(),
    dcc.Dropdown(
        id='nodegroup-dropdown',
        options=[
            {'label': 'demoNodegroupPlotting-JWWtemp', 'value': 'demoNodegroupPlotting-JWWtemp'},
            {'label': 'demoNodegroupPlotting-JWWtemp2', 'value': 'demoNodegroupPlotting-JWWtemp2'}
        ],
        value='demoNodegroupPlotting-JWWtemp'
    ),
    
    dcc.Graph(
        id='figure2'
    )
])


@app.callback(
    Output(component_id='figure2', component_property='figure'),
    Input(component_id='nodegroup-dropdown', component_property='value')
)
def update_figure(input_value):
    return semtk3.select_plot_by_id(input_value)
    

if __name__ == '__main__':
    app.run_server(debug=True)