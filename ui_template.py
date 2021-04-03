# Template Dash UI
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import semtk3
from dash.dependencies import Input, Output

external_stylesheets = []  #  (automatically uses css files from /assets directory)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

semtk3.set_host("http://vesuvius-test")
semtk3.override_ports(utility_port="5900")

app.layout = html.Div(children=[
    
    html.H1(children='Template UI'),
    html.Br(),
    
    html.Label('Select a nodegroup:'),
    html.Br(),
    dcc.Dropdown(
        id='nodegroup-dropdown',
        options=[
            {'label': 'demoNodegroupPlotting-JWW1', 'value': 'demoNodegroupPlotting-JWW1'},
            {'label': 'demoNodegroupPlotting-JWW2', 'value': 'demoNodegroupPlotting-JWW2'},
        ],
        value='demoNodegroupPlotting-JWW1',
        style={'width': '50%'}
    ),
    
    html.Label('Select a plot:'),
    html.Br(),
    dcc.Dropdown(
        id='plot-dropdown',
        style={'width': '50%'}
    ),
    
    html.Br(),
    dcc.Graph(
        id='figure2',
        style={'width': '40%'}
    )
])


@app.callback(
    Output(component_id='plot-dropdown', component_property='options'),
    Input(component_id='nodegroup-dropdown', component_property='value')
)
def update_plot_list(nodegroup_id):
    names = semtk3.get_plot_spec_names_by_id(nodegroup_id)
    return [{'label': i, 'value': i} for i in names]
  
    
@app.callback(
    Output(component_id='figure2', component_property='figure'),
    Input(component_id='nodegroup-dropdown', component_property='value'),
    Input(component_id='plot-dropdown', component_property='value')
)
def update_figure(nodegroup_id, plot_name):
    return semtk3.select_plot_by_id(nodegroup_id, plot_name)    

if __name__ == '__main__':
    app.run_server(debug=True)