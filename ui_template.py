# Template Dash UI
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import semtk3
from dash.dependencies import Input, Output

external_stylesheets = []  #  (automatically uses css files from /assets directory)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

semtk3.set_host("http://vesuvius-test")
semtk3.override_ports(utility_port="5900")

app.layout = html.Div(children=[
    
    html.H1(children='Template UI'),
    html.H4(children='Use this as a starting point for a custom web app!'),
    
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
    html.Br(),
        
    html.Label('Select a plot:'),
    html.Br(),
    dcc.Dropdown(
        id='plot-dropdown',
        style={'width': '50%'}
    ),
    
    html.Br(),
    dcc.Graph(
        id='figure',
        style={'width': '40%'}
    )
])


# if user selects nodegroup, update the plot dropdown
@app.callback(
    Output(component_id='plot-dropdown', component_property='options'),
    Input(component_id='nodegroup-dropdown', component_property='value')
)
def update_plot_dropdown(nodegroup_id):
    names = semtk3.get_plot_spec_names_by_id(nodegroup_id)
    return [{'label': i, 'value': i} for i in names]
  
  
# if user selects plot, update the graph    
@app.callback(
    Output(component_id='figure', component_property='figure'),
    Input(component_id='plot-dropdown', component_property='value'),
    state=[Input(component_id='nodegroup-dropdown', component_property='value')]
)
def update_figure(plot_name, nodegroup_id):
    #print("update_figure: nodegroup",nodegroup_id,"plot_name '",plot_name,"'")
    if plot_name is not None:
        return semtk3.select_plot_by_id(nodegroup_id, plot_name)
    else:
        return go.Figure()  # blank figure
        

if __name__ == '__main__':
    app.run_server(debug=True)