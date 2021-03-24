# Template Dash UI
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import semtk3

external_stylesheets = []

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

semtk3.set_host("http://vesuvius-test")
semtk3.override_ports(utility_port="5900")
fig = semtk3.select_plot_by_id("demoNodegroupPlotting-JWWtemp")
fig2 = semtk3.select_plot_by_id("demoNodegroupPlotting-JWWtemp2")

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='figure1',
        figure=fig
    ),
    
    dcc.Graph(
        id='figure2',
        figure=fig2
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)