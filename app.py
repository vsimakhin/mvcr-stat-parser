import json
import pandas as pd
from dash import Dash, dcc, html, Output, Input
import plotly.graph_objs as go

# Load your data once
with open("./output/parsed_data_raw.json", "r") as file:
    data = json.load(file)

app = Dash(__name__)

countries = sorted(data.keys())

app.layout = html.Div([
    html.H2("Cizinci s povoleným pobytem v ČR"),
    dcc.Dropdown(
        id='country-dropdown',
        options=[{'label': c, 'value': c} for c in countries],
        value=countries[0],
        clearable=False,
        style={'width': '50%'}
    ),
    dcc.Graph(id='migration-chart', style={'height': '600px'}),
    html.Div(id='min-max-text', style={'whiteSpace': 'pre-line', 'padding': '10px', 'fontFamily': 'monospace'})
])

@app.callback(
    Output('migration-chart', 'figure'),
    Output('min-max-text', 'children'),
    Input('country-dropdown', 'value')
)
def update_chart_and_text(selected_country):
    x = []
    total = []
    long = []
    permanent = []
    asyl = []

    for date in sorted(data[selected_country].keys(), key=lambda d: pd.to_datetime(d, format="%m.%Y", errors="coerce")):
        x.append(date)
        total.append(data[selected_country][date].get('total', {}).get('celkem', 0))
        long.append(data[selected_country][date].get('přechodně', {}).get('celkem', 0))
        permanent.append(data[selected_country][date].get('trvale', {}).get('celkem', 0))
        asyl.append(data[selected_country][date].get('dočasná ochrana', {}).get('celkem', 0))

    x_dates = pd.to_datetime(x, format="%m.%Y", errors="coerce")
    df = pd.DataFrame({
        "Date": x_dates,
        "total": total,
        "long": long,
        "permanent": permanent,
        "asyl": asyl
    }).sort_values("Date")

    traces = [
        go.Scatter(x=df["Date"], y=df["total"], mode='lines+markers', name='Total'),
        go.Scatter(x=df["Date"], y=df["long"], mode='lines+markers', name='Long'),
        go.Scatter(x=df["Date"], y=df["permanent"], mode='lines+markers', name='Permanent'),
        go.Scatter(x=df["Date"], y=df["asyl"], mode='lines+markers', name='Asyl'),
    ]

    layout = go.Layout(
        title=f"Cizinci s povoleným pobytem - {selected_country}",
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode='closest',
        height=600
    )

    df['Year'] = df['Date'].dt.year
    lines = []
    for year in sorted(df['Year'].dropna().unique()):
        df_year = df[df['Year'] == year]
        line = (
            f"{year}: "
            f"Total Min={df_year['total'].min()}, Max={df_year['total'].max()} | "
            f"Permanent Min={df_year['permanent'].min()}, Max={df_year['permanent'].max()} | "
            f"Long Min={df_year['long'].min()}, Max={df_year['long'].max()} | "
            f"Asyl Min={df_year['asyl'].min()}, Max={df_year['asyl'].max()}"
        )
        lines.append(line)
    summary_text = "\n".join(lines)

    return {'data': traces, 'layout': layout}, summary_text

if __name__ == '__main__':
    app.run(debug=False)
