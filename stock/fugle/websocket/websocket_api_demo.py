from fugle.websocket.fugle_realtime_websocket_api import *

api_token = '174047bf6e7c04d1006406339ca65eea'
chart = chart_websocket_api(api_token=api_token)
quote = quote_websocket_api(api_token=api_token)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[

    dcc.Graph(id='min_K_plot', style={'height': '80vh', 'width': '75%',
                                      'vertical-align': 'top', 'display': 'inline-block'}),

    html.Div(id='order_book', style={'width': '25%',
                                     'display': 'inline-block', 'vertical-align': 'top'}),

    dcc.Interval(id='interval1', interval=5 * 1000),
    dcc.Interval(id='interval2', interval=1 * 1000)
])


# The "inputs" and "outputs" of our application interface are described declaratively through the app.callback decorator.
@app.callback(
    Output('min_K_plot', 'figure'),
    [Input('interval1', 'n_intervals')])
def candlestick_chart(interval1):
    df_ohlc = chart.get_chart_data(5, '2884')

    return {
        'data': [
            chart.plot_ohlc(df_ohlc, 'red', 'green'),
            chart.plot_MA(df_ohlc, 5, 'black', 4),
            chart.plot_MA(df_ohlc, 10, 'brown', 2),
            chart.plot_volume_bar(df_ohlc, 'red', 'green'),
        ],
        'layout': {
            'xaxis': {'rangeslider': {'visible': False}, 'anchor': 'y2'},
            'yaxis': {'domain': [0.4, 1]},
            'yaxis2': {'domain': [0, 0.35]},
            'legend': {'orientation': 'h'},
            'title': '分K線圖'
        }
    }


@app.callback(
    Output('order_book', 'children'),
    [Input('interval2', 'n_intervals')])
def order_book_table(interval2):
    df_quote, price_list, symbol = quote.update_quote_data('2884')

    return [
        quote.plot_order_book(df_quote, price_list, symbol)
    ]


if __name__ == '__main__':
    app.run_server()