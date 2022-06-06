from fugle.restful.fugle_realtime_RESTful_api import *

api_token = '174047bf6e7c04d1006406339ca65eea'

chart = chart_api(api_token=api_token)
quote = quote_api(api_token=api_token)
line = line_notify(api_token=api_token,
                   line_token='zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(children=[

    dcc.Input(id='symbol_input', type='text',
              placeholder='input symbol_id', style={'display': 'inline-block'}),

    daq.BooleanSwitch(
        id='strategy_switch',
        on=False,
        color='lime',
        label='Line Notify Switch',
        style={'width': '10%', 'display': 'inline-block'}
    ),

    dcc.Graph(id='min_K_plot', style={'height': '80vh', 'width': '75%',
                                      'vertical-align': 'top', 'display': 'inline-block'}),

    dcc.Interval(id='interval_60s', interval=60 * 1000),

    html.Div(id='order_book', style={'width': '25%',
                                     'display': 'inline-block', 'vertical-align': 'top'}),

    dcc.Interval(id='interval_5s', interval=5 * 1000)
])


# The "inputs" and "outputs" of our application interface are described declaratively through the app.callback decorator.
@app.callback(
    Output('min_K_plot', 'figure'),
    [
        Input('interval_60s', 'n_intervals'),
        Input('symbol_input', 'value'),
    ]
)
def candlestick_chart(interval_60s, symbol_input):
    df_ohlc = chart.get_chart_data(5, symbol_input)

    return {
        'data': [
            chart.plot_ohlc(df_ohlc, 'red', 'green'),
            chart.plot_MA(df_ohlc, 3, 'black', 3),
            chart.plot_MA(df_ohlc, 5, 'brown', 3),
            chart.plot_MA(df_ohlc, 10, 'blue', 1),
            chart.plot_volume_bar(df_ohlc, 'red', 'green'),
        ],
        'layout': {
            'xaxis': {'rangeslider': {'visible': False}, 'anchor': 'y2'},
            'yaxis': {'domain': [0.4, 1]},
            'yaxis2': {'domain': [0, 0.35]},
            'legend': {'orientation': 'h'},
            'title': symbol_input + ' ' + '分K線圖'
        }
    }


@app.callback(
    Output('order_book', 'children'),
    [
        Input('interval_5s', 'n_intervals'),
        Input('symbol_input', 'value'),
        Input('strategy_switch', 'on'),
    ]
)
def order_book_table(interval2, symbol_input, strategy_switch):
    if strategy_switch == True:
        line.target_price_strategy('2330', 330, 320)
        line.target_change_strategy('2884', 0.01, 0.01)
        line.target_price_strategy('2317', 90, 80)
    else:
        pass

    df_quote, price_list, symbol = quote.update_quote_data(symbol_input)

    return [
        quote.plot_order_book(df_quote, price_list, symbol)
    ]


if __name__ == '__main__':
    app.run_server()