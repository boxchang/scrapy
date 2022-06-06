from fugle.restful.fugle_realtime_RESTful_api import *

quote = quote_api(api_token='demo')
quote.update_quote_data(input_symbol='2884')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[

    html.Div(id='order_book', style={'width': '25%', 'display': 'inline-block'}),

    dcc.Interval(id='interval', interval=1 * 1000)
])

@app.callback(
    Output('order_book', 'children'),
    [Input('interval', 'n_intervals')]
)
def order_book_table(interval):
    df_quote, price_list, symbol = quote.update_quote_data(input_symbol='2884')

    return [
        quote.plot_order_book(dataframe=df_quote, price_list=price_list, symbol_id=symbol)
    ]


if __name__ == '__main__':
    app.run_server()