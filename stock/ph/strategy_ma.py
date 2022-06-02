from fastquant import get_pse_data
import pandas as pd
import datetime
from matplotlib import pyplot as plt
import numpy as np



class php_stock(object):

    def execute(self):

        stock_list = ["GLO", "ALI", "FMETF", "TEL", "ICT", "SCC", "MER", "SHLPH", "UBP", "PSE"]
        #stock_list = ["TEL", "ICT", "SCC", "MER", "SHLPH", "UBP", "PSE"]

        for stock_no in stock_list:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            yesterday = yesterday.strftime('%Y-%m-%d')

            d30ago = datetime.datetime.now() - datetime.timedelta(days=45)
            d30ago = d30ago.strftime('%Y-%m-%d')

            df = get_pse_data(stock_no, d30ago, yesterday)

            continue_3day, close_price = self.continue_3day(df)
            continue_10day, close_price = self.continue_10day(df)

            if continue_3day == True and continue_10day == False:
                msg = "Stock {stock} 5日線站上20日均線，均線向上，昨日價格為{price}".format(stock=stock_no, price=close_price)
                print(msg)
            if continue_3day == False and continue_10day == True:
                msg = "Stock {stock} 5日線跌破20日均線，均線向下，昨日價格為{price}".format(stock=stock_no, price=close_price)
                print(msg)



    def continue_3day(self, df):
        days = 3
        ma20 = df.close.rolling(20).mean().tail(days)
        ma5 = df.close.rolling(5).mean().tail(days)
        close = df.close.tail(1)[0]

        close_ma5_ma20 = pd.concat([ma5, ma20], keys=['MA5', 'MA20'], axis=1)

        result = True
        for x in range(days):
            ma5 = close_ma5_ma20["MA5"][x]
            ma20 = close_ma5_ma20["MA20"][x]
            if close < ma5 or ma5 < ma20:
                result = False

        return result, close

    def continue_10day(self, df):
        days = 10
        ma20 = df.close.rolling(20).mean().tail(days)
        ma5 = df.close.rolling(5).mean().tail(days)
        close = df.close.tail(1)[0]

        close_ma5_ma20 = pd.concat([ma5, ma20], keys=['MA5', 'MA20'], axis=1)

        result = True
        for x in range(days):
            ma5 = close_ma5_ma20["MA5"][x]
            ma20 = close_ma5_ma20["MA20"][x]
            if close < ma5 or ma5 < ma20:
                result = False

        return result, close


stock = php_stock()
stock.execute()




