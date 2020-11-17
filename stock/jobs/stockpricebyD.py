# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import datetime
from stock.database import database
import MySQLdb
import requests
import pandas as pd
from io import StringIO
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")


class StockPriceDay(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("---", "0").replace("--", "0")
        except:
            pass
        return result

    def validate(self, data_date, stock_type):
        cur = self.conn.cursor()
        sql = "SELECT * FROM stockprice where batch_no = '{data_date}' and stock_type={stock_type}"
        sql = sql.format(data_date=data_date, stock_type=stock_type)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def InsStockPriceByDate(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into stockprice({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

    def DelStockPriceByDate(self, data_date, stock_type):
        cur = self.conn.cursor()
        sql = "delete from stockprice where batch_no = {data_date} and stock_type={stock_type}"
        sql = sql.format(data_date=data_date, stock_type=stock_type)
        cur.execute(sql)

    def GetStockPriceByPeriod(self, fromDate, toDate):

        for data_date in range(int(fromDate), int(toDate)):
            self.GetStockPriceByDate(str(data_date))

    #上市
    def GetStockPriceByDate(self, data_date):

        #data_date = '20200422'
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='+data_date+'&type=ALL'
        res = requests.get(url)

        self.DelStockPriceByDate(data_date)
        if len(res.text) > 0 and self.validate(data_date, 1):

            df = pd.read_csv(StringIO(res.text.replace("=", "")),
                                           header=["證券代號" in l for l in res.text.split("\n")].index(True)-1)
            for index, row in df.iterrows():
                if len(self.clean(row['證券代號'])) == 4:
                    item = {}
                    item['stock_type'] = 1
                    item['batch_no'] = data_date
                    item['stock_no'] = self.clean(row['證券代號']).zfill(6)
                    item['stock_name'] = self.clean(row['證券名稱'])
                    item['stock_buy'] = self.clean(row['成交股數'])
                    item['stock_num'] = self.clean(row['成交筆數'])
                    item['stock_amount'] = self.clean(row['成交金額'])
                    item['stock_sprice'] = self.clean(row['開盤價'])
                    item['stock_hprice'] = self.clean(row['最高價'])
                    item['stock_lprice'] = self.clean(row['最低價'])
                    item['stock_eprice'] = self.clean(row['收盤價'])
                    item['stock_status'] = self.clean(row['漲跌(+/-)'])
                    item['stock_gap'] = row['漲跌價差']
                    item['stock_last_buy'] = self.clean(row['最後揭示買價'])
                    item['stock_last_bnum'] = self.clean(row['最後揭示買量'])
                    item['stock_last_sell'] = self.clean(row['最後揭示賣價'])
                    item['stock_last_snum'] = self.clean(row['最後揭示賣量'])
                    item['stock_value'] = self.clean(row['本益比'])
                    self.InsStockPriceByDate(item)

            self.conn.commit()

    #上櫃
    def GetStockPriceByDate2(self, data_date):

        url_date = str(int(data_date[0:4]) - 1911) + "/" + str(data_date[4:6]) + "/" + str(data_date[6:8])
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d={data_date}&s=0,asc,0'
        url = url.format(data_date=url_date)
        res = requests.get(url)

        self.DelStockPriceByDate(data_date, 2)
        if len(res.text) > 0 and self.validate(data_date, 2):

            df = pd.read_csv(StringIO(res.text), header=2)
            #df = pd.read_csv(StringIO(res.text.replace("=", "")), header=["代號" in l for l in res.text.split("\n")].index(True)-1)
            for index, row in df.iterrows():
                if len(self.clean(row['代號'])) == 4:
                    item = {}
                    item['stock_type'] = 2
                    item['batch_no'] = data_date
                    item['stock_no'] = self.clean(row['代號']).zfill(6)
                    item['stock_name'] = self.clean(row['名稱'])
                    item['stock_eprice'] = self.clean(row['收盤 '])
                    item['stock_status'] = self.clean(row['漲跌'])
                    item['stock_sprice'] = self.clean(row['開盤 '])
                    item['stock_hprice'] = self.clean(row['最高 '])
                    item['stock_lprice'] = self.clean(row['最低'])
                    item['stock_buy'] = self.clean(row['成交股數  '])
                    item['stock_amount'] = self.clean(row['成交金額(元)'])
                    item['stock_num'] = self.clean(row['成交筆數 '])
                    item['stock_last_buy'] = self.clean(row['最後買價'])
                    item['stock_last_bnum'] = self.clean(row['最後買量(千股)'])
                    item['stock_last_sell'] = self.clean(row['最後賣價'])
                    item['stock_last_snum'] = self.clean(row['最後賣量(千股)'])
                    self.InsStockPriceByDate(item)

            self.conn.commit()


#
# stock_days = {'201901':('20190101','20190131'),
#               '201902':('20190201','20190228'),
#               '201903': ('20190301', '20190331'),
#               '201904': ('20190401', '20190430'),
#               '201905': ('20190501', '20190531'),
#               '201906': ('20190601', '20190630'),
#               '201907': ('20190701', '20190722')}
# for stock_day in stock_days.values():
#     fromDate = stock_day[0]
#     toDate = stock_day[1]
#     sp.GetStockPriceByPeriod(fromDate, toDate)

# stock_days = {'20191216',
# '20190801',
# '20190802',
# '20190805',
# '20190806',
# '20190807',
# '20190808',
# '20190812',
# '20190813',
# '20190814',
# '20190815',
# '20190816',
# '20190819',
# '20190820',
# '20190821',
# '20190822',
# '20190823',
# '20190826',
# '20190827',
# '20190828',
# '20190829',
# '20190830',
# '20190731',
# '20190531',
# '20190430'
# }

# sp = StockPriceDay()
# for stock_day in stock_days:
#     data_date = stock_day
#     sp.GetStockPriceByDate(data_date)

if len(sys.argv) == 1:
    data_date = datetime.date.today().strftime('%Y%m%d')
else:
    data_date = sys.argv[1]

sp = StockPriceDay()
sp.GetStockPriceByDate(data_date)
sp.GetStockPriceByDate2(data_date)


sp.conn_close()
