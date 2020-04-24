# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import datetime
from stock.database import database
import requests
import json

#crawl taiex的改良，爬每日的加權指數，可以指定日期或跑時間區間補資料
class TaiexbyDay(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    # 將數字字串裡的千分符','用''取代，例如123,456轉成123456
    def num_comma_clear(self, arg):
        return str(arg.replace(",", ""))

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def validate(self, data_date):
        cur = self.conn.cursor()
        sql = "SELECT * FROM taiex where data_date = '{data_date}' "
        sql = sql.format(data_date=data_date)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def InsTaiexByDate(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into taiex({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

    def DelTaiexByDate(self, data_date):
        cur = self.conn.cursor()
        sql = "delete from taiex where data_date = '" + data_date + "'"
        cur.execute(sql)

    def GetTaiexByMonth(self, data_date):

        #data_date = '20200422'
        url = 'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date='+data_date
        res = requests.get(url)

        if len(res.text) > 0:
            json_data = json.loads(res.text)

            for j in json_data['data']:
                tmp = str(j[0]).replace(" ", "").split("/")
                date_tmp = str(int(tmp[0]) + 1911) + tmp[1] + tmp[2]

                if self.validate(date_tmp):
                    #self.DelTaiexByDate(data_date)
                    item = {}
                    item['data_date'] = date_tmp  # 日期
                    item['open_index'] = self.num_comma_clear(j[1])  # 開盤指數
                    item['high_index'] = self.num_comma_clear(j[2])  # 最高指數
                    item['low_index'] = self.num_comma_clear(j[3])  # 最低指數
                    item['close_index'] = self.num_comma_clear(j[4])  # 收盤指數
                    self.InsTaiexByDate(item)

            self.conn.commit()


stock_days = {
'201911': ('20191101', '20191130'),
'201910': ('20191001', '20191031'),
'201909': ('20190901', '20190930'),
'201908': ('20190801', '20190831'),
'201907': ('20190701', '20190731'),
'201906': ('20190601', '20190630'),
'201905': ('20190501', '20190531'),
'201904': ('20190401', '20190430'),
'201903': ('20190301', '20190331'),
'201902':('20190201','20190228'),
'201901':('20190101','20190131'),}

taiex = TaiexbyDay()
for stock_day in stock_days.values():
    endDate = stock_day[1]
    taiex.GetTaiexByMonth(endDate)


# if len(sys.argv) == 1:
#     data_date = datetime.date.today().strftime('%Y%m%d')
# else:
#     data_date = sys.argv[1]
#
# taiex = TaiexbyDay()
# taiex.GetTaiexByDate(data_date)