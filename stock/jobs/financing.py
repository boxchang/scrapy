# -*- coding: utf-8 -*
#!/usr/bin/python
import sys
import datetime
sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage

if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf8')

#日排程
#跑所有的股票資料
#執行完scrapy crawl financing
class Financing(object):
    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def execute(self):
        data_date = datetime.date.today().strftime('%Y%m%d')
        # data_date = "20191224"
        if self.validate(data_date):
            self.open_conn()
            self.cur = self.conn.cursor()
            self.cur.execute("select stock_no,stock_name,round(a.today_borrow_stock/a.today_borrow_money*100,2) percent,data_date,today_borrow_stock,today_borrow_money from financing a where round(a.today_borrow_stock/a.today_borrow_money*100,2) >= 80")
            if self.cur.rowcount > 0:
                rows = self.cur.fetchall()
                token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
                for row in rows:
                    msg = "{data_date}\nStock No : {stock_no}({stock_name})\n融券餘額{today_borrow_stock},融資餘額{today_borrow_money}\n目前資券比{percent}超過80%"
                    msg = msg.format(stock_no=row[0], stock_name=row[1], percent=row[2], data_date=row[3], today_borrow_stock=row[4], today_borrow_money=row[5])
                    lineNotifyMessage(token, msg)


    def validate(self, data_date):
        db = database()
        conn = db.create_connection()
        cur = conn.cursor()
        sql = "SELECT * FROM taiex where data_date = '{data_date}'"
        sql = sql.format(data_date=data_date)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return True
        else:
            return False

financing = Financing()
financing.execute()