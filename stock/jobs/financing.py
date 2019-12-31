# -*- coding: utf-8 -*
#!/usr/bin/python
import sys

sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage


#日排程
#跑所有的股票資料
#執行完scrapy crawl financing
class Financing(object):
    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def execute(self):
        self.open_conn()

        self.cur = self.conn.cursor()
        self.cur.execute("select stock_no,stock_name,round(a.today_borrow_stock/a.today_borrow_money*100,2) percent,data_date,today_borrow_stock,today_borrow_money from financing a where round(a.today_borrow_stock/a.today_borrow_money*100,2) >= 80")
        if self.cur.rowcount > 0:
            rows = self.cur.fetchall()
            token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
            for row in rows:
                msg = "{data_date} Stock No : {stock_no}({stock_name}) 融券餘額{today_borrow_stock},融資餘額{today_borrow_money} 目前資券比{percent}超過80%"
                msg = msg.format(stock_no=row[0], stock_name=row[1], percent=row[2], data_date=row[3], today_borrow_stock=row[4], today_borrow_money=row[5])
                lineNotifyMessage(token, msg)

financing = Financing()
financing.execute()