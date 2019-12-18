# -*- coding: utf-8 -*
#!/usr/bin/python
import sys

sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage

class Financing(object):
    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def execute(self):
        self.open_conn()

        self.cur = self.conn.cursor()
        self.cur.execute("select stock_no,stock_name,round(a.today_borrow_stock/a.today_borrow_money,2)*100 percent from financing a where round(a.today_borrow_stock/a.today_borrow_money,2)*100 >= 80")
        if self.cur.rowcount > 0:
            rows = self.cur.fetchall()
            token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
            for row in rows:
                msg = "Stock No :{stock_no}({stock_name}) 目前資券比{percent}超過80%"
                msg = msg.format(stock_no=row[0], stock_name=row[1], percent=row[2])
                lineNotifyMessage(token, msg)

financing = Financing()
financing.execute()