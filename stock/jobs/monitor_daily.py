# -*- coding: utf-8 -*
#!/usr/bin/python
import sys
import datetime

sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage

# 每日執行檢查排程
class Monitor(object):
    def execute(self):
        bResult = False
        data_date = datetime.date.today().strftime('%Y%m%d')
        #data_date = "20191224"

        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()

        token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"

        msg = "【Daily Monitor】"
        if self.validate(data_date):
            self.cur.execute("SELECT data_date FROM financing where data_date ='" + data_date + "'")
            if self.cur.rowcount == 0:
                msg_tmp = "\nfinancing{data_date}資料，只會影響每日融資融券異常警訊不會發出"
                msg += msg_tmp.format(data_date=data_date)
                bResult = True


            self.cur.execute("SELECT data_date FROM legalperson where data_date ='" + data_date + "'")
            if self.cur.rowcount == 0:
                msg_tmp = "\nlegalperson無{data_date}資料，影響legalperson_price也會沒資料"
                msg += msg_tmp.format(data_date=data_date)
                bResult = True


            self.cur.execute("SELECT batch_no FROM stockprice where batch_no ='" + data_date + "'")
            if self.cur.rowcount == 0:
                msg_tmp = "\nstockprice無{data_date}資料，影響legalperson_price也會沒資料"
                msg += msg_tmp.format(data_date=data_date)
                bResult = True


            self.cur.execute("SELECT batch_no FROM legalperson_price where batch_no ='" + data_date + "'")
            if self.cur.rowcount == 0:
                msg_tmp = "\nlegalperson_price無{data_date}資料，影響legalperson_daily也會沒資料"
                msg += msg_tmp.format(data_date = data_date)
                bResult = True

            if bResult:
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

monitor = Monitor()
monitor.execute()