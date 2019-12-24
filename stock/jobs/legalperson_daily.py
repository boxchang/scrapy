# -*- coding: utf-8 -*
import sys

sys.path.append("..")
import datetime
from stock.database import database
from stock.line import lineNotifyMessage

#日排程
#在跑完financing 及 legalperson_price之後才跑
class LegalPersonDaily(object):
    LEGALPERSON_DAILY_TABLE = ('CREATE TABLE if not exists legalperson_daily (stock_no varchar(10) NOT NULL, '
                               'increase int DEFAULT 0, decrease int DEFAULT 0, created_date TimeStamp DEFAULT CURRENT_TIMESTAMP, updated_date TimeStamp, de_gap_count float NULL DEFAULT 0 , in_gap_count float NULL DEFAULT 0)')

    CREATE_LEGALPERSON_DATE_TABLE = ('create table if not exists legalperson_date(data_date varchar(10), flag varchar(1), '
                               'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def execute(self):
        self.create_legalpersonDaily_table()
        self.create_legalpersonDate_table()

        data_date = datetime.date.today().strftime('%Y%m%d')
        #data_date = '20191223'

        self.open_conn()
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT batch_no FROM legalperson_price where batch_no ='"+data_date+"'")
        if self.cur.rowcount > 0:
            if self.validate(data_date):  #還沒跑過才執行
                self.save_legalperson_date(data_date)
                self.count_legalperson_price(data_date)
            self.alarm_legalperson_monitor()

    def alarm_legalperson_monitor(self):
        db = database()
        self.conn = db.create_connection()
        sql = "select a.stock_no, c.stock_name,a.in_gap_count,increase,round(b.today_borrow_stock/b.today_borrow_money*100,2) financing from legalperson_daily a, financing b, stockcode c where (a.in_gap_count > 1.5) " \
              "and b.today_borrow_stock/b.today_borrow_money*100 <10 and a.stock_no = b.stock_no and a.stock_no = c.stock_no"

        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
        for row in rows:
            msg = "【Daily Monitor】\nStock No :{stock_no}({stock_name})\n當日買超比例超過1.5% : {in_gap_count}%\n連續買超{increase}日\n資券比小於10% : {financing}%"
            msg = msg.format(stock_no=row[0], stock_name=row[1], in_gap_count=row[2], increase=row[3], financing=row[4])

            lineNotifyMessage(token, msg)

    def create_legalpersonDate_table(self):
        sql = self.CREATE_LEGALPERSON_DATE_TABLE
        db = database()
        db.execute_sql(sql)

    def create_legalpersonDaily_table(self):
        sql = self.LEGALPERSON_DAILY_TABLE
        db = database()
        db.execute_sql(sql)

    # 準備好要做累加的資料
    def prepare_count_data(self):
        db = database()
        sql = "insert into legalperson_daily(stock_no) " \
              " (select stock_no from stockcode a where not exists (select * from legalperson_daily b where a.stock_no = b.stock_no))"
        db.execute_sql(sql)


    # 判斷連續上升或下降
    def count_legalperson_price(self, data_date):
        db = database()
        # 準備好要做累加的資料
        self.prepare_count_data()

        current_date = data_date  # 本次日期

        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作

        # 取得上一次最後日期
        sql = "select * from legalperson_date where data_date < '{data_date}' order by data_date desc limit 1"
        sql = sql.format(data_date=data_date)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        if rows:
            last_date = rows[0][0]  # 上次日期
            sql = "select c.stock_no,c.percent co,l.percent lo from " \
                  " (select * from legalperson_price where batch_no = '{current_date}') c, " \
                  " (select * from legalperson_price where batch_no = '{last_date}') l " \
                  " where c.stock_no = l.stock_no "
            sql = sql.format(current_date=current_date, last_date=last_date)

            self.cur.execute(sql)

            rows = self.cur.fetchall()

            for row in rows:
                current_percent = float(row[1])
                last_percent = float(row[2])
                if current_percent > last_percent:
                    gap = current_percent - last_percent
                    sql = "update legalperson_daily set increase=increase+1, decrease = 0, de_gap_count=0, in_gap_count=IFNULL(in_gap_count,0)+{gap}, updated_date = now() where stock_no = '{stock_no}'"
                    sql = sql.format(stock_no=row[0], gap=gap)
                    db.execute_sql(sql)
                else:
                    gap = last_percent - current_percent
                    sql = "update legalperson_daily set increase=0, in_gap_count=0, decrease = decrease+1, de_gap_count=IFNULL(de_gap_count,0)+{gap}, updated_date = now() where stock_no = '{stock_no}'"
                    sql = sql.format(stock_no=row[0], gap=gap)
                    db.execute_sql(sql)

        # 完成後更新狀態
        sql = "update legalperson_date set flag = 'Y' where data_date='{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

    # 將Holder Date資料塞到stockerholder_date
    def save_legalperson_date(self, data_date):
        db = database()
        # 執行前先清除Table，該次的日期去刪除，所以先前的資料都還在
        sql = "delete from legalperson_date where data_date = '{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

        # 將本次整理的資料塞進去
        insert_sql = "insert into legalperson_date(data_date) values('{data_date}')"
        insert_sql = insert_sql.format(data_date=data_date)
        db.execute_sql(insert_sql)

    def validate(self, data_date):
        db = database()
        conn = db.create_connection()
        cur = conn.cursor()
        sql = "SELECT * FROM legalperson_date where data_date = '{data_date}' and flag ='Y'"
        sql = sql.format(data_date=data_date)
        cur.execute(sql)

        rows = self.cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

legal_daily = LegalPersonDaily()
legal_daily.execute()