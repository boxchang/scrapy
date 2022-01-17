# -*- coding: utf-8 -*
import sys
sys.path.append("..")
from funcs.stockflag import stockflag
from funcs.dividend import countAvgDividend, getOffer6YearDividend
from funcs.stockinfo import getTodayPrice

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

    data_date = datetime.date.today().strftime('%Y%m%d')

    #data_date = '20220113'

    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def execute(self):
        self.create_legalpersonDaily_table()
        self.create_legalpersonDate_table()

        #計算旗標日
        self.open_conn()
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT batch_no FROM legalperson_price where batch_no ='"+self.data_date+"'")
        if self.cur.rowcount > 0:
            if self.validate(self.data_date):  #還沒跑過才執行
                self.save_legalperson_date(self.data_date)
                self.count_legalperson_price(self.data_date)
                self.alarm_legalperson_monitor()

    def alarm_legalperson_monitor(self):
        db = database()
        self.conn = db.create_connection()
        sql = "select a.stock_no, c.stock_name,a.in_gap_count,increase,round(b.today_borrow_stock/b.today_borrow_money*100,2) financing,a.updated_date from legalperson_daily a, financing b, stockcode c where (a.in_gap_count > 1.5) " \
              "and b.today_borrow_stock/b.today_borrow_money*100 <20 and a.stock_no = b.stock_no and a.stock_no = c.stock_no"

        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
        for row in rows:
            stock_no = row[0]
            dividend_avg = countAvgDividend(stock_no)
            dividend_years = getOffer6YearDividend(stock_no)
            today_price = getTodayPrice(self.data_date, stock_no)
            dividend_avg_rate = round(dividend_avg/today_price,2)
            updated_date = row[5]


            #紀錄旗標日, 若已存在就不紀錄
            conn = db.create_connection()
            cur = conn.cursor()
            sql = "SELECT data_date FROM stockflag where stock_no ='{stock_no}' and enable is null"
            sql = sql.format(stock_no=stock_no)
            cur.execute(sql)

            if cur.rowcount == 0:
                msg = "【Daily Monitor】觸發旗標日\nStock No :{stock_no}({stock_name})\n累計買超比例超過1.5% : {in_gap_count}%\n連續買超{increase}日\n資券比小於20% : {financing}%\n股息發放年數 : {dividend_years}\n平均股息率 : {dividend_avg_rate}\nUpdated Date: {updated_date}"
                msg = msg.format(stock_no=stock_no, stock_name=row[1].encode('utf-8'), in_gap_count=row[2],
                                 increase=row[3], financing=row[4], dividend_years=dividend_years,
                                 dividend_avg_rate=dividend_avg_rate, updated_date=updated_date)

                lineNotifyMessage(token, msg)

                sf = stockflag()
                sf.saveFlagDate(self.data_date,stock_no)

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


        sql = "select stock_no,percent from legalperson_price where batch_no = '{current_date}' "

        sql = sql.format(current_date=current_date)

        self.cur.execute(sql)

        rows = self.cur.fetchall()

        for row in rows:
            percent = float(row[1])
            if percent > -0.01: #若賣超不多，則不中斷連續計算
                sql = "update legalperson_daily set increase=increase+1, decrease = 0, de_gap_count=0, in_gap_count=IFNULL(in_gap_count,0)+{percent}, updated_date = now() where stock_no = '{stock_no}'"
                sql = sql.format(stock_no=row[0], percent=percent)
                db.execute_sql(sql)
            else:
                sql = "update legalperson_daily set increase=0, in_gap_count=0, decrease = decrease+1, de_gap_count=IFNULL(de_gap_count,0)+{percent}, updated_date = now() where stock_no = '{stock_no}'"
                sql = sql.format(stock_no=row[0], percent=percent)
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

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

legal_daily = LegalPersonDaily()
legal_daily.execute()