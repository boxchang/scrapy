# -*- coding: utf-8 -*
#!/usr/bin/python
import sys

sys.path.append("..")

# 檢查例外狀況
# select stock_no ,count(*) from stockholder group by stock_no having count(*) <> 17
from stock.database import database
from stock.line import lineNotifyMessage


#週排程
class Public(object):
    CREATE_HOLDERDATE_TABLE = ('create table if not exists stockholder_date(data_date varchar(10), flag varchar(1), '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
    CREATE_LIST_TABLE = ('create table if not exists {}(stock_no varchar(10), '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
    CREATE_SUM_TABLE = ('create table if not exists stockholder_sum(data_date varchar(20), stock_no varchar(10), percent float,'
                         'stock_num bigint, holder_num bigint, created_date TimeStamp DEFAULT CURRENT_TIMESTAMP, stock_num_total bigint)')
    CREATE_SUM_COUNT_TABLE = (
        'create table if not exists stockholder_sum_count(stock_no varchar(10), increase int DEFAULT 0, decrease int DEFAULT 0, '
        ' created_date TimeStamp DEFAULT CURRENT_TIMESTAMP, updated_date TimeStamp, de_gap_count float NULL DEFAULT 0 , in_gap_count float NULL DEFAULT 0,stock_num_gap FLOAT NULL DEFAULT 0) ')

    def execute(self):
        self.open_conn()

        self.cur = self.conn.cursor()
        self.cur.execute("SELECT data_date FROM stockholder limit 1")
        if self.cur.rowcount > 0:
            data_date = self.cur.fetchall()[0][0]

            self.create_sum_table()
            self.create_stockholderdate_table()

            if self.validate(data_date):  # JOB每天都會跑，但只有讀到新資料時才會寫入
                self.stockholder_sum(data_date)  #  進行400張以上的大戶比例統計，抓到另一張表
                self.save_stockholder_date(data_date)  #  加入本次統計的日期
                self.stockholder_sum_count(data_date)  #  進行比對，把上次大戶的比例跟這次比例相比，將結果計算在另一張表




    # 準備好要做累加的Table
    def prepare_stock_list_count(self):
        sql = self.CREATE_SUM_COUNT_TABLE
        db = database()
        db.execute_sql(sql)

        sql = "insert into stockholder_sum_count(stock_no) " \
              " (select stock_no from stockcode a where not exists (select * from stockholder_sum_count b where a.stock_no = b.stock_no))"
        db.execute_sql(sql)

    # 判斷連續上升或下降
    def stockholder_sum_count(self, data_date):
        db = database()
        # 取得股票清單
        self.prepare_stock_list_count()

        current_date = data_date  # 本次日期

        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作

        #取得上一次最後日期
        sql = "select * from stockholder_date where data_date < '{data_date}' order by data_date desc limit 1"
        sql = sql.format(data_date = data_date)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        if rows:
            last_date = rows[0][0]  # 上次日期
            sql = "select c.stock_no,c.percent co,l.percent lo,c.stock_num_total ct,l.stock_num_total lt from " \
                  " (select * from stockholder_sum where data_date = '{current_date}') c, " \
                  " (select * from stockholder_sum where data_date = '{last_date}') l " \
                  " where c.stock_no = l.stock_no "
            sql = sql.format(current_date=current_date, last_date=last_date)

            self.cur.execute(sql)

            rows = self.cur.fetchall()

            for row in rows:
                current_percent = float(row[1])
                last_percent = float(row[2])
                current_total = float(row[3])
                last_total = float(row[4])
                stock_num_gap = current_total - last_total
                if current_percent > last_percent:
                    gap = current_percent - last_percent
                    sql = "update stockholder_sum_count set increase=increase+1, decrease = 0, de_gap_count=0, in_gap_count=IFNULL(in_gap_count,0)+{gap}, stock_num_gap = {stock_num_gap}, updated_date = now() where stock_no = '{stock_no}'"
                    sql = sql.format(stock_no=row[0], gap=gap, stock_num_gap=stock_num_gap)
                    db.execute_sql(sql)
                else:
                    gap = last_percent - current_percent
                    sql = "update stockholder_sum_count set increase=0, in_gap_count=0, decrease = decrease+1, de_gap_count=IFNULL(de_gap_count,0)+{gap}, stock_num_gap = {stock_num_gap}, updated_date = now() where stock_no = '{stock_no}'"
                    sql = sql.format(stock_no=row[0], gap=gap, stock_num_gap=stock_num_gap)
                    db.execute_sql(sql)

        # 完成後更新狀態
        sql = "update stockholder_date set flag = 'Y' where data_date='{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

    def put_into_list(self):
        pass

    def create_list_table(self):
        pass

    def create_stockholderdate_table(self):
        sql = self.CREATE_HOLDERDATE_TABLE
        db = database()
        db.execute_sql(sql)

    def create_sum_table(self):
        sql = self.CREATE_SUM_TABLE
        db = database()
        db.execute_sql(sql)
        print(sql)

    # 將Holder Date資料塞到stockerholder_date
    def save_stockholder_date(self, data_date):
        db = database()
        # 執行前先清除Table，該次的日期去刪除，所以先前的資料都還在
        sql = "delete from stockholder_date where data_date = '{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

        # 將本次整理的資料塞進去
        insert_sql = "insert into stockholder_date(data_date) values('{data_date}')"
        insert_sql = insert_sql.format(data_date=data_date)
        db.execute_sql(insert_sql)

    # 將統計的結果放在sum，將大戶比例計算好塞到stockholder_sum
    def stockholder_sum(self, data_date):
        db = database()
        # 執行前先清除Table，該次的日期去刪除，所以先前的資料都還在
        sql = "delete from stockholder_sum where data_date = str_to_date('{data_date}', '%Y-%m-%d')"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

        # 將本次整理的資料塞進去
        # insert_sql = ('insert into stockholder_sum(data_date,stock_no,percent,stock_num,holder_num) ('
        #               ' select data_date, stock_no,sum(percent), sum(stock_num) , sum(holder_num) '
        #               ' from stockholder a where level > 11 and level < 16 '
        #               ' group by stock_no)')

        insert_sql = 'insert into stockholder_sum(data_date,stock_no,percent,stock_num,holder_num,stock_num_total) (' \
                     'select a.data_date,a.stock_no,percent_sum,stock_num_sum,holder_num_sum, stock_num_total from (' \
                     'select data_date, stock_no,sum(percent) percent_sum, sum(stock_num) stock_num_sum , sum(holder_num) holder_num_sum ' \
                     'from stockholder a where level > 11 and level < 16 group by stock_no) a, ' \
                     '(select data_date, stock_no, stock_num stock_num_total from stockholder a where level = 17) b ' \
                     'where a.stock_no = b.stock_no'


        print(insert_sql)

        db.execute_sql(insert_sql)

    def open_conn(self):
        db = database()
        self.conn = db.create_connection()
    
    def validate(self, data_date):
        db = database()
        conn = db.create_connection()
        cur = conn.cursor()
        sql = "SELECT * FROM stockholder_sum where data_date = '{data_date}' "
        sql = sql.format(data_date = data_date)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

class Robert(Public):
    CONFIG = {
        'owner': 'robert',
        'stock_table': 'robert_stock_list', # 股票清單
        'holder_num' : 200,
        'stock_num': 1000000000,
        'percent': 50
    }

    #還需排除增資或減資所帶來的持股比例波動 , 目前stock_num_gap還沒有值，等有值時，要補上不為0才發送訊息
    def foundout(self):
        db = database()
        self.conn = db.create_connection()
        sql = "select a.stock_no, c.stock_name,b.increase,b.decrease,b.in_gap_count,b.de_gap_count,b.stock_num_gap from robert_stock_list a, stockholder_sum_count b, stockcode c " \
              "where a.stock_no = c.stock_no and a.stock_no = b.stock_no " \
              " and (increase > 2 or decrease > 2) and (in_gap_count>3 or de_gap_count>3) "

        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
        for row in rows:
            if int(row[2]) > 0:
                msg = "Stock No :{stock_no}({stock_name}) Count Gap:{in_gap_count}% 出現連續向上{times}次"
                msg = msg.format(stock_no=row[0], stock_name=row[1], in_gap_count=row[4], times=row[2])
            else:
                msg = "Stock No :{stock_no}({stock_name}) Count Gap:-{de_gap_count}% 出現連續向下{times}次"
                msg = msg.format(stock_no=row[0], stock_name=row[1], de_gap_count=row[5], times=row[3])
            lineNotifyMessage(token, msg)

    def execute(self):
        self.create_list_table()
        self.put_into_list()
        self.foundout()

    def create_list_table(self):
        sql = super(Robert, self).CREATE_LIST_TABLE
        sql = sql.format(self.CONFIG['stock_table'])
        db = database()
        db.execute_sql(sql)

    # 把符合條件的股票放入口袋名單
    # stockholder只會保留當前的資料
    def put_into_list(self):
        db = database()
        self.conn = db.create_connection()

        sql = ('select stock_no from stockholder a where level > 11 and level < 16'
                ' group by stock_no having sum(percent) > {percent} and sum(stock_num) > {stock_num} and sum(holder_num)>{holder_num} and not exists (select * from robert_stock_list where stock_no = a.stock_no)')
        sql = sql.format(stock_num = self.CONFIG['stock_num'], percent = self.CONFIG['percent'], holder_num = self.CONFIG['holder_num'])
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        for row in rows:
            insert_sql = "insert into {stock_table}(stock_no) values('{stock_no}')"
            insert_sql = insert_sql.format(stock_table = self.CONFIG['stock_table'],  stock_no = row[0])
            db.execute_sql(insert_sql)





# 將大戶比例計算好塞到stockholder_sum
public = Public()
public.execute()

# 將設定好的規則放入口袋名單內
robert = Robert()
robert.execute()