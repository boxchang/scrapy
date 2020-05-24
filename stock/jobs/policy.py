# encoding=utf8
#!/usr/bin/python
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("..")

# check exception
# select stock_no ,count(*) from stockholder group by stock_no having count(*) <> 17
from stock.database import database
from stock.line import lineNotifyMessage


#weekly job
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
        result = False
        self.open_conn()

        self.cur = self.conn.cursor()
        self.cur.execute("SELECT data_date FROM stockholder limit 1")
        if self.cur.rowcount > 0:
            data_date = self.cur.fetchall()[0][0]

            self.create_sum_table()
            self.create_stockholderdate_table()

            if self.validate(data_date):  # update only have new data
                self.stockholder_sum(data_date)  #  count over 400 of legalperson insert to the other table
                self.save_stockholder_date(data_date)  # insert this time data date
                self.stockholder_sum_count(data_date)  # compare this time and last time gap，record the result to the other table
                result = True

        return result




    # prepare count table
    def prepare_stock_list_count(self):
        sql = self.CREATE_SUM_COUNT_TABLE
        db = database()
        db.execute_sql(sql)

        sql = "insert into stockholder_sum_count(stock_no) " \
              " (select stock_no from stockcode a where not exists (select * from stockholder_sum_count b where a.stock_no = b.stock_no))"
        db.execute_sql(sql)

    # judge continuely up or down
    def stockholder_sum_count(self, data_date):
        db = database()
        # get stock list
        self.prepare_stock_list_count()

        current_date = data_date  # get this time date

        self.conn = db.create_connection()
        self.cur = self.conn.cursor()

        #get last date
        sql = "select * from stockholder_date where data_date < '{data_date}' order by data_date desc limit 1"
        sql = sql.format(data_date = data_date)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        if rows:
            last_date = rows[0][0]  # last data date
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
                current_total = 0
                if row[3] is not None:
                    current_total = float(row[3])

                last_total = 0
                if row[4] is not None:
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

        # update status when finish
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

    # Holder Date insert to stockerholder_date
    def save_stockholder_date(self, data_date):
        db = database()
        # clear table before execute
        sql = "delete from stockholder_date where data_date = '{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

        # put this time result to table
        insert_sql = "insert into stockholder_date(data_date) values('{data_date}')"
        insert_sql = insert_sql.format(data_date=data_date)
        db.execute_sql(insert_sql)

    # count the result and insert to stockholder_sum
    def stockholder_sum(self, data_date):
        db = database()
        # clear table before execute
        sql = "delete from stockholder_sum where data_date = str_to_date('{data_date}', '%Y-%m-%d')"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)

        # insert_sql = ('insert into stockholder_sum(data_date,stock_no,percent,stock_num,holder_num) ('
        #               ' select data_date, stock_no,sum(percent), sum(stock_num) , sum(holder_num) '
        #               ' from stockholder a where level > 11 and level < 16 '
        #               ' group by stock_no)')

        insert_sql = 'insert into stockholder_sum(data_date,stock_no,percent,stock_num,holder_num,stock_num_total) (' \
                     'select a.data_date,a.stock_no,percent_sum,stock_num_sum,holder_num_sum, stock_num_total from (' \
                     'select data_date, stock_no,sum(percent) percent_sum, sum(stock_num) stock_num_sum , sum(holder_num) holder_num_sum ' \
                     'from stockholder a where level > 11 and level < 16 group by stock_no) a, ' \
                     '(select data_date, stock_no, stock_num stock_num_total from stockholder a where level = 17) b ' \
                     'where a.stock_no = b.stock_no)'


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
        'stock_table': 'robert_stock_list', # stock list
        'holder_num' : 200,
        'stock_num': 1000000000,
        'percent': 50
    }

    def foundout(self):
        db = database()
        self.conn = db.create_connection()
        sql = "select a.stock_no, c.stock_name,b.increase,b.decrease,b.in_gap_count,b.de_gap_count,b.stock_num_gap,b.updated_date from robert_stock_list a, stockholder_sum_count b, stockcode c " \
              "where a.stock_no = c.stock_no and a.stock_no = b.stock_no " \
              " and increase > 2 and in_gap_count>2 " \
              " and a.stock_no not in (select stock_no from (select stock_no,count(*) from stockholder e where level = 17 group by stock_no having count(*) > 1) a)"
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
        for row in rows:
            stock_no = row[0]
            stock_name = row[1]
            in_gap_count = row[4]
            de_gap_count = row[5]
            stock_num_gap = row[6]
            updated_date = row[7]
            if int(row[2]) > 0:
                times = row[2]
                msg = "Stock No :{stock_no}({stock_name})\nCount Gap:{in_gap_count}%\nrise continuously {times} weeks\nStock Amount Changed Gap : {stock_num_gap}\nUpdated Date : {updated_date}"
                msg = msg.format(stock_no=stock_no, stock_name=stock_name, in_gap_count=in_gap_count, times=times, stock_num_gap=stock_num_gap, updated_date=updated_date)

                self.update_stock_flag(stock_no, in_gap_count)  #use percent of legalholder to calculate the up and down of stock
            else:
                times = row[3]
                msg = "Stock No :{stock_no}({stock_name})\nCount Gap:-{de_gap_count}%\nfall continuously {times} weeks\nStock Amount Changed Gap : {stock_num_gap}\nUpdated Date : {updated_date}"
                msg = msg.format(stock_no=row[0], stock_name=row[1], de_gap_count=de_gap_count, times=times, stock_num_gap=stock_num_gap, updated_date=updated_date)

                self.close_stock_flag(stock_no)   #close flag date
            lineNotifyMessage(token, msg)


    def update_stock_flag(self,stock_no,in_gap_count):
        times = 0
        if in_gap_count <=1 :
            times = 15
        elif in_gap_count > 1 and in_gap_count <= 3:
            times = 9
        elif in_gap_count > 3 and in_gap_count <= 5:
            times = 8
        elif in_gap_count > 5 and in_gap_count <= 10:
            times = 6
        elif in_gap_count > 10 and in_gap_count <= 20:
            times = 5
        elif in_gap_count > 20:
            times = 4


        percent = in_gap_count * times
        percent50 = percent+5
        percent80 = percent-5
        percent90 = percent-10

        db = database()
        conn = db.create_connection()
        cur = conn.cursor()
        sql = "SELECT stock_lprice FROM stockflag where stock_no ='{stock_no}' and enable is null"
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)

        if cur.rowcount > 0:
            rows = cur.fetchall()
            for row in rows:
                stock_lprice = row[0]
                percent_price70 = round(stock_lprice * (1 + (percent / 100)), 1)
                percent_price50 = round(stock_lprice * (1 + (percent50 / 100)), 1)
                percent_price80 = round(stock_lprice * (1 + (percent80 / 100)), 1)
                percent_price90 = round(stock_lprice * (1 + (percent90 / 100)), 1)

                sql = "update stockflag set price90 ={percent_price90},price80={percent_price80},price70={percent_price70},price50={percent_price50} where stock_no ='{stock_no}' and enable is null"
                sql = sql.format(percent_price90=percent_price90,percent_price80=percent_price80,percent_price70=percent_price70,percent_price50=percent_price50,stock_no=stock_no)
                db.execute_sql(sql)

    def close_stock_flag(self,stock_no):
        db = database()
        sql = "update stockflag set enable = 'N' where stock_no = '{stock_no}' and enable is null"
        sql = sql.format(stock_no=stock_no)
        db.execute_sql(sql)

    def execute(self):
        self.create_list_table()
        self.put_into_list()
        self.foundout()

    def create_list_table(self):
        sql = super(Robert, self).CREATE_LIST_TABLE
        sql = sql.format(self.CONFIG['stock_table'])
        db = database()
        db.execute_sql(sql)

    # match condition stock put into the list
    # stockholder only keep the newest data
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





# count into stockholder_sum
public = Public()
if public.execute():
    # filter by condition to get stock
    robert = Robert()
    robert.execute()