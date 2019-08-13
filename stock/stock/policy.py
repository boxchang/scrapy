from stock.database import database


# 檢查例如狀況
# select stock_no ,count(*) from stockholder group by stock_no having count(*) <> 17

class Policy(object):
    CREATE_LIST_TABLE = ('create table if not exists {}(owner varchar(10), stock_no varchar(10), '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
    CREATE_SUM_TABLE = ('create table if not exists stockholder_sum(data_date varchar(20), stock_no varchar(10), percent float,'
                         'stock_num bigint, holder_num bigint, owner varchar(10), created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        pass

    def put_into_list(self):
        pass

    def create_list_table(self):
        pass

    def stockholder_sum(self):
        pass

    def open_conn(self):
        db = database()
        self.conn = db.create_connection()


class Robert(Policy):
    CONFIG = {
        'owner': 'robert',
        'stock_table': 'robert_stock_list', # 股票清單
        'holder_num' : 100,
        'stock_num': 1000000000,
        'percent': 50
    }

    def execute(self):
        super().open_conn()

        self.cur = self.conn.cursor()
        self.cur.execute("SELECT data_date FROM stockholder limit 1")
        data_date = self.cur.fetchall()[0][0]

        if robert.validate(data_date):
            self.create_list_table()
            self.create_sum_table()
            self.put_into_list()
            self.stockholder_sum(data_date)

    # 將統計的結果放在sum
    def stockholder_sum(self, data_date):
        db = database()
        # 執行前先清除Table
        sql = "delete from stockholder_sum where owner = '{owner}' and data_date = str_to_date('{data_date}', '%Y-%m-%d')"
        sql = sql.format(owner = self.CONFIG['owner'], data_date = data_date)
        db.execute_sql(sql)


        insert_sql = ('insert into stockholder_sum(data_date,stock_no,percent,stock_num,holder_num,owner) ('
                      ' select data_date, stock_no,sum(percent), sum(stock_num) , sum(holder_num), \'{owner}\''
                      ' from stockholder a where level > 11 and level < 16'
                      ' group by stock_no having sum(percent) > {percent} and sum(stock_num) > {stock_num} and sum(holder_num)>{holder_num})')
        insert_sql = insert_sql.format(stock_num=self.CONFIG['stock_num'], percent=self.CONFIG['percent'], holder_num=self.CONFIG['holder_num'], owner = self.CONFIG['owner'])
        print(insert_sql)

        db.execute_sql(insert_sql)


    # 把符合條件的股票放入口袋名單
    def put_into_list(self):
        sql = ('select data_date, stock_no,sum(percent), sum(stock_num) , sum(holder_num) from stockholder a where level > 11 and level < 16'
                ' group by stock_no having sum(percent) > {percent} and sum(stock_num) > {stock_num} and sum(holder_num)>{holder_num} and not exists (select * from robert_stock_list where stock_no = a.stock_no)')
        sql = sql.format(stock_num = self.CONFIG['stock_num'], percent = self.CONFIG['percent'], holder_num = self.CONFIG['holder_num'])
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        db = database()
        for row in rows:
            insert_sql = "insert into {stock_table}(owner, stock_no) values('{owner}','{stock_no}')"
            insert_sql = insert_sql.format(stock_table = self.CONFIG['stock_table'], owner = self.CONFIG['owner'], stock_no = row[0])
            db.execute_sql(insert_sql)

    def create_list_table(self):
        sql = super().CREATE_LIST_TABLE
        sql = sql.format(self.CONFIG['stock_table'])
        db = database()
        db.execute_sql(sql)

    def create_sum_table(self):
        sql = super().CREATE_SUM_TABLE
        db = database()
        db.execute_sql(sql)



    def validate(self, data_date):
        self.cur = self.conn.cursor()
        sql = "SELECT * FROM stockholder_sum where data_date = str_to_date('{data_date}', '%Y-%m-%d') and owner = '{owner}'"
        sql = sql.format(data_date = data_date, owner = self.CONFIG['owner'])
        self.cur.execute(sql)

        rows = self.cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True




robert = Robert()
robert.execute()