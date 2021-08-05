# -*- coding: utf-8 -*
import sys
sys.path.append("..")
from jobs.app_lib.strategy import DynamicStrategy, StaticStrategy
from funcs.stockflag import stockflag
import datetime
from stock.database import database
import MySQLdb
from stock.line import lineNotifyMessage

if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf-8')

class MaMonitorDaily(object):
    today = ""
    token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"

    def conn_close(self):
        self.conn.close()

    def __init__(self, data_date):
        db = database()
        self.conn = db.create_connection()
        self.today = data_date

    def execute(self):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = """
                select * from (
                select f.stock_industry,a.stock_no,stock_name,a.avg_price ma5,b.avg_price ma10,c.avg_price ma20,d.avg_price ma60,
                (b.avg_price/a.avg_price)-1 gap1, (c.avg_price/a.avg_price)-1 gap2, (d.avg_price/a.avg_price)-1 gap3
                from stockprice_ma5 a,stockprice_ma10 b,stockprice_ma20 c,stockprice_ma60 d,robert_stock_list e, stockcode f 
                where a.stock_no = b.stock_no and a.stock_no = c.stock_no and a.stock_no = e.stock_no and a.stock_no = f.stock_no
                and a.stock_no = d.stock_no) aa,
                (select (today_borrow_money*1000/stock_num_total)*400 xx,a.stock_no from financing a,(
                select stock_no,max(stock_num) stock_num_total from stockholder where level = 17 group by stock_no ) b
                where a.stock_no = b.stock_no and (today_borrow_money*1000/stock_num_total)*100 <10) bb,
                (select * from stockprice a where a.batch_no = '{data_date}') cc,
                (select * from legalperson_daily) dd
                where gap1 > -0.01 and gap1 < 0.01
                and gap2 > -0.01 and gap2 < 0.01
                and gap3 > -0.01 and gap3 < 0.01
                and aa.stock_no = bb.stock_no and aa.stock_no = cc.stock_no and cc.stock_buy > 500000
                and dd.stock_no = aa.stock_no and in_gap_count > 0.4
                """
        sql = sql.format(data_date=self.today)
        cur.execute(sql)
        rows = cur.fetchall()

        msg = "均線糾結，單日成交量大於500張，融資比小於10%，外資連續買進\n"
        for row in rows:
            print(row)
            temp = "{stock_name}({stock_no}) 今日股價{stock_price} 融資比{borrow}"
            temp = temp.format(stock_name=row["stock_name"], stock_no=row["stock_no"], stock_price=row["stock_eprice"], borrow=round(row["xx"],2))
            msg += temp
            lineNotifyMessage(self.token, msg)




if len(sys.argv) > 1:
    data_date = sys.argv[1]
else:
    data_date = datetime.date.today().strftime('%Y%m%d')

ma = MaMonitorDaily(data_date)
ma.execute()