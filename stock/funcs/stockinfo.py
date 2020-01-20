# -*- coding: utf-8 -*
from stock.database import database

#  今日收盤價
def getTodayPrice(data_date, stock_no):
    sql = "select stock_eprice from stockprice where stock_no = '{stock_no}' and batch_no = '{data_date}'"
    sql = sql.format(data_date=data_date, stock_no=stock_no)
    db = database()
    conn = db.create_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    price = 0

    if cur.rowcount > 0:
        if rows[0][0] > 0:
            price = rows[0][0]

    return price