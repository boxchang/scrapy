# -*- coding: utf-8 -*
from stock.database import database


#計算平均殖息率
def countAvgDividend(stock_no):
    sql = "select round(avg(total),2) from dividend where stock_no = '{stock_no}'"
    sql = sql.format(stock_no=stock_no)
    db = database()
    conn = db.create_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    dividend_avg = 0

    if cur.rowcount > 0:
        if rows[0][0] > 0:
            dividend_avg = rows[0][0]

    return dividend_avg

#連續發放股利年數
def getOffer6YearDividend(stock_no):
    sql = "select total from dividend where stock_no = '{stock_no}' order by stock_no"
    sql = sql.format(stock_no=stock_no)
    db = database()
    conn = db.create_connection()
    cur = conn.cursor()
    cur.execute(sql)

    rows = cur.fetchall()

    count = 0
    for row in rows:
        if row[0] > 0:
            count+=1
        else:
            break

    return count


#x = getOffer6YearDividend('001101')
#y = countAvgDividend('001101')