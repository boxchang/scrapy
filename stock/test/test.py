# -*- coding: utf-8 -*-
# 匯入必要模組
import pandas as pd
from sqlalchemy import create_engine
# 初始化資料庫連線，使用pymysql模組
# MySQL的使用者：root, 密碼:147369, 埠：3306,資料庫：mydb
engine = create_engine('mysql://web104:cnap*74182@111.185.227.34:3307/stock')
# 查詢語句，選出employee表中的所有資料
sql = """
select date_format(STR_TO_DATE(data_date,'%%Y%%m%%d'),'%%Y-%%m-%%d') data_date,legalperson from legalperson_hist 
where stock_no = '002345' and data_date between '20190101' and '20191231';
"""


# read_sql_query的兩個引數: sql語句， 資料庫連線
df = pd.read_sql_query(sql, engine)
df.set_index('data_date',inplace=True)
# 輸出employee表的查詢結果
df
print(df.index)


# import MySQLdb
# from stock.database import database
# import pandas as pd
#
# db = database()
# conn = db.create_connection()
# cur = conn.cursor(MySQLdb.cursors.DictCursor)
# sql = "SELECT * FROM legalperson"
# cur.execute(sql)
#
# rows = cur.fetchall()
# print(type(rows))
#
# s = pd.DataFrame.from_dict(rows)
# pd.sql

