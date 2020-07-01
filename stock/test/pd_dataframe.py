import numpy as np
import pandas as pd
from scipy import stats
#from sklearn.feature_selection import f_regression
import matplotlib
import matplotlib.pyplot as plt
#import seaborn as sns
import MySQLdb

# 导入数据
from stock.database import database

car_dict = {'brand_model':['Acura Legend V6','Audi 100','BMW 535i','Buick Century','Buick Riviera V6'],
 'weight':[1000,2000,3000,4000,5000],
 'circle':[42,39,39,41,41],
 'max_speed':[100,400,300,500,600],
 'horsepower': [160, 130, 208, 110, 165]}
car = pd.DataFrame(car_dict)
print(car)
# 画散点图矩阵
#sns.pairplot(car)
#plt.show()


# numpy包计算样本相关系数
print(np.corrcoef((car['weight'],car['circle'],car['max_speed'],car['horsepower'])))
print(car.corr())

db = database()
conn = db.create_connection()
cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
sql = """
SELECT china_buy,china_sell,china_sum,invest_buy,invest_sell,invest_sum,com_sum,legalperson,stock_buy,stock_num,stock_amount,stock_eprice
FROM legalperson_hist a , stockprice b WHERE a.stock_no=b.stock_no AND a.data_date=b.batch_no AND a.stock_no = '{stock_no}' 
AND a.data_date between '{from_date}' and '{to_date}'
ORDER BY data_date;
"""
sql = sql.format(stock_no="002345",from_date='20190101',to_date='20191231')
print(sql)
cur2.execute(sql)
rows = cur2.fetchall()

count_posi = 0
count_negi = 0
tmp_dict = {}
index_dict = {}
index = 0
for row in rows:
    if row['legalperson'] > 0:
        count_posi += row['legalperson']
        count_negi = 0
        row.update({'foreign_sum':count_posi})
    else:
        count_negi += row['legalperson']
        count_posi = 0
        row.update({'foreign_sum': count_negi})

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',100)

df = pd.DataFrame.from_dict(rows)
print(df)

print(df.corr())