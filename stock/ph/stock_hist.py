from fastquant import get_pse_data
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np

df = get_pse_data("ALI", "2022-05-22", "2022-05-27")
print(df.head())

# Derive the 30 day SMA of JFC's closing prices
ma20 = df.close.rolling(20).mean()
ma5 = df.close.rolling(5).mean()
# Combine the closing prices with the 30 day SMA
close_ma30_ma5 = pd.concat([df.close, ma20, ma5], axis=1).dropna()  # axis=1 取列值 # dropna去掉NA值
close_ma30_ma5.columns = ['Closing Price', 'Simple Moving Average (20 day)', 'Simple Moving Average (5 day)']
# Plot the closing prices with the 30 day SMA
close_ma30_ma5.plot(figsize=(10, 6))
plt.title("Daily Closing Prices vs 20 day SMA of JFC\nfrom 2022-02-01 to 2022-04-30", fontsize=20)
#df.close.plot(figsize=(10, 6))
plt.show()

# df =pd.DataFrame(np.random.rand(10,4), columns=['a', 'b', 'c', 'd'])
# df.plot.bar(stacked=True)
# plt.show()