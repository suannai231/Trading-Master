import pandas as pd

df1 = pd.read_csv('//Jack-NAS/Work/Python/RawData/2021-10-27_stock.csv')
df2 = pd.read_csv('//Jack-NAS/Work/Python/RawData/2021-10-27_qfq.csv')
result = pd.merge(df1, df2, how='left', on=["ticker", "date"])
result.to_csv('//Jack-NAS/Work/Python/RawData/2021-10-27.csv')