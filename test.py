import akshare as ak
qfq_df = ak.stock_us_daily(symbol="DTST", adjust="qfq-factor")
print(qfq_df)