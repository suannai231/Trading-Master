from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import akshare as ak

days=365*10

def get_stock(ticker):
    shares = -1
    try:
        quote_data = si.get_quote_data(ticker)
        shares = quote_data['sharesOutstanding']
    except Exception as e:
        if (str(e) == "'sharesOutstanding'") | (str(e) == 'Invalid response from server.  Check if ticker is\n                              valid.'):
            try:
                info = yf.Ticker(ticker).info
                shares = info['sharesOutstanding']
            except Exception as e:
                if str(e) == "'sharesOutstanding'":
                    return pd.DataFrame()
                elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                    print(e)
                    exit()
                else:
                    return pd.DataFrame()
        elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            print(e)
            exit()
        else:
            return pd.DataFrame()
    if (shares is None):
        return pd.DataFrame()
    else:
        if int(shares) < 1:
            return pd.DataFrame()
    try:
        df = si.get_data(ticker,start, end)
    except Exception as e:
        if (str(e) == "'timestamp'") | (str(e) == "'NoneType' object is not subscriptable"):
            return pd.DataFrame()
        elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            print(e)
            exit()
        else:
            return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df["shares"] = shares
    df["marketCap"] = df["close"]*shares
    df.index.name = 'date'
    return df

def get_qfq(ticker):
    try:
        qfq_df = ak.stock_us_daily(symbol=ticker, adjust="qfq-factor")
    except Exception as e:
        print(e)
        return pd.DataFrame()
    qfq_df['ticker'] = ticker
    # qfq_df['date'] = qfq_df.index
    return qfq_df

start = datetime.datetime.now() - datetime.timedelta(days)
end = datetime.date.today()

path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    nasdaq = si.tickers_nasdaq()
    other = si.tickers_other()
    tickers = nasdaq + other
    files = os.listdir(path)
    stock_file = str(end)+'_stock.csv'
    qfq_file = str(end)+'_qfq.csv'
    if (stock_file in files) & (qfq_file in files):
        exit()

    cores = multiprocessing.cpu_count()
    pool = Pool(cores*2)
    if stock_file not in files:
        stock_async_result = pool.map_async(get_stock,tickers)

    if qfq_file not in files:
        qfq_async_result = pool.map_async(get_qfq,tickers)

    pool.close()
    pool.join()

    if stock_file not in files:
        stock_df_list = stock_async_result.get()
        stock_concat_df = pd.DataFrame()
        stock_concat_df = pd.concat(stock_df_list)
        stock_concat_df.to_csv(path+f'{end}'+'_stock.csv')
    if qfq_file not in files:
        qfq_df_list = qfq_async_result.get()
        qfq_concat_df = pd.DataFrame()
        qfq_concat_df = pd.concat(qfq_df_list)
        qfq_concat_df.to_csv(path+f'{end}'+'_qfq.csv')
    if (stock_file not in files) & (qfq_file not in files):
        merged_df = pd.merge(stock_concat_df, qfq_concat_df, how='left', on=["ticker", "date"])
        merged_df.to_csv(path+f'{end}'+'.csv')
    # os.popen(f'python C:/Users/jayin/OneDrive/Code/prepare_data_MP.py')