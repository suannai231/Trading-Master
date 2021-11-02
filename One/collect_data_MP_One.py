from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import akshare as ak

days=365*10

def get_stock(ticker_chunk):
    ticker_chunk_df = pd.DataFrame()
    for ticker in ticker_chunk:
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
                        continue
                    elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                        print(e)
                        exit()
                    else:
                        continue
            elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                print(e)
                exit()
            else:
                continue
        if (shares is None):
            continue
        else:
            if int(shares) < 1:
                continue
        try:
            df = si.get_data(ticker,start, end)
        except Exception as e:
            if (str(e) == "'timestamp'") | (str(e) == "'NoneType' object is not subscriptable"):
                continue
            elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                print(e)
                exit()
            else:
                continue
        if not df.empty:
            df["shares"] = shares
            df["marketCap"] = df["close"]*shares
            df.index.name = 'date'
            ticker_chunk_df = ticker_chunk_df.append(df)
    return ticker_chunk_df

def get_qfq(ticker_chunk):
    ticker_chunk_df = pd.DataFrame()
    for ticker in ticker_chunk:
        try:
            qfq_df = ak.stock_us_daily(symbol=ticker, adjust="qfq-factor")
        except Exception as e:
            print(e)
            continue
        if not qfq_df.empty:
            qfq_df['ticker'] = ticker
            ticker_chunk_df = ticker_chunk_df.append(qfq_df)
    return ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

start = datetime.datetime.now() - datetime.timedelta(days)
end = datetime.date.today()

path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    files = os.listdir(path)
    stock_file = str(end)+'_stock.feather'
    qfq_file = str(end)+'_qfq.feather'
    if (stock_file in files) & (qfq_file in files):
        exit()
    
    nasdaq = si.tickers_nasdaq()
    other = si.tickers_other()
    tickers = nasdaq + other

    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/cores)))

    if qfq_file not in files:
        pool1 = Pool(cores)
        qfq_async_results = []
        for ticker_chunk in ticker_chunk_list:
            qfq_async_result = pool1.apply_async(get_qfq,args=(ticker_chunk,))
            qfq_async_results.append(qfq_async_result)
        pool1.close()

    if stock_file not in files:
        pool2 = Pool(cores)
        stock_async_results = []
        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool2.apply_async(get_stock,args=(ticker_chunk,))
            stock_async_results.append(stock_async_result)
        pool2.close()

    # pool1.join()
    if qfq_file not in files:
        qfq_concat_df = pd.DataFrame()
        for qfq_async_result in qfq_async_results:
            qfq_chunk_df = qfq_async_result.get()
            if not qfq_chunk_df.empty:
                qfq_concat_df = qfq_concat_df.append(qfq_chunk_df)
        qfq_concat_df.reset_index(inplace=True)
        qfq_concat_df.to_feather(path+f'{end}'+'_qfq.feather')
    
    # pool2.join()
    if stock_file not in files:
        stock_concat_df = pd.DataFrame()
        for stock_async_result in stock_async_results:
            stock_chunk_df = stock_async_result.get()
            if not stock_chunk_df.empty:
                stock_concat_df = stock_concat_df.append(stock_chunk_df)
        stock_concat_df.reset_index(inplace=True)
        stock_concat_df.to_feather(path+f'{end}'+'_stock.feather')

    if (stock_file not in files) & (qfq_file not in files):
        merged_df = pd.merge(stock_concat_df, qfq_concat_df, how='left', on=["ticker", "date"])
        merged_df.to_feather(path+f'{end}'+'.feather')
    os.popen(f'python C:/Code/One/process_data_MP_One.py')