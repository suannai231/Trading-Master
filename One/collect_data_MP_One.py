from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import datetime
import os
import sys
import multiprocessing
from multiprocessing import Pool
import akshare as ak
import math

days=365

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'T':1000000000000}

def string_to_int(string):
    if string[-1].isdigit(): # check if no suffix
        return int(string)
    mult = multipliers[string[-1]] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return int(float(string[:-1]) * mult)

def get_stock(ticker_chunk):
    ticker_chunk_df = pd.DataFrame()
    for ticker in ticker_chunk:
        try:
            df = si.get_data(ticker,start, end + datetime.timedelta(1))
        except Exception as e:
            print("si.get_data "+ticker+" error: "+str(e))

            if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                return ticker_chunk_df
            else:
                continue
        # try:
        #     dic = si.get_quote_table(ticker)
        # except Exception as e:
        #     print("si.get_quote_table "+ticker+" error:"+str(e))
        #     continue
        
        # if('Market Cap' in dic.keys()):
        #     if isinstance(dic['Market Cap'],str):
        #         marketcap = string_to_int(dic['Market Cap'])
        #     else:
        #         print(ticker+" marketcap is not available")
        #         continue
        # else:
        #     print(ticker+" marketcap is not available")
        #     continue

        if not df.empty:
            # df["shares"] = shares
            # df["marketCap"] = marketcap
            df.index.name = 'date'
            ticker_chunk_df = pd.concat([ticker_chunk_df,df])
        else:
            print(ticker+" data is not available")
            continue
    return ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

date_time = datetime.datetime.now() 
start = date_time - datetime.timedelta(days)
end = datetime.date.today()

datetime_str = date_time.strftime("%m%d%Y-%H%M%S")

path = 'C:/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    files = os.listdir(path)
    stock_file = datetime_str + '.feather'

    if (stock_file in files):
        print("error: " + stock_file + " existed.")
        sys.exit(1)
    
    nasdaq = si.tickers_nasdaq()
    other = si.tickers_other()
    tickers = nasdaq + other

    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/(cores*3))))
    proc_num = len(ticker_chunk_list)


    pool = Pool(proc_num)
    stock_async_results = []

    for ticker_chunk in ticker_chunk_list:
        stock_async_result = pool.apply_async(get_stock,args=(ticker_chunk,))
        stock_async_results.append(stock_async_result)

    pool.close()

    stock_concat_df = pd.DataFrame()
    for stock_async_result in stock_async_results:
        stock_chunk_df = stock_async_result.get()
        if not stock_chunk_df.empty:
                stock_concat_df = pd.concat([stock_concat_df,stock_chunk_df])

    if not stock_concat_df.empty:
        stock_concat_df.reset_index(inplace=True)
        stock_concat_df.to_feather(path+stock_file)
        os.popen(f'python C:/Code/One/process_data_MP_One.py')
    else:
        print("stock_concat_df is empty.")
        sys.exit(2)