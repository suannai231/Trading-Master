from operator import index
from urllib3 import Timeout
from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import datetime
import os
import sys
import multiprocessing
from multiprocessing import Pool,TimeoutError
# import akshare as ak
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
import logging
import time

days=365

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'T':1000000000000}

def string_to_int(string):
    if string[-1].isdigit(): # check if no suffix
        return int(string)
    mult = multipliers[string[-1]] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return int(float(string[:-1]) * mult)

def get_stock(ticker):
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start,end + datetime.timedelta(1),index_as_date=True)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            logging.critical("si.get_data "+ticker+" error: "+str(e)+". sys.exit...")
            sys.exit(3)
    df.index.name = 'date'
    if not df.empty:
        if(len(df.loc[df.index==str(end)])==0):         #get real time stock price
            close = si.get_live_price(ticker)
            try:
                open = si.get_quote_table(ticker)['Open']
                low = si.get_quote_table(ticker)["Day's Range"].split(" - ")[0]
                high = si.get_quote_table(ticker)["Day's Range"].split(" - ")[1]
                volume = si.get_quote_table(ticker)['Volume']
            except Exception as e:
                logging.critical(ticker+" "+str(e))
                # open = close
                # low = close
                # high = close
                # volume = df.iloc[-1].volume
                return pd.DataFrame()
            d = {'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
            # ser = pd.Series(data=d, name=str(end), index=['open', 'high', 'low', 'close', 'adjclose', 'volume', 'ticker'])
            df2=pd.DataFrame(d,index=[str(end)])
            df3= pd.concat([df,df2])
            # logging.warning(ticker+" "+str(end)+" data is not available, sleep 60 seconds...")
            # time.sleep(60)
            return df3
    return df

def get_stock_mt(ticker_chunk):

    thread_number = 20

    with ThreadPoolExecutor(thread_number) as tp:
        jobs = [tp.submit(get_stock,ticker)  for ticker in ticker_chunk]
        ticker_chunk_df = pd.DataFrame()
        for job in cf.as_completed(jobs):
            df = job.result()
            if not df.empty:
                ticker_chunk_df = pd.concat([ticker_chunk_df,df])
            # else:
            #     print(df.tail(-1).ticker+" data is not available")
            #     continue

    return ticker_chunk_df

def get_stock_mp(ticker_chunk):
    ticker_chunk_df = pd.DataFrame()
    for ticker in ticker_chunk:
        try:
            df = si.get_data(ticker,start, end + datetime.timedelta(1))
        except Exception as e:
            logging.critical("si.get_data "+ticker+" error: "+str(e))

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
            logging.info(ticker+" data is not available")
            continue
    return ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

date_time = datetime.datetime.now()
start = date_time - datetime.timedelta(days)
end = datetime.date.today()

path = '//jack-nas/Work/Python/RawData/'

logpath = '//jack-nas/Work/Python/'
logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_collect.log"
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

if __name__ == '__main__':
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    # files = os.listdir(path)
    # stock_file = start_time + '.feather'

    # if (stock_file in files):
    #     print("error: " + stock_file + " existed.")
    #     sys.exit(1)
    
    nasdaq = si.tickers_nasdaq()
    other = si.tickers_other()
    tickers = nasdaq + other

    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/(cores))))
    proc_num = len(ticker_chunk_list)

    while True:
        start_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)

        pool = Pool(proc_num)
        stock_async_results = []

        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_mt,args=(ticker_chunk,))
            stock_async_results.append(stock_async_result)

        pool.close()

        stock_concat_df = pd.DataFrame()
        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=120)
            except TimeoutError as e:
                logging.error(str(e), " timeout 120 seconds, terminating process pool...")
                pool.terminate()
                pool.join()
                break
            if not stock_chunk_df.empty:
                stock_concat_df = pd.concat([stock_concat_df,stock_chunk_df])

        if not stock_concat_df.empty:
            stock_concat_df.reset_index(inplace=True)
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            stock_concat_df.to_feather(path + stop_time + ".feather")
            logging.info("stop time:" + stop_time)
            # os.popen(f'python C:/Code/One/process_data_MP_One.py')
        else:
            logging.error("stock_concat_df is empty.")
            # sys.exit(2)

    # thread_number = proc_num

    # with ThreadPoolExecutor(thread_number) as tp:
    #     jobs = [tp.submit(get_stock,ticker_chunk)  for ticker_chunk in ticker_chunk_list]
    #     stock_concat_df = pd.DataFrame()
    #     for job in cf.as_completed(jobs):
    #         stock_chunk_df = job.result()
    #         if not stock_chunk_df.empty:
    #             stock_concat_df = pd.concat([stock_concat_df,stock_chunk_df])

    #     if not stock_concat_df.empty:
    #         stock_concat_df.reset_index(inplace=True)
    #         stock_concat_df.to_feather(path+stock_file)
    #         # os.popen(f'python C:/Code/One/process_data_MP_One.py')
    #     else:
    #         print("stock_concat_df is empty.")
    #         sys.exit(2)