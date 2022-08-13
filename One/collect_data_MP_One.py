from operator import index
from urllib3 import Timeout
from yahoo_fin import stock_info as si
# import yfinance as yf
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
import math

days=365*2

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'T':1000000000000}

def string_to_int(string):
    if string[-1].isdigit(): # check if no suffix
        return int(string)
    mult = multipliers[string[-1]] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return int(float(string[:-1]) * mult)

def get_stock_history(ticker):
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start,end,index_as_date=True)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            logging.critical("get_stock_history "+ticker+" error: "+str(e)+". sys.exit...")
            sys.exit(3)
    df.index.name = 'date'
    return df

def get_stock_realtime(ticker):
    try:
        df = si.get_data(ticker,end,end + datetime.timedelta(1),index_as_date=True)
        if(df.empty):
            close = float(si.get_live_price(ticker))
            quote_table = si.get_quote_table(ticker)
            open = float(quote_table['Open'])
            low = float(quote_table["Day's Range"].split(" - ")[0])
            high = float(quote_table["Day's Range"].split(" - ")[1])
            volume = int(quote_table['Volume'])
            d = {'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
            df=pd.DataFrame(d,index=[str(end)])
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            logging.critical("get_stock_realtime "+ticker+" error: "+str(e)+". sys.exit...")
            sys.exit(3)
        # logging.critical(ticker+" "+str(e))
        # open = close
        # low = close
        # high = close
        # volume = df.iloc[-1].volume
        return pd.DataFrame()
    df.index.name = 'date'
    return df

def get_stock_history_mt(ticker_chunk):

    thread_number = 20

    with ThreadPoolExecutor(thread_number) as tp:
        jobs = [tp.submit(get_stock_history,ticker)  for ticker in ticker_chunk]
        ticker_chunk_df = pd.DataFrame()
        for job in cf.as_completed(jobs):
            df = job.result()
            if not df.empty:
                ticker_chunk_df = pd.concat([ticker_chunk_df,df])
    return ticker_chunk_df

def get_stock_realtime_mt(ticker_chunk):

    thread_number = 20

    with ThreadPoolExecutor(thread_number) as tp:
        jobs = [tp.submit(get_stock_realtime,ticker)  for ticker in ticker_chunk]
        ticker_chunk_df = pd.DataFrame()
        for job in cf.as_completed(jobs):
            df = job.result()
            if not df.empty:
                ticker_chunk_df = pd.concat([ticker_chunk_df,df])
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
    now = datetime.datetime.now()
    start_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("collect process start time:" + start_time)

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
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))
    proc_num = len(ticker_chunk_list)

    stock_history_concat_df = pd.DataFrame()
    while(stock_history_concat_df.empty):
        now = datetime.datetime.now()
        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)
        pool = Pool(proc_num)
        stock_async_results = []

        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_history_mt,args=(ticker_chunk,))
            stock_async_results.append(stock_async_result)

        pool.close()
        
        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=360)
            except TimeoutError as e:
                logging.error(str(e) + " timeout 360 seconds, terminating process pool...")
                pool.terminate()
                pool.join()
                break
            if not stock_chunk_df.empty:
                stock_history_concat_df = pd.concat([stock_history_concat_df,stock_chunk_df])
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("stop time:" + stop_time)

    today830am = now.replace(hour=8,minute=30,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)
    # if(now>today3pm):
    #     logging.info("time passed 3:05pm.")
    #     break

    stock_realtime_concat_df = pd.DataFrame()
    while((now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):         #get real time stock price
        now = datetime.datetime.now()
        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)
        pool = Pool(proc_num)
        stock_async_results = []

        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_realtime_mt,args=(ticker_chunk,))
            stock_async_results.append(stock_async_result)

        pool.close()
        df = pd.DataFrame()
        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=360)
            except TimeoutError as e:
                logging.error(str(e) + " timeout 360 seconds, terminating process pool...")
                pool.terminate()
                pool.join()
                break
            if not stock_chunk_df.empty:
                df = pd.concat([df,stock_chunk_df])

        if not df.empty: 
            stock_concat_df = pd.concat([stock_history_concat_df,df])
            stock_concat_df.reset_index(inplace=True)
            stock_realtime_concat_df = stock_concat_df
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            try:
                stock_concat_df.to_feather(path + stop_time + ".feather")
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("stop time:" + stop_time)

    if (not stock_realtime_concat_df.empty):
        stock_concat_df = stock_realtime_concat_df
    else:
        stock_concat_df = stock_history_concat_df

    stock_concat_df.reset_index(inplace=True)
    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    try:
        stock_concat_df.to_feather(path + stop_time + ".feather")
    except Exception as e:
        logging.critical("to_feather:"+str(e))
    # os.popen(f'python C:/Code/One/process_data_MP_One.py')

    # sys.exit(2)
    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    logging.info("collect process exit time:" + stop_time)
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