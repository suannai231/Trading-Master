from operator import index
from yahoo_fin import stock_info as si
import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool,TimeoutError
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
import logging
import math
import numpy as np
import time
import sys

days=365*5
date_time = datetime.datetime.now()

today8am = date_time.replace(hour=8,minute=0,second=0,microsecond=0)
today3pm = date_time.replace(hour=15,minute=0,second=0,microsecond=0)

end = datetime.date.today()
start = end - datetime.timedelta(days)
# if((date_time.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)):
#     end = datetime.date.today()
# else:
#     end = datetime.date.today()+ datetime.timedelta(1)

# def get_quote_data(ticker):
#     df = pd.DataFrame()
#     try:
#         dict = si.get_quote_data(ticker)
#         # dict['ticker'] = ticker
#         if 'sharesOutstanding' in dict.keys():
#             sharesOutstanding = dict['sharesOutstanding']
#         else:
#             return pd.DataFrame()
#     except Exception as e:
#         if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
#             return -1
#         else:
#             return pd.DataFrame()
#     d={'ticker':[ticker],'sharesOutstanding':[sharesOutstanding]}
#     df = pd.DataFrame(d)
#     df = df.set_index('ticker')
#     return df

def get_stock_history(ticker):
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start,end,index_as_date=False)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            # log("error","get_stock_history " + ticker + " " + str(e))
            return pd.DataFrame()
    # df.index.name = 'date'
    # if (not df.empty) and df.index[-1]!=datetime.date.today():
    #     log("error",ticker+" date error")
    #     return pd.DataFrame()
    return df

def get_stock_realtime(ticker):
    df = pd.DataFrame()
    try:
        close = float(si.get_live_price(ticker))
        # quote_table = si.get_quote_table(ticker)
        # open = float(quote_table['Open'])
        # low = float(quote_table["Day's Range"].split(" - ")[0])
        # high = float(quote_table["Day's Range"].split(" - ")[1])
        # volume = int(quote_table['Volume'])
        open = np.nan
        low = np.nan
        high = np.nan
        volume = np.nan
        d = {'date':end, 'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
        # df=pd.DataFrame(d,index=[str(end)])
        df=pd.DataFrame(d,index=[str(end)])
        df.date=pd.to_datetime(df.date)
        # df.index.name = 'date'
        # if (not df.empty) and df.index[-1]!=str(datetime.date.today()):
        #     log("error",ticker+" date error")
        #     return pd.DataFrame()
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            # log("error","get_stock_realtime " + ticker + " " + str(e))
            return pd.DataFrame()
    # if ticker=="NEXA":
    #     log("info",ticker)
    return df

def get_stock_data_mt(func,ticker_chunk,thread_number):

    with ThreadPoolExecutor(thread_number) as tp:
        jobs = [tp.submit(func,ticker)  for ticker in ticker_chunk]
        ticker_chunk_df = pd.DataFrame()
        for job in cf.as_completed(jobs):
            df = job.result()
            if isinstance(df,int):
                return -1
            if not df.empty:
                ticker_chunk_df = pd.concat([ticker_chunk_df,df])
    return ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def collect_data(func,cores,thread_number):
    log('info',"collect_data" + str(func) + " start.")

    df = pd.DataFrame()
    while(df.empty):
        pool = Pool(cores)
        stock_async_results = []
        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_data_mt,args=(func,ticker_chunk,thread_number))
            stock_async_results.append(stock_async_result)
        pool.close()
        log('info',"process pool start.")

        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=120)
            except TimeoutError as e:
                log('critical',"timeout 2 minutes, terminating process pool...")
                pool.terminate()
                pool.join()
                if(thread_number<40):
                    thread_number += 1
                else:
                    thread_number = 20
                log('critical',"thread_number:"+str(thread_number))
                break
            if isinstance(stock_chunk_df,int):
                log('critical',"network connection error, terminating process pool...")
                pool.terminate()
                pool.join()
                if(thread_number>1):
                    thread_number -= 1
                else:
                    thread_number = 20
                log('critical',"thread_number:"+str(thread_number))
                df = pd.DataFrame()
                break
            if not stock_chunk_df.empty:
                df = pd.concat([df,stock_chunk_df])
            else:
                log("error","collect_data" + str(func) + " stock_chunk_df empty.")
    log('info',"collect_data" + str(func) + " stop.")
    return df

def log(type,string):
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_collect.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
    
    now = datetime.datetime.now()
    log_time = now.strftime("%m%d%Y-%H%M%S")
    if type=='info':
        logging.info(log_time+":"+string)
    elif type=='warning':
        logging.warning(log_time+":"+string)
    elif type=='error':
        logging.error(log_time+":"+string)
    elif type=='critical':
        logging.critical(log_time+":"+string)

if __name__ == '__main__':
    path = '//jack-nas.home/Work/Python/RawData/'
    quote_data_path = '//jack-nas.home/Work/Python/QuoteData/'
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    log('info','collect process started.')

    nasdaq = []
    other = []
    tickers = []

    # while((len(nasdaq)==0) or (len(other)==0)):
    #     try:
    #         nasdaq = si.tickers_nasdaq()
    #         other = si.tickers_other()
    #     except Exception as e:
    #         log('critical','get tickers exception:'+str(e))
    #         continue
    #     tickers = nasdaq + other
    quote_data_files = os.listdir(quote_data_path)
    while len(quote_data_files) == 0:
        log('warning',"quote data not ready, sleep 10 seconds...")
        time.sleep(10)
        quote_data_files = os.listdir(quote_data_path)

    log('info',"processing "+quote_data_files[-1])
    try:
        time.sleep(1)
        quote_data_df = pd.read_feather(quote_data_path + quote_data_files[-1])
        log('info',quote_data_path + quote_data_files[-1]+" loaded.")
    except Exception as e:
        log('critical',str(e))
        sys.exit()

    tickers = quote_data_df[(quote_data_df.marketCap<=1000000000) & (((quote_data_df.regularMarketPreviousClose<=10) & (quote_data_df.regularMarketPreviousClose>=1)) | ((quote_data_df.regularMarketPreviousClose>=11) & (quote_data_df.regularMarketPreviousClose<=20)))].ticker.values

    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))

    # sharesOutstanding_df=collect_data(get_quote_data,cores)
    # log('info','sharesOutstanding_df is ready.')
    # if not sharesOutstanding_df.empty: 
    #     sharesOutstanding_df.reset_index(inplace=True)
    #     stop_time = datetime.datetime.now().strftime("%m%d%Y")
    #     try:
    #         sharesOutstanding_path = 'C:/Python/sharesOutstanding/'
    #         sharesOutstanding_df.to_feather(sharesOutstanding_path + stop_time + "_sharesOutstanding.feather")
    #         log('info','sharesOutstanding_df to_feather saved.')
    #     except Exception as e:
    #         log('critical',"to_feather:"+str(e))

    stock_history_concat_df=collect_data(get_stock_history,cores,10)
    
    if (not stock_history_concat_df.empty):
        log('info','stock_history_concat_df is ready.')
        now = datetime.datetime.now()
        today15 = now.replace(hour=15,minute=0,second=0,microsecond=0)
        today235959 = now.replace(hour=23,minute=59,second=59,microsecond=0)
        if ((now.weekday() <= 4) & (today15 <= datetime.datetime.now() <= today235959)):
            realtime_df=collect_data(get_stock_realtime,cores,10)
            if not realtime_df.empty:
                log('info','realtime_df is ready')
                realtime_df.reset_index(inplace=True,drop=True)
                stock_concat_df = pd.concat([stock_history_concat_df,realtime_df])
                stock_concat_df.reset_index(inplace=True,drop=True)
                # stock_realtime_concat_df = stock_concat_df
                stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
                try:
                    stock_concat_df.to_feather(path + stop_time + ".feather")
                    log('info',path + stop_time + ".feather" + "saved")
                except Exception as e:
                    log('critical',"to_feather:"+str(e))
            else:
                log('error','realtime_df is empty')
        else:  
            stock_history_concat_df.reset_index(inplace=True,drop=True)
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            try:
                stock_history_concat_df.to_feather(path + stop_time + ".feather")
                log('info','stock_history_concat_df to_feather saved.')
            except Exception as e:
                log('critical',"to_feather:"+str(e))

        now = datetime.datetime.now()
        today8 = now.replace(hour=8,minute=0,second=0,microsecond=0)
        today15 = now.replace(hour=15,minute=0,second=0,microsecond=0)
        while(True):         #get real time stock price
            if ((now.weekday() <= 4) & (today8 <= datetime.datetime.now() <= today15)):
                realtime_df=collect_data(get_stock_realtime,cores,10)
                if not realtime_df.empty:
                    log('info','realtime_df is ready')
                    realtime_df.reset_index(inplace=True,drop=True)
                    stock_concat_df = pd.concat([stock_history_concat_df,realtime_df])
                    stock_concat_df.reset_index(inplace=True,drop=True)
                    # stock_realtime_concat_df = stock_concat_df
                    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
                    try:
                        stock_concat_df.to_feather(path + stop_time + ".feather")
                        log('info',path + stop_time + ".feather" + "saved")
                    except Exception as e:
                        log('critical',"to_feather:"+str(e))
                else:
                    log('error','realtime_df is ready')
            else:
                break
    else:
        log('error','stock_history_concat_df is empty.')

    log('info','collect process exit.')