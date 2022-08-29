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
import locale

days=365

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'T':1000000000000}

def string_to_int(string):
    if string[-1].isdigit(): # check if no suffix
        return int(string)
    mult = multipliers[string[-1]] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return int(float(string[:-1]) * mult)

# def get_FityTwo_Week_Low(ticker):
#     try:
#         quote_table = si.get_quote_table(ticker)
#     except Exception as e:
#         if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
#             logging.critical("get_stock_realtime "+ticker+" error: "+str(e)+". sys.exit...")
#             sys.exit(3)
#         else:
#             logging.debug(ticker+str(e))
#             return pd.DataFrame()
#     FityTwo_Week_Low = float('nan')
#     if isinstance(quote_table["52 Week Range"], str):
#         locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
#         try:
#             FityTwo_Week_Low = locale.atof(quote_table["52 Week Range"].split(" - ")[0])
#         except Exception as e:
#             logging.error(quote_table["52 Week Range"].split(" - ")[0] + " string to float exception:"+str(e))
#     else:
#         logging.error(ticker + "52 week range is nan.")
#     d = {'FityTwo_Week_Low':FityTwo_Week_Low}
#     df=pd.DataFrame(d,index=[ticker],)
#     return df

def get_quote_data(ticker):
    df = pd.DataFrame()
    try:
        dict = si.get_quote_data(ticker)
        # dict['ticker'] = ticker
        if 'sharesOutstanding' in dict.keys():
            sharesOutstanding = dict['sharesOutstanding']
        else:
            return pd.DataFrame()
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            # logging.critical("get_stock_history "+ticker+" error: "+str(e)+". sys.exit...")
            return -1
        else:
            return pd.DataFrame()
    d={'ticker':[ticker],'sharesOutstanding':[sharesOutstanding]}
    df = pd.DataFrame(d)
    df = df.set_index('ticker')
    return df

def get_stock_history(ticker):
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start,end,index_as_date=True)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            # logging.critical("get_stock_history "+ticker+" error: "+str(e)+". sys.exit...")
            return -1
    df.index.name = 'date'
    return df

def get_stock_realtime(ticker):
    df = pd.DataFrame()
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
        df.index.name = 'date'
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
        #     logging.critical("get_stock_realtime "+ticker+" error: "+str(e)+". sys.exit...")
            return -1
        # logging.critical(ticker+" "+str(e))
        # open = close
        # low = close
        # high = close
        # volume = df.iloc[-1].volume
        # return pd.DataFrame()
    return df

# def get_FityTwo_Week_Low_mt(ticker_chunk):

#     thread_number = 20

#     with ThreadPoolExecutor(thread_number) as tp:
#         jobs = [tp.submit(get_FityTwo_Week_Low,ticker)  for ticker in ticker_chunk]
#         ticker_chunk_df = pd.DataFrame()
#         for job in cf.as_completed(jobs):
#             df = job.result()
#             if not df.empty:
#                 ticker_chunk_df = pd.concat([ticker_chunk_df,df])
#     return ticker_chunk_df

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

# def get_stock_history_mt(ticker_chunk,thread_number):

#     with ThreadPoolExecutor(thread_number) as tp:
#         jobs = [tp.submit(get_stock_history,ticker)  for ticker in ticker_chunk]
#         ticker_chunk_df = pd.DataFrame()
#         for job in cf.as_completed(jobs):
#             df = job.result()
#             if isinstance(df,int):
#                 return -1
#             if not df.empty:
#                 ticker_chunk_df = pd.concat([ticker_chunk_df,df])
#     return ticker_chunk_df

# def get_stock_realtime_mt(ticker_chunk,thread_number):

#     with ThreadPoolExecutor(thread_number) as tp:
#         jobs = [tp.submit(get_stock_realtime,ticker)  for ticker in ticker_chunk]
#         ticker_chunk_df = pd.DataFrame()
#         for job in cf.as_completed(jobs):
#             df = job.result()
#             if isinstance(df,int):
#                 return -1
#             if not df.empty:
#                 ticker_chunk_df = pd.concat([ticker_chunk_df,df])
#     return ticker_chunk_df

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
today830am = date_time.replace(hour=8,minute=30,second=0,microsecond=0)
today3pm = date_time.replace(hour=15,minute=0,second=0,microsecond=0)

if((date_time.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):
    end = datetime.date.today()
else:
    end = datetime.date.today()+ datetime.timedelta(1)

# path = '//jack-nas.home/Work/Python/RawData/'

# logpath = '//jack-nas.home/Work/Python/'
path = 'C:/Python/RawData/'
logpath = 'C:/Python/'
isPathExists = os.path.exists(path)
if not isPathExists:
    os.makedirs(path)

logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_collect.log"
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

def collect_data(func):
    thread_number = 20
    stock_history_concat_df = pd.DataFrame()
    while(stock_history_concat_df.empty):
        now = datetime.datetime.now()
        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("collect_data" + str(func) + "start time:" + start_time)
        pool = Pool(proc_num)
        stock_async_results = []

        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_data_mt,args=(func,ticker_chunk,thread_number))
            stock_async_results.append(stock_async_result)

        pool.close()
        
        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=720)
            except TimeoutError as e:
                logging.error(str(e) + " timeout 720 seconds, terminating process pool...")
                pool.terminate()
                pool.join()
                break
            if isinstance(stock_chunk_df,int):
                if(thread_number!=1):
                    thread_number -= 1
                else:
                    thread_number = 20
                logging.critical("thread_number:"+str(thread_number))
                pool.terminate()
                pool.join()
                stock_history_concat_df = pd.DataFrame()
                break
            if not stock_chunk_df.empty:
                stock_history_concat_df = pd.concat([stock_history_concat_df,stock_chunk_df])
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("collect_data" + str(func) + "stop time:" + stop_time)
        return stock_history_concat_df

if __name__ == '__main__':
    now = datetime.datetime.now()
    start_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("collect process start time:" + start_time)

    # files = os.listdir(path)
    # stock_file = start_time + '.feather'

    # if (stock_file in files):
    #     print("error: " + stock_file + " existed.")
    #     sys.exit(1)
    
    nasdaq = []
    other = []
    tickers = []

    while((len(nasdaq)==0) or (len(other)==0)):
        try:
            nasdaq = si.tickers_nasdaq()
            other = si.tickers_other()
        except Exception as e:
            logging.critical(str(e))
            continue
        tickers = nasdaq + other

    cores = int(multiprocessing.cpu_count()/4)
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))
    proc_num = len(ticker_chunk_list)

    # FityTwo_Week_Low_df = pd.DataFrame()
    # while(FityTwo_Week_Low_df.empty):
    #     now = datetime.datetime.now()
    #     start_time = now.strftime("%m%d%Y-%H%M%S")
    #     logging.info("start time:" + start_time)
    #     pool = Pool(proc_num)
    #     stock_async_results = []

    #     for ticker_chunk in ticker_chunk_list:
    #         stock_async_result = pool.apply_async(get_FityTwo_Week_Low_mt,args=(ticker_chunk,))
    #         stock_async_results.append(stock_async_result)

    #     pool.close()
        
    #     for stock_async_result in stock_async_results:
    #         try:
    #             stock_chunk_df = stock_async_result.get(timeout=720)
    #         except TimeoutError as e:
    #             logging.error(str(e) + " timeout 720 seconds, terminating process pool...")
    #             pool.terminate()
    #             pool.join()
    #             break
    #         if not stock_chunk_df.empty:
    #             FityTwo_Week_Low_df = pd.concat([FityTwo_Week_Low_df,stock_chunk_df])
    #     if not FityTwo_Week_Low_df.empty: 
    #         stop_time = datetime.datetime.now().strftime("%m%d%Y")
    #         try:
    #             FityTwo_Week_Low_df.reset_index(inplace=True)
    #             FityTwo_Week_Low_df.rename(columns={"index":"ticker"},inplace=True)
    #             FityTwo_Week_Low_df.to_feather(path + stop_time + "_FityTwo_Week_Low_df.feather")
    #         except Exception as e:
    #             logging.critical("to_feather:"+str(e))
    #     stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    #     logging.info("stop time:" + stop_time)

    sharesOutstanding_df=collect_data(get_quote_data)
    logging.info("sharesOutstanding_df is ready.")
    if not sharesOutstanding_df.empty: 
        sharesOutstanding_df.reset_index(inplace=True)
        stop_time = datetime.datetime.now().strftime("%m%d%Y")
        try:
            sharesOutstanding_path = 'C:/Python/sharesOutstanding/'
            sharesOutstanding_df.to_feather(sharesOutstanding_path + stop_time + "_sharesOutstanding.feather")
            logging.info("sharesOutstanding_df to_feather saved.")
        except Exception as e:
            logging.critical("to_feather:"+str(e))

    stock_history_concat_df=collect_data(get_stock_history)
    logging.info("stock_history_concat_df is ready.")

    today830am = now.replace(hour=8,minute=30,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    if(not (now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):
        if (not stock_history_concat_df.empty):
            stock_history_concat_df.reset_index(inplace=True)
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            try:
                stock_history_concat_df.to_feather(path + stop_time + ".feather")
                logging.info("stock_history_concat_df to_feather saved.")
            except Exception as e:
                logging.critical("to_feather:"+str(e))

    # if(now>today3pm):
    #     logging.info("time passed 3:05pm.")
    #     break

    # thread_number = 20
    # stock_realtime_concat_df = pd.DataFrame()
    while((now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):         #get real time stock price
        # now = datetime.datetime.now()
        # start_time = now.strftime("%m%d%Y-%H%M%S")
        # logging.info("start time:" + start_time)
        # pool = Pool(proc_num)
        # stock_async_results = []

        # for ticker_chunk in ticker_chunk_list:
        #     stock_async_result = pool.apply_async(get_stock_realtime_mt,args=(ticker_chunk,thread_number))
        #     stock_async_results.append(stock_async_result)

        # pool.close()
        # df = pd.DataFrame()
        # for stock_async_result in stock_async_results:
        #     try:
        #         stock_chunk_df = stock_async_result.get(timeout=360)
        #     except TimeoutError as e:
        #         logging.error(str(e) + " timeout 360 seconds, terminating process pool...")
        #         pool.terminate()
        #         pool.join()
        #         break
        #     if isinstance(stock_chunk_df,int):
        #         if(thread_number!=1):
        #             thread_number -= 1
        #         else:
        #             thread_number = 20
        #         pool.terminate()
        #         pool.join()
        #         logging.critical("thread_number:"+str(thread_number))
        #         break
        #     if not stock_chunk_df.empty:
        #         df = pd.concat([df,stock_chunk_df])

        realtime_df=collect_data(get_stock_realtime)
        logging.info('realtime_df is ready')
        if not realtime_df.empty: 
            stock_concat_df = pd.concat([stock_history_concat_df,realtime_df])
            stock_concat_df.reset_index(inplace=True)
            # stock_realtime_concat_df = stock_concat_df
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            try:
                stock_concat_df.to_feather(path + stop_time + ".feather")
                logging.info(path + stop_time + ".feather" + "saved")
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        # stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        # logging.info("stop time:" + stop_time)


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