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

days=365

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
            return -1
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

date_time = datetime.datetime.now()
start = date_time - datetime.timedelta(days)
today830am = date_time.replace(hour=8,minute=30,second=0,microsecond=0)
today3pm = date_time.replace(hour=15,minute=0,second=0,microsecond=0)

if((date_time.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):
    end = datetime.date.today()
else:
    end = datetime.date.today()+ datetime.timedelta(1)

path = 'C:/Python/RawData/'
logpath = 'C:/Python/'
isPathExists = os.path.exists(path)
if not isPathExists:
    os.makedirs(path)

logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_collect.log"
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

def collect_data(func,cores):
    thread_number = 20
    stock_history_concat_df = pd.DataFrame()
    while(stock_history_concat_df.empty):
        now = datetime.datetime.now()
        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("collect_data" + str(func) + "start time:" + start_time)
        pool = Pool(cores)
        stock_async_results = []

        for ticker_chunk in ticker_chunk_list:
            stock_async_result = pool.apply_async(get_stock_data_mt,args=(func,ticker_chunk,thread_number))
            stock_async_results.append(stock_async_result)

        pool.close()
        
        for stock_async_result in stock_async_results:
            try:
                stock_chunk_df = stock_async_result.get(timeout=180)
            except TimeoutError as e:
                logging.error(str(e) + " timeout 3 minutes, terminating process pool...")
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

    cores = int(multiprocessing.cpu_count()/2)
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))

    sharesOutstanding_df=collect_data(get_quote_data,cores)
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

    stock_history_concat_df=collect_data(get_stock_history,cores)
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

    while((now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):         #get real time stock price

        realtime_df=collect_data(get_stock_realtime,cores)
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

    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    logging.info("collect process exit time:" + stop_time)