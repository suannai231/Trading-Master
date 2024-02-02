from operator import index
from yahoo_fin import stock_info as si
import pandas as pd
from datetime import datetime
from datetime import timedelta
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
import requests
from bs4 import BeautifulSoup

days=365
date_time = datetime.now()

# marketCapMax = 5000000000
marketCapMin = 10000000

# regularMarketPreviousCloseMax = 20
regularMarketPreviousCloseMin = 1

today8am = date_time.replace(hour=8,minute=0,second=0,microsecond=0)
today3pm = date_time.replace(hour=15,minute=0,second=0,microsecond=0)

end = datetime.today()
start = end - timedelta(days)

# Define a dictionary to map units to multipliers
multipliers = {
    '百': 100,
    '千': 1000,
    '万': 10000,
    '亿': 100000000,
}

# Define a function to convert an input string to an integer
def convert_int_string(input_string):
    multiplier = 1
    for unit, value in multipliers.items():
        if unit in input_string:
            multiplier = value
            input_string = input_string.replace(unit, '')
            break
    return float(input_string) * multiplier

def realtime_required(df):

    dt_str = end.strftime('%Y-%m-%d')
    np_dt = np.datetime64(dt_str)
    return not np_dt in df.date.values

def get_stock_realtime_xueqiu(ticker):
    # if ticker=="EH":
    #     log("debug",ticker)
    df = pd.DataFrame()
    try:
        close = float(si.get_live_price(ticker))
        url = f"https://xueqiu.com/S/{ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
        }
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        regularMarketVolume_element = soup.find_all('td')[2].find('span')
        if regularMarketVolume_element is not None:
            regularMarketVolume = regularMarketVolume_element.text
            if regularMarketVolume == "--":
                volume = -1
            else:
                volume = convert_int_string(regularMarketVolume[:-1])
        else:
            # log("error",ticker+" marketCap is None")
            volume = -1
        open_element = soup.find_all('td')[1].find('span')
        if open_element is not None:
            open_text = open_element.text
            if open_text == "--":
                open = -1
            else:
                open = float(open_text)
        else:
            # log("error",ticker+" marketCap is None")
            open = -1
        day_range_element = soup.find_all('td')[0].find('span')
        if day_range_element is not None:
            day_range_text = day_range_element.text
            if day_range_text == "--":
                high = -1
            else:
                high = float(day_range_text)
        else:
            # log("error",ticker+" marketCap is None")
            high = -1
        day_range_element = soup.find_all('td')[4].find('span')
        if day_range_element is not None:
            day_range_text = day_range_element.text
            if day_range_text == "--":
                low = -1
            else:
                low = float(day_range_text)
        else:
            # log("error",ticker+" marketCap is None")
            low = -1

        d = {'date':pd.to_datetime(end.strftime('%Y-%m-%d')), 'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
        df=pd.DataFrame(d,index=[str(end)])
    except Exception as e:
        if ticker=="ADIL":
            log("info",ticker)
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            return pd.DataFrame()
    if ticker=="ANVS":
        log("info",ticker)
    return df

def get_stock_realtime(ticker):
    df = pd.DataFrame()
    try:
        close = float(si.get_live_price(ticker))
        url = f"https://finance.yahoo.com/quote/{ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
        }
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        regularMarketVolume_element = soup.find("fin-streamer", {"data-field": "regularMarketVolume"})
        if regularMarketVolume_element is not None:
            regularMarketVolume = regularMarketVolume_element.text
            if regularMarketVolume == "N/A":
                volume = -1
            else:
                volume = int(regularMarketVolume.replace(",", ""))
        else:
            # log("error",ticker+" marketCap is None")
            volume = -1
        open_element = soup.find("td", {"data-test": "OPEN-value"})
        if open_element is not None:
            open_text = open_element.text
            if open_text == "N/A":
                open = -1
            else:
                open = float(open_text)
        else:
            # log("error",ticker+" marketCap is None")
            open = -1
        day_range_element = soup.find("td", {"data-test": "DAYS_RANGE-value"})
        if day_range_element is not None:
            day_range_text = day_range_element.text
            if day_range_text == "N/A":
                low = -1
                high = -1
            else:
                numbers = day_range_text.split(' - ')
                low = float(numbers[0])
                high = float(numbers[1])
        else:
            # log("error",ticker+" marketCap is None")
            low = -1
            high = -1
        d = {'date':pd.to_datetime(end.strftime('%Y-%m-%d')), 'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
        df=pd.DataFrame(d,index=[str(end)])
    except Exception as e:
        if ticker=="ADIL":
            log("info",ticker)
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            return pd.DataFrame()
    if ticker=="SOPH":
        log("info",ticker)
    return df

def get_stock_history(ticker):
    if ticker=="EH":
        log("debug",ticker)
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start.strftime("%m/%d/%Y"),end.strftime("%m/%d/%Y"),index_as_date=False)
        if(df.tail(1).isnull().values.any()):
            log("error",ticker+" nan")
            df = df[:-1]
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            return pd.DataFrame()
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
                log('critical',"sleep 10 seconds")
                time.sleep(10)
                df = pd.DataFrame()
                break
            if isinstance(stock_chunk_df,int):
                log('critical',"network connection error, terminating process pool...")
                pool.terminate()
                pool.join()
                log('critical',"sleep 10 seconds")
                time.sleep(10)
                df = pd.DataFrame()
                break
            if not stock_chunk_df.empty:
                df = pd.concat([df,stock_chunk_df])
            else:
                log("error","collect_data" + str(func) + " stock_chunk_df empty.")
                df = pd.DataFrame()
                break
    log('info',"collect_data" + str(func) + " stop.")
    return df

def log(type,string):
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.now().strftime("%m%d%Y") + "_collect.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
    
    now = datetime.now()
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
    else:
        log('info','path exists.')
        # delete existing files but keep the last 1 file
        files = os.listdir(path)
        files.sort()
        for file in files[:-1]:
            os.remove(path+file)
            log('info',file+" deleted.")
            
    log('info','collect process started.')

    nasdaq = []
    other = []
    tickers = []

    quote_data_files = os.listdir(quote_data_path)

    # Sort the files by modification time
    quote_data_files.sort(key=lambda x: os.path.getmtime(os.path.join(quote_data_path, x)))

    while len(quote_data_files) == 0:
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

    tickers = quote_data_df[(quote_data_df.marketCap>=marketCapMin) & (quote_data_df.regularMarketPreviousClose>=regularMarketPreviousCloseMin)].ticker.values

    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))

    stock_history_concat_df=collect_data(get_stock_history,cores,10).dropna()
    flag = realtime_required(stock_history_concat_df)

    if (not stock_history_concat_df.empty):
        log('info','stock_history_concat_df is ready.')
        now = datetime.now()
        today15 = now.replace(hour=15,minute=0,second=0,microsecond=0)
        today235959 = now.replace(hour=23,minute=59,second=59,microsecond=0)
        if ((now.weekday() <= 4) & (today15 <= datetime.now() <= today235959) and flag):
            realtime_df=collect_data(get_stock_realtime_xueqiu,cores,10)
            if not realtime_df.empty:
                log('info','realtime_df is ready')
                realtime_df.reset_index(inplace=True,drop=True)
                stock_concat_df = pd.concat([stock_history_concat_df,realtime_df])
                stock_concat_df.reset_index(inplace=True,drop=True)
                # stock_realtime_concat_df = stock_concat_df
                stop_time = datetime.now().strftime("%m%d%Y-%H%M%S")
                try:
                    stock_concat_df.to_feather(path + stop_time + ".feather")
                    log('info',path + stop_time + ".feather" + "saved")
                except Exception as e:
                    log('critical',"to_feather:"+str(e))
            else:
                log('error','realtime_df is empty')
        else:  
            stock_history_concat_df.reset_index(inplace=True,drop=True)
            stop_time = datetime.now().strftime("%m%d%Y-%H%M%S")
            try:
                stock_history_concat_df.to_feather(path + stop_time + ".feather")
                log('info','stock_history_concat_df to_feather saved.')
            except Exception as e:
                log('critical',"to_feather:"+str(e))

        now = datetime.now()
        today8 = now.replace(hour=8,minute=0,second=0,microsecond=0)
        today15 = now.replace(hour=15,minute=0,second=0,microsecond=0)
        while(True):         #get real time stock price
            if ((now.weekday() <= 4) & (today8 <= datetime.now() <= today15) and flag):
                realtime_df=collect_data(get_stock_realtime_xueqiu,cores,10)
                if not realtime_df.empty:
                    log('info','realtime_df is ready')
                    realtime_df.reset_index(inplace=True,drop=True)
                    stock_concat_df = pd.concat([stock_history_concat_df,realtime_df])
                    stock_concat_df.reset_index(inplace=True,drop=True)
                    # stock_realtime_concat_df = stock_concat_df
                    stop_time = datetime.now().strftime("%m%d%Y-%H%M%S")
                    try:
                        stock_concat_df.to_feather(path + stop_time + ".feather")
                        log('info',path + stop_time + ".feather" + "saved")
                    except Exception as e:
                        log('critical',"to_feather:"+str(e))
                else:
                    log('error','realtime_df is empty')
            else:
                break
    else:
        log('error','stock_history_concat_df is empty.')

    log('info','collect process exit.')