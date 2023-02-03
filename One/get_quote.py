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
import yfinance as yf
import requests
from bs4 import BeautifulSoup

def convert_to_num(s):
    if s[-1] == 'M':
        return float(s[:-1]) * 1000000
    elif s[-1] == 'B':
        return float(s[:-1]) * 1000000000
    elif s[-1] == 'T':
        return float(s[:-1]) * 1000000000000
    else:
        raise ValueError("Input string must end with 'M', 'B', or 'T'")

def get_quote_data(ticker):
    df = pd.DataFrame()
    try:
        dict = si.get_quote_data(ticker)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            log("critical",ticker+" "+str(e))
            return -1
        elif str(e).startswith('Invalid response from server') | str(e).startswith("IndexError('list index out of range')"):
            return pd.DataFrame()
        else:
            log("error",ticker+" "+str(e))
            return pd.DataFrame()

    # dict['ticker'] = ticker
    if 'marketCap' in dict.keys():
        marketCap = dict['marketCap']
    else:
        url = f"https://finance.yahoo.com/quote/{ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
        }
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        market_cap_element = soup.find("td", {"data-test": "MARKET_CAP-value"})
        if market_cap_element is not None:
            marketCap = market_cap_element.text
            if marketCap == "N/A":
                log("error",ticker+" marketCap is N/A")
                return pd.DataFrame()
        if (not isinstance(marketCap,float)) or (stock.fast_info['market_cap'] is None):
            if(ticker=="GNS"):
                log("info",ticker)
            log("error",ticker+" stock.fast_info['market_cap'] error")
            return pd.DataFrame()
    if 'regularMarketPreviousClose' in dict.keys():
        regularMarketPreviousClose = dict['regularMarketPreviousClose']
    else:
        log("error",ticker+" regularMarketPreviousClose is not available")
        return pd.DataFrame()

    d={'ticker':[ticker],'marketCap':[marketCap],'regularMarketPreviousClose':[regularMarketPreviousClose]}
    df = pd.DataFrame(d)
    df = df.set_index('ticker')
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
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_quote.log"
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
    path = '//jack-nas.home/Work/Python/QuoteData/'
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    log('info','collect process started.')

    nasdaq = []
    other = []
    tickers = []

    while((len(nasdaq)==0) or (len(other)==0)):
        try:
            nasdaq = si.tickers_nasdaq()
            other = si.tickers_other()
        except Exception as e:
            log('critical','get tickers exception:'+str(e))
            continue
        tickers = nasdaq + other

    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/(cores))))

    stock_quote_data_df=collect_data(get_quote_data,cores,10)
    
    if (not stock_quote_data_df.empty):
        log('info','stock_history_concat_df is ready.')

        stock_quote_data_df.reset_index(inplace=True,drop=False)
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        try:
            stock_quote_data_df.to_feather(path + stop_time + ".feather")
            log('info','stock_quote_data_df to_feather saved.')
        except Exception as e:
            log('critical',"to_feather:"+str(e))
    else:
        log('error','stock_quote_data_df is empty.')

    log('info','collect process exit.')