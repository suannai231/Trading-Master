import multiprocessing
import pandas as pd
import datetime
import os
import sys
from multiprocessing import Pool
import numpy as np
# from datetime import timedelta
import time
import logging
import math
from yahoo_fin import stock_info as si

screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"
processed_data_path="C:/Python/ProcessedData/"
raw_data_path = 'C:/Python/RawData/'
base_days = 13

def high_vol_in_last_20_days(df,Float):
    if(len(df)<20):
        return False
    Last_20days_df = df.iloc[len(df)-20:]
    vol_max = max(Last_20days_df.volume)
    vol_max_turnover_rate = vol_max/Float
    if vol_max_turnover_rate>0.05:
        low = Last_20days_df.loc[Last_20days_df.volume==vol_max,'low'][0]
        close = Last_20days_df.iloc[-1]['close']
        if(close>=low):
            return True
    return False

def screen(df,lines):
    close = df.iloc[-1]['close']
    ema20 = df.iloc[-1]['EMA20']

    if lines=="Close to EMA20":
        if(close<=ema20*1.2):
            return True
        else:
            return False
    elif lines=="OBV":
        if(len(df)<60):
            return False
        last_60days_df = df.iloc[len(df)-60:]
        obv_max = max(last_60days_df.OBV)
        OBV = df.iloc[-1]['OBV']
        if OBV >= obv_max:
            return True
        else:
            return False
    return False

def run(ticker_chunk_df,Float_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        if ticker not in Float_chunk_df.ticker.values:
            continue
        Float_df = Float_chunk_df[Float_chunk_df.ticker==ticker]
        Float = Float_df.iloc[-1]['Float']
        if(ticker=="VLCN"):
            logging.info("VLCN")

        Close_to_EMA20 = screen(df,"Close to EMA20")
        OBV = screen(df,"OBV")

        if(high_vol_in_last_20_days(df,Float) & Close_to_EMA20 & OBV):
            today_df = df.iloc[[-1]]
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
        
    return return_ticker_chunk_df

def save(return_df,async_results,processed_data_file):
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = pd.concat([df,result])
    
    if(not df.empty):
        df.reset_index(drop=False,inplace=True)
        try:
            df.to_csv(screened_data_path + processed_data_file + '.csv')
            end = datetime.date.today()
            df = df.loc[df.date==str(end),'ticker']
            df.to_csv(screened_data_path + processed_data_file + '.txt',header=False, index=False)
            return_df = pd.concat([return_df,df])
        except Exception as e:
            logging.critical("return_df to_csv:"+str(e))
    else:
        logging.error("return_df empty")
    return return_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == '__main__':
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_screen.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    now = datetime.datetime.now()
    start_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("screen_data process start time:" + start_time)

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    today830am = now.replace(hour=8,minute=30,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    Float_df = pd.DataFrame()
    while(Float_df.empty):
        Float_path = 'C:/Python/Float/'
        today_date = datetime.datetime.now().strftime("%m%d%Y")
        file_name = today_date + "_Float.feather"
        full_path_name = Float_path + today_date + "_Float.feather"
        files = os.listdir(Float_path)
        if file_name not in files:
            logging.warning("Float_file is not ready, sleep 10 seconds...")
            time.sleep(10)
            continue
        try:
            Float_df = pd.read_feather(full_path_name)
            logging.info('Float_df is ready')
        except Exception as e:
            logging.critical('Float_df read_feather:'+str(e))
            continue

    # sharesOutstanding_df = pd.DataFrame()
    # while(sharesOutstanding_df.empty):
    #     sharesOutstanding_path = 'C:/Python/sharesOutstanding/'
    #     today_date = datetime.datetime.now().strftime("%m%d%Y")
    #     file_name = today_date + "_sharesOutstanding.feather"
    #     full_path_name = sharesOutstanding_path + today_date + "_sharesOutstanding.feather"
    #     files = os.listdir(sharesOutstanding_path)
    #     if file_name not in files:
    #         logging.warning("sharesOutstanding_file is not ready, sleep 10 seconds...")
    #         time.sleep(10)
    #         continue
    #     try:
    #         sharesOutstanding_df = pd.read_feather(full_path_name)
    #         logging.info('sharesOutstanding_df is ready')
    #     except Exception as e:
    #         logging.critical('sharesOutstanding_df read_feather:'+str(e))
    #         continue

    while((now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):
    # while(True):
        now = datetime.datetime.now()

        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)

        processed_data_files = os.listdir(processed_data_path)
        if len(processed_data_files) == 0:
            logging.warning("processed data not ready, sleep 10 seconds...")
            time.sleep(10)
            continue

        screened_data_files = os.listdir(screened_data_path)
        processed_data_files_str = processed_data_files[-1] + '_AMP.txt'
        if processed_data_files_str in screened_data_files:
            logging.warning("error: " + processed_data_files_str + " existed, sleep 10 seconds...")
            time.sleep(10)
            continue

        logging.info("processing "+processed_data_files[-1])

        try:
            time.sleep(1)
            df = pd.read_feather(processed_data_path + processed_data_files[-1])
        except Exception as e:
            logging.critical(str(e))
            continue

        tickers = df.ticker.unique()
        cores = int(multiprocessing.cpu_count()/2)
        ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
        pool=Pool(cores)

        async_results_AMP = []
        for ticker_chunk in ticker_chunk_list:
            ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
            # sharesOutstanding_chunk_df = sharesOutstanding_df[sharesOutstanding_df['ticker'].isin(ticker_chunk)]
            Float_chunk_df = Float_df[Float_df['ticker'].isin(ticker_chunk)]
            async_result_AMP = pool.apply_async(run, args=(ticker_chunk_df,Float_chunk_df))
            async_results_AMP.append(async_result_AMP)
        pool.close()
        del(df)
        return_df = pd.DataFrame()

        return_df = save(return_df,async_results_AMP,processed_data_files[-1]+"_AMP")

        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("stop time:" +stop_time)