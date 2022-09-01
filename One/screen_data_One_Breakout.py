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
# from yahoo_fin import stock_info as si

def high_vol_in_last_20_days(df,sharesOutstanding):
    if(len(df)<20):
        return False
    Last_20days_df = df.iloc[len(df)-20:]
    vol_max = max(Last_20days_df.volume)
    vol_max_turnover_rate = vol_max/sharesOutstanding
    if vol_max_turnover_rate>0.05:
        low = Last_20days_df.loc[Last_20days_df.volume==vol_max,'low'][0]
        close = Last_20days_df.iloc[-1]['close']
        if(close>=low):
            return True
    return False

def screen(df,lines):
    close = df.iloc[-1]['close']
    ema20 = df.iloc[-1]['EMA20']
    change = df.iloc[-1]['change']
    turnover = df.iloc[-1]['volume']*close

    if lines=="Close to EMA20":
        if(close<=ema20*1.2):
            return True
        else:
            return False
    elif lines=="change":
        if change >=0:
            return True
        else:
            return False
    elif lines=="OBV":
        if(len(df)<60):
            return False
        last_60days_df = df.iloc[len(df)-60:]
        obv_max = max(last_60days_df.OBV)
        OBV = df.iloc[-1]['OBV']
        if OBV == obv_max:
            return True
        else:
            return False
    elif lines=="turnover":
        if(turnover >= 1000000):
            return True
        else:
            return False
    return False

def run(ticker_chunk_df,sharesOutstanding_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        if ticker not in sharesOutstanding_chunk_df.ticker.values:
            continue
        sharesOutstanding_df = sharesOutstanding_chunk_df[sharesOutstanding_chunk_df.ticker==ticker]
        sharesOutstanding = sharesOutstanding_df.iloc[-1]['sharesOutstanding']
        # if(ticker=="TMC"):
        #     logging.info("TMC")

        # Close_to_EMA20 = screen(df,"Close to EMA20")
        change  = screen(df,"change")
        OBV = screen(df,"OBV")
        turnover = screen(df,'turnover')

        if(high_vol_in_last_20_days(df,sharesOutstanding) & OBV & change & turnover):
            today_df = df.iloc[[-1]]
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
        
    return return_ticker_chunk_df

# def save(return_df,async_results,processed_data_file):
#     df = pd.DataFrame()
#     for async_result in async_results:
#         result = async_result.get()
#         if not result.empty:
#             df = pd.concat([df,result])
    
#     if(not df.empty):
#         df.reset_index(drop=False,inplace=True)
#         try:
#             df.to_csv(screened_data_path + processed_data_file + '.csv')
#             end = datetime.date.today()
#             df = df.loc[df.date==str(end),'ticker']
#             df.to_csv(screened_data_path + processed_data_file + '.txt',header=False, index=False)
#             return_df = pd.concat([return_df,df])
#         except Exception as e:
#             logging.critical("return_df to_csv:"+str(e))
#     else:
#         logging.error("return_df empty")
#     return return_df

def screen_data():
    now = datetime.datetime.now()
    start_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("screen_data start time:" + start_time)

    processed_data_files = os.listdir(processed_data_path)
    if len(processed_data_files) == 0:
        logging.warning("processed data not ready, sleep 10 seconds...")
        time.sleep(10)
        return

    screened_data_files = os.listdir(screened_data_path)
    processed_data_files_str = processed_data_files[-1] + '.txt'
    if processed_data_files_str in screened_data_files:
        logging.warning("error: " + processed_data_files_str + " existed, sleep 10 seconds...")
        time.sleep(10)
        return

    logging.info("processing "+processed_data_files[-1])

    try:
        time.sleep(1)
        df = pd.read_feather(processed_data_path + processed_data_files[-1])
    except Exception as e:
        logging.critical(str(e))
        return

    tickers = df.ticker.unique()
    cores = int(multiprocessing.cpu_count()/4)
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    pool=Pool(cores)

    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        sharesOutstanding_chunk_df = sharesOutstanding_df[sharesOutstanding_df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run, args=(ticker_chunk_df,sharesOutstanding_chunk_df))
        async_results.append(async_result)
    pool.close()

    # return_df = pd.DataFrame()

    # return_df = save(return_df,async_results_AMP,processed_data_files[-1]+"_AMP")
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = pd.concat([df,result])
    
    if(not df.empty):
        df.reset_index(drop=False,inplace=True)
        try:
            df.to_csv(screened_data_path + processed_data_files[-1] + '.csv')
            # end = datetime.date.today()
            # df = df.loc[df.date==str(end),'ticker']
            df.ticker.to_csv(screened_data_path + processed_data_files[-1] + '.txt',header=False, index=False)
            # return_df = pd.concat([return_df,df])
        except Exception as e:
            logging.critical("return_df to_csv:"+str(e))
    else:
        logging.error("return_df empty")
    # return return_df

    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    logging.info("screen_data stop time:" +stop_time)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == '__main__':

    screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"
    processed_data_path="C:/Python/ProcessedData/"
    raw_data_path = 'C:/Python/RawData/'

    logpath = 'C:/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_screen.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    now = datetime.datetime.now()
    start_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("screen_data process start time:" + start_time)

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    sharesOutstanding_path = 'C:/Python/sharesOutstanding/'
    sharesOutstanding_df = pd.DataFrame()
    while(sharesOutstanding_df.empty):
        today_date = datetime.datetime.now().strftime("%m%d%Y")
        file_name = today_date + "_sharesOutstanding.feather"
        full_path_name = sharesOutstanding_path + today_date + "_sharesOutstanding.feather"
        files = os.listdir(sharesOutstanding_path)
        if file_name not in files:
            logging.warning("sharesOutstanding_file is not ready, sleep 10 seconds...")
            time.sleep(10)
            continue
        try:
            sharesOutstanding_df = pd.read_feather(full_path_name)
            logging.info('sharesOutstanding_df is ready')
        except Exception as e:
            logging.critical('sharesOutstanding_df read_feather:'+str(e))
            continue

    screen_data()

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)):
        screen_data()

    now = datetime.datetime.now()
    stop_time = now.strftime("%m%d%Y-%H%M%S")
    logging.info("screen_data process stop time:" + start_time)