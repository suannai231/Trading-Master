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
import statistics

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'T':1000000000000, 'k':1000, 'm':1000000, 'b':1000000000, 't':1000000000000}

def string_to_int(string):
    if string[-1].isdigit(): # check if no suffix
        return int(string)
    mult = multipliers[string[-1]] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return int(float(string[:-1]) * mult)

# def get_float(ticker):
#     float = 0
#     while(float == 0):
#         try:
#             df = si.get_stats(ticker)
#             float = string_to_int(df[df.Attribute=="Float 8"].iloc[-1].Value)
#         except Exception as e:
#             log("critical",ticker + " get_stats exception:" + str(e))
#             if str(e).startswith('list index out of range'):
#                 log("critical","sleep 60 seconds.")
#                 time.sleep(60)
#             else:
#                 return -1
#     return float

def screen(df,lines):
    close = df.iloc[-1]['close']
    ema20 = df.iloc[-1]['EMA20']
    change = df.iloc[-1]['change']

    if lines=="Close to EMA20":
        if(ema20 <= close <= ema20*1.15):
            return True
        else:
            return False
    elif lines == "Above EMA20":
        high = df.iloc[-1]['high']
        if(high < ema20):
            return False
        else:
            return True
    elif lines=="change":
        if change >= 0:
            return True
        else:
            return False
    # elif lines=="OBV":
    #     if(len(df)<60):
    #         return False
    #     last_60days_df = df.iloc[len(df)-60:]
    #     obv_max = max(last_60days_df.OBV)
    #     OBV = df.iloc[-1]['OBV']
        # OBV_SMA20 = df.iloc[-1]['OBV_SMA20']
        # OBV_SMA60 = df.iloc[-1]['OBV_SMA60']
        # last_20days_df = df.iloc[len(df)-20:]
        # DIS=statistics.pstdev(last_20days_df.OBV)
        # UPPER=OBV_SMA20+(2*DIS)
        # LOWER=OBV_SMA20-(2*DIS)

        # ticker = df.iloc[-1]['ticker']
        # if ticker=="PHIO":
        #     log("info",ticker)

        # if OBV == obv_max:
        #     return True
        # else:
        #     return False
    elif lines=="turnover":
        turnover = df.iloc[-1]['volume']*close
        if(turnover >= 200000):
            return True
        else:
            return False
    elif lines=="above_high_vol_low_20_days":
        if(len(df)<20):
            return False
        Last_20days_df = df.iloc[len(df)-20:]
        vol_max = max(Last_20days_df.volume.dropna(axis=0))
        low = Last_20days_df.loc[Last_20days_df.volume==vol_max,'low'][0]
        close = Last_20days_df.iloc[-1]['close']
        if close > low:
            return True
        else:
            return False
    # elif lines=="buy":
    #     open = df.iloc[-1]['open']
    #     close = df.iloc[-1]['close']
    #     change = abs((close - open)/open)
    #     if change <= 0.05:
    #         return True
    #     else:
    #         return False
    # elif lines=="podifan":
    #     if(len(df)<20):
    #         return False
    #     min_low = min(df.low)
    #     pre_low = df.iloc[-2]['low']
    #     # pre_close = df.iloc[-2]['close']
    #     close = df.iloc[-1]['close']
    #     if (pre_low == min_low) and (close > pre_low):
    #         return True
    #     else:
    #         return False
    elif lines=="60day High":
        if(len(df)<60):
            return False
        Last_60days_df = df.iloc[len(df)-60:]  
        high = max(Last_60days_df.close)
        if df.iloc[-1].close == high:
            return True
        else:
            return False
    elif lines=="price limit":
        low = min(df.close)
        if(close>low*2.5):
            return False
        else:
            return True
    elif lines=="AMP":
        open = df.iloc[-1].open
        close = df.iloc[-1].close
        amp = abs((close-open)/open)
        if amp <= 0.05:
            return True
        else:
            return False
    elif lines=="EMA60 60days High":
        if(len(df)<60):
            return False
        Last_60days_df = df.iloc[len(df)-60:]
        EMA60_MAX = max(Last_60days_df.EMA60)
        EMA60 = df.iloc[-1].EMA60
        if(EMA60==EMA60_MAX):
            return True
        else:
            return False
    elif lines=="VOL_EMA20 20days Low":
        if(len(df)<20):
            return False
        Last_20days_df = df.iloc[len(df)-20:]
        VOL_EMA20_MIN = min(Last_20days_df.VOL_EMA20)
        VOL_EMA20 = df.iloc[-1].VOL_EMA20
        if(VOL_EMA20==VOL_EMA20_MIN):
            return True
        else:
            return False
    # elif lines=="active":
    #     Last_20days_df = df.iloc[len(df)-20:]
    #     vol_max = max(Last_20days_df.volume)
    #     ticker = df.iloc[-1]['ticker']
    #     float = get_float(ticker)
    #     if float==-1:
    #         return -1
    #     max_turnover = vol_max/float
    #     vol = df.iloc[-1]['volume']
    #     turnover = vol/float
    #     if ((max_turnover >= 0.07) and (turnover >= 0.05)):
    #         return True
    return False

def run_last_20_days(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        df_len = len(ticker_df)
        if df_len < 20:
            continue
        _60day_High = False
        for i in range(df_len-19, df_len+1):
            slice_df = ticker_df.iloc[0:i]
            if slice_df.empty:
                log("error","slice_df is empty")

            if screen(slice_df,'60day High'):
                _60day_High = True
            Close_to_EMA20 = screen(slice_df,'Close to EMA20')
            # above_high_vol_low_20_days = screen(slice_df,"above_high_vol_low_20_days")
            turnover = screen(slice_df,"turnover")
            # price_limit = screen(slice_df,"price limit")
            # AMP = screen(slice_df,"AMP")
            # change = screen(slice_df,"change")
            # EMA60_60days_High = screen(slice_df,"EMA20 60days High")
            # VOL_EMA10_20days_Low = screen(slice_df,"VOL_EMA5 20days Low")

            if((i==df_len) and _60day_High and Close_to_EMA20 and turnover):
                today_df = slice_df.iloc[[-1]]
                return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
                log("info",ticker)

    return return_ticker_chunk_df

def run(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        if(df.iloc[-1].ticker=="ATXI"):
            log("info","")
        # Close_to_EMA20 = screen(df,'Close to EMA20')
        # above_high_vol_low_20_days = screen(df,"above_high_vol_low_20_days")
        turnover = screen(df,"turnover")
        # price_limit = screen(df,"price limit")
        # AMP = screen(df,"AMP")
        # change = screen(df,"change")
        # EMA60_60days_High = screen(df,"EMA60 60days High")
        # VOL_EMA20_20days_Low = screen(df,"VOL_EMA20 20days Low")
        _60day_High = screen(df,"60day High")


        if(turnover and _60day_High):
            today_df = df.iloc[[-1]]
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
            log("info",ticker)
        
    return return_ticker_chunk_df

def screen_data():
    log('info',"screen_data start.")

    processed_data_files = os.listdir(processed_data_path)
    if len(processed_data_files) == 0:
        log('warning',"processed data not ready, sleep 10 seconds...")
        time.sleep(10)
        return

    screened_data_files = os.listdir(screened_data_path)
    processed_data_files_str = processed_data_files[-1] + '.txt'
    if processed_data_files_str in screened_data_files:
        log('warning',"error: " + processed_data_files_str + " existed, sleep 10 seconds...")
        time.sleep(10)
        return

    log('info',"processing "+processed_data_files[-1])
    try:
        time.sleep(1)
        df = pd.read_feather(processed_data_path + processed_data_files[-1])
        log('info',processed_data_path + processed_data_files[-1] + " loaded.")
    except Exception as e:
        log('critical',str(e))
        return

    tickers = df.ticker.unique()
    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    pool=Pool(cores)

    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        # sharesOutstanding_chunk_df = sharesOutstanding_df[sharesOutstanding_df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run_last_20_days, args=(ticker_chunk_df,))
        async_results.append(async_result)
    pool.close()
    log('info',"process pool start.")

    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = pd.concat([df,result])
    
    if(not df.empty):
        df.reset_index(drop=False,inplace=True)
        try:
            df.to_csv(screened_data_path + processed_data_files[-1] + '.csv')
            df.ticker.to_csv(screened_data_path + processed_data_files[-1] + '.txt',header=False, index=False)
            log('info',screened_data_path + processed_data_files[-1] +" is saved.")
        except Exception as e:
            log('critical',"df to_csv:"+str(e))
    else:
        log('error',"df empty")

    log('info',"screen_data stop.")

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def log(type,string):
    logpath = 'C:/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_screen.log"
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

    screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"
    processed_data_path="C:/Python/ProcessedData/"
    raw_data_path = 'C:/Python/RawData/'

    log('info',"screen_data process start.")

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    screen_data()

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)):
        screen_data()

    now = datetime.datetime.now()
    stop_time = now.strftime("%m%d%Y-%H%M%S")
    log('info',"screen_data process exit.")