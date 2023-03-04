import multiprocessing
import pandas as pd
import datetime
import os
from multiprocessing import Pool
import time
import logging
import math
import numpy as np


def screen(df,lines):
    if len(df)<50:
        return False
    close = df.iloc[-1].close
    # close_Yesterday = df.iloc[-2].close
    volume_10d_avg = df.tail(10).volume.mean()
    turnover_10d_avg = volume_10d_avg*close
    turnover_flag = turnover_10d_avg > 300000
    EMA120 = df.iloc[-1].EMA120
    ema120_flag = (close >= EMA120)
    EMA60 = df.iloc[-1].EMA60
    ema60_flag = (close >= EMA60)
    # EMA120_Yesterday = df.iloc[-2].EMA120
    # ema120_Yesterday_flag = (close_Yesterday >= EMA120_Yesterday)
    change = df.iloc[-1].change >=0.05
    if df.iloc[-1].ticker == "MTC":
        log("info", df.iloc[-1].ticker)

    highest_volume_30days=df.tail(30).volume.max()
    low = df[df.volume==highest_volume_30days].low[0]
    bottom = True if low<=close<=low*1.5 else False

    volume = df.iloc[-1].volume
    volume_spike = True if volume>=volume_10d_avg*2 else False

    UO=df.iloc[-1].UO
    buy = True if UO<=30 else False

    


    if lines == "bottom":
        # DIF_Yesterday = df.iloc[-2].DIF
        # DEA_Yesterday = df.iloc[-2].DEA
        # MACD_Yesterday = DIF_Yesterday>=DEA_Yesterday
        # STD20_Yesterday = df.iloc[-2].STD20
        # STD20_EMA5_Yesterday = df.iloc[-2].STD20_EMA5
        # STD_Yesterday = STD20_Yesterday>=STD20_EMA5_Yesterday
        # yesterday = MACD_Yesterday and STD_Yesterday and ema120_Yesterday_flag
        # DIF = df.iloc[-1].DIF
        # DEA = df.iloc[-1].DEA
        # MACD = DIF>=DEA
        # STD20 = df.iloc[-1].STD20
        # STD20_EMA5 = df.iloc[-1].STD20_EMA5
        # STD = STD20>=STD20_EMA5
        DIFF = df.iloc[-1].DIFF
        DIFF_EMA20 = df.iloc[-1].DIFF_EMA20
        max_volume_low_60days = df.iloc[-1].max_volume_low_60days
        gain_rate = DIFF/max_volume_low_60days
        flag = (DIFF>=DIFF_EMA20) and (DIFF>=0) and (gain_rate < 0.5)
        today = flag and turnover_flag and ema60_flag and change
        if today:
            return True
        else:
            return False
    # elif lines == "Volatile":

    #     close_max_30days = max(df.tail(30)['close'])
    #     close_max_100days = max(df.tail(100)['close'])

    #     new_high = (close_max_30days == close_max_100days) and (close < close_max_30days)

    #     if(len(df)<=3):
    #         return False
    #     last_high = df.iloc[-2].high
    #     last_2_high = df.iloc[-3].high
        
    #     strong = (close >= last_high) and ema120_flag and (close_Yesterday<=last_2_high)
        

    #     if(new_high and turnover_flag and strong and change):
    #         return True
    #     else:
    #         return False

    return False

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
        # if (not df.empty) and df.index[-1]!=datetime.date.today():
        #     log("error",ticker+" date error.")
        #     continue
        try:
            Volatile = screen(df,"bottom")
        except Exception as e:
            log('critical',str(e))
            return pd.DataFrame()
        if(Volatile):
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
    # df=df[df.date!=str(datetime.date.today())]
    tickers = df.ticker.unique()
    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    pool=Pool(cores)

    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        # sharesOutstanding_chunk_df = sharesOutstanding_df[sharesOutstanding_df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run, args=(ticker_chunk_df,))
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
    logpath = '//jack-nas.home/Work/Python/'
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
    processed_data_path="//jack-nas.home/Work/Python/ProcessedData/"
    raw_data_path = '//jack-nas.home/Work/Python/RawData/'

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