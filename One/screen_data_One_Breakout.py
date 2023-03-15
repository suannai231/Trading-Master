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
    if len(df)<120:
        return False
    close = df.iloc[-1].close
    # close_Yesterday = df.iloc[-2].close
    volume_60d_avg = df.tail(60).volume.mean()
    turnover_10d_avg = volume_60d_avg*close
    turnover_flag = turnover_10d_avg > 100000
    # EMA120 = df.iloc[-1].EMA120
    # ema120_flag = (close >= EMA120)
    EMA60 = df.iloc[-1].EMA60
    ema60_flag = (close >= EMA60)
    # EMA120_Yesterday = df.iloc[-2].EMA120
    # ema120_Yesterday_flag = (close_Yesterday >= EMA120_Yesterday)
    change = df.iloc[-1].change >=-0.03

    # highest_volume_30days=df.tail(30).volume.max()
    # low = df[df.volume==highest_volume_30days].low[0]
    # bottom = True if low<=close<=low*1.5 else False

    volume = df.iloc[-1].volume
    volume_flag = True if volume>=volume_60d_avg*2 else False

    # UO=df.iloc[-1].UO
    # buy = True if UO<=30 else False

    if lines == "2060":
        # DIFF20L_Y = df.iloc[-2].DIFF20L
        # DIFF60L_Y = df.iloc[-2].DIFF60L
        DIFF120L_Y = df.iloc[-2].DIFF120L
        DIFF120L_10H_Y = df.iloc[-2].DIFF120L_10H
        DIFF120L_60H_Y = df.iloc[-2].DIFF120L_60H
        # DIFF250L_Y = df.iloc[-2].DIFF250L
        flag_Y = DIFF120L_Y==DIFF120L_10H_Y==DIFF120L_60H_Y and DIFF120L_60H_Y>=0 or (DIFF120L_Y==DIFF120L_10H_Y and np.isnan(DIFF120L_60H_Y) and DIFF120L_10H_Y>=0)

        # DIFF20L = df.iloc[-1].DIFF20L
        # DIFF60L = df.iloc[-1].DIFF60L
        DIFF120L = df.iloc[-1].DIFF120L
        # DIFF250L = df.iloc[-1].DIFF250L
        DIFF120L_10H = df.iloc[-1].DIFF120L_10H
        DIFF120L_60H = df.iloc[-1].DIFF120L_60H
        flag = DIFF120L==DIFF120L_10H==DIFF120L_60H and DIFF120L_60H>=0 or (DIFF120L==DIFF120L_10H and np.isnan(DIFF120L_60H) and DIFF120L_10H>=0)
        today = flag and not flag_Y and turnover_flag and change and volume_flag
        if df.iloc[-1].ticker == "SOPH":
            log("info", df.iloc[-1].ticker)
        if today:
            return True
        else:
            return False
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
            Volatile = screen(df,"2060")
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