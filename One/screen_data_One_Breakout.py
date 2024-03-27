import multiprocessing
import pandas as pd
import datetime
import os
from multiprocessing import Pool
import time
import logging
import math
import numpy as np
# from playsound import playsound

def screen(df):
    close = df.iloc[-1].close
    volume_10d_avg = df.tail(10).volume.mean()
    turnover_10d_avg = volume_10d_avg*close
    turnover_flag = turnover_10d_avg > 200000
    change = df.iloc[-1].change >=0.01

    volume_today = df.iloc[-1].volume
    volume_yesterday = df.iloc[-2].volume

    # EMA5
    EMA5 = df.iloc[-1].EMA5

    max_volume_date_120 = df.tail(120).volume.idxmax()
    max_volume_high_120 = df.loc[max_volume_date_120].high
    max_volume_low_120 = df.loc[max_volume_date_120].low
    mid_120 = (max_volume_high_120 + max_volume_low_120) / 2

    max_volume_date_60 = df.tail(60).volume.idxmax()
    max_volume_high_60 = df.loc[max_volume_date_60].high
    max_volume_low_60 = df.loc[max_volume_date_60].low
    mid_60 = (max_volume_high_60 + max_volume_low_60) / 2

    max_volume_date_20 = df.tail(20).volume.idxmax()
    max_volume_high_20 = df.loc[max_volume_date_20].high
    max_volume_low_20 = df.loc[max_volume_date_20].low
    mid_20 = (max_volume_high_20 + max_volume_low_20) / 2

    close_60days = df.tail(60).close.max()

    flag = close>=EMA5 and turnover_flag and close >= mid_120 and close >= mid_60 and close >= mid_20 and close >= close_60days and change and volume_today >= volume_yesterday

    if df.iloc[-1].ticker == 'WETG':
        log("info", "WETG")

    if flag == True:
        log("info", df.iloc[-1].ticker)
        log("info", "close: " + str(close))
        log("info", "volume_10d_avg: " + str(volume_10d_avg))
        log("info", "turnover_10d_avg: " + str(turnover_10d_avg))
        log("info", "change: " + str(df.iloc[-1].change))
        log("info", "EMA5: " + str(EMA5))
        log("info", "max_volume_date_120: " + str(max_volume_date_120))
        log("info", "max_volume_high_120: " + str(max_volume_high_120))
        log("info", "max_volume_low_120: " + str(max_volume_low_120))
        log("info", "mid_120: " + str(mid_120))
        log("info", "max_volume_date_60: " + str(max_volume_date_60))
        log("info", "max_volume_high_60: " + str(max_volume_high_60))
        log("info", "max_volume_low_60: " + str(max_volume_low_60))
        log("info", "mid_60: " + str(mid_60))
        log("info", "max_volume_date_20: " + str(max_volume_date_20))
        log("info", "max_volume_high_20: " + str(max_volume_high_20))
        log("info", "max_volume_low_20: " + str(max_volume_low_20))
        log("info", "mid_20: " + str(mid_20))
        log("info", "close_60days: " + str(close_60days))

    if flag:
        return True
    else:
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
            Volatile = screen(df)
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
        log('warning',"processed data not ready, sleep 1 second...")
        time.sleep(1)
        return

    screened_data_files = os.listdir(screened_data_path)

    # Sort the files by modification time
    screened_data_files.sort(key=lambda x: os.path.getmtime(os.path.join(screened_data_path, x)))
    
    if processed_data_files[-1] in screened_data_files:
        log('warning',"warning: " + processed_data_files[-1] + " existed, sleep 10 second...")
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
            df.to_feather(screened_data_path + processed_data_files[-1])
            # df.to_csv(screened_text_path + processed_data_files[-1] + '.csv')
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
        directory_path = os.getcwd()
        # file_path = directory_path+'\Sounds\PriceNotice.wav'
        # try:
        #     playsound(file_path)
        # except Exception as e:
        #     logging.info(log_time+":"+str(e))
        logging.error(log_time+":"+string)
    elif type=='critical':
        directory_path = os.getcwd()
        # file_path = directory_path+'\Sounds\PriceNotice.wav'
        # try:
        #     playsound(file_path)
        # except Exception as e:
        #     logging.info(log_time+":"+str(e))
        logging.critical(log_time+":"+string)

if __name__ == '__main__':

    screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"
    screened_text_path="//jack-nas.home/Work/Python/ScreenedText/"
    processed_data_path="//jack-nas.home/Work/Python/ProcessedData/"
    raw_data_path = '//jack-nas.home/Work/Python/RawData/'

    log('info',"screen_data process start.")

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)
    else:
        log('info','path exists.')
        # delete existing files but keep the last 1 file
        files = os.listdir(screened_data_path)
        files.sort()
        for file in files[:-1]:
            os.remove(screened_data_path+file)
            log('info',file+" deleted.")

    # isPathExists = os.path.exists(screened_text_path)
    # if not isPathExists:
    #     os.makedirs(screened_text_path)
    # else:
    #     log('info','path exists.')
    #     # delete existing files but keep the last 1 file
    #     files = os.listdir(screened_text_path)
    #     files.sort()
    #     for file in files[:-1]:
    #         os.remove(screened_text_path+file)
    #         log('info',file+" deleted.")

    # isPathExists = os.path.exists(screened_text_path)
    # if not isPathExists:
    #     os.makedirs(screened_text_path)

    screen_data()

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)):
        screen_data()

    now = datetime.datetime.now()
    stop_time = now.strftime("%m%d%Y-%H%M%S")
    log('info',"screen_data process exit.")