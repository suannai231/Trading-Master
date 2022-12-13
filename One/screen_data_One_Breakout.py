import multiprocessing
import pandas as pd
import datetime
import os
from multiprocessing import Pool
import time
import logging
import math


def screen(df,lines):
    if lines == "Volatile":
        close_max_10days = max(df.tail(10)['close'])
        close_max_120days = max(df.tail(120)['close'])
        volume = df.tail(10).volume.mean()
        close = df.iloc[-1].close
        turnover = volume*close
        turnover_flag = turnover > 1000000
        ema250 = df.iloc[-1].EMA250
        ema250_flag = ema250*1.2 >= close >= ema250
        if df.iloc[-1].ticker == "AGRIW":
            log("info", "AGRIW")
        if((close_max_10days == close_max_120days) and turnover_flag and ema250_flag):
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

        Volatile = screen(df,"Volatile")

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

    tickers = df.ticker.unique()
    cores = int(multiprocessing.cpu_count()/2)
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