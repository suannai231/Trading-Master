import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import time
import logging
import math
import numpy as np

length = 60

def cal_basics(df,ticker_history_df):
    if ticker_history_df.empty:
        df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
        # EMA20
        df['EMA20'] = df['close'].ewm(span = 20, adjust = False).mean()
        # EMA60
        df['EMA60'] = df['close'].ewm(span = 60, adjust = False).mean()
        # DIS
        df['DIS'] = df['EMA20'] - df['EMA60']
        # DIS_EMA5
        df['DIS_EMA5'] = df['DIS'].ewm(span = 5, adjust = False).mean()


        return df
    else:
        if ticker_history_df.iloc[-1].date==df.iloc[-1].date:
            index = len(ticker_history_df)-1

            ticker_history_df.loc[index,'open']=df.iloc[-1].open
            ticker_history_df.loc[index,'high']=df.iloc[-1].high
            ticker_history_df.loc[index,'low']=df.iloc[-1].low
            ticker_history_df.loc[index,'close']=df.iloc[-1].close
            ticker_history_df.loc[index,'adjclose']=df.iloc[-1].adjclose
            ticker_history_df.loc[index,'volume']=df.iloc[-1].volume
            ticker_history_df.loc[index,'change']=(df.iloc[-1].close - df.iloc[-2].close)/df.iloc[-2].close

            ticker_history_df.loc[index,'EMA20'] = ticker_history_df.close.ewm(span = 20, adjust = False).mean().iloc[-1]
            ticker_history_df.loc[index,'EMA60'] = ticker_history_df.close.ewm(span = 60, adjust = False).mean().iloc[-1]
            ticker_history_df.loc[index,'DIS'] = ticker_history_df.EMA20 - ticker_history_df.EMA60
            ticker_history_df.loc[index,'DIS_EMA5'] = ticker_history_df.DIS.ewm(span = 5, adjust = False).mean().iloc[-1]

            return ticker_history_df
        else:
            log("error",df.iloc[-1].ticker+" date mismatch")
            return pd.DataFrame()

def run(ticker_chunk_df,ticker_chunk_history_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        ticker_history_df = pd.DataFrame()
        if not ticker_chunk_history_df.empty:
            ticker_history_df = ticker_chunk_history_df[ticker_chunk_history_df.ticker==ticker].reset_index(drop=True)
        if len(df)<length:
            log("warning",df.iloc[-1].ticker+f" len < {length}")
            continue
        df = cal_basics(df,ticker_history_df)

        if not df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,df],ignore_index=True)
        else:
            return df
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_data(history_df):
    log('info',"process_data start.")
    global global_df
    raw_data_files = os.listdir(raw_data_path)

    # Sort the files by modification time
    raw_data_files.sort(key=lambda x: os.path.getmtime(os.path.join(raw_data_path, x)))

    if len(raw_data_files) == 0:
        # log('warning',"raw data not ready, sleep 1 second...")
        time.sleep(1)
        return pd.DataFrame()
    # date_time = datetime.datetime.now() 
    # datetime_str = date_time.strftime("%m%d%Y-%H")
    # processed_data_file = datetime_str + '.feather'

    processed_data_files = os.listdir(processed_data_path)
    if raw_data_files[-1] in processed_data_files:
        log('warning',"warning: " + raw_data_files[-1] + " existed, sleep 10 seconds...")
        time.sleep(10)
        return pd.DataFrame()
    
    log('info',"processing "+raw_data_files[-1])
    try:
        time.sleep(1)
        df = pd.read_feather(raw_data_path + raw_data_files[-1])
        log('info',raw_data_path + raw_data_files[-1]+" loaded.")
    except Exception as e:
        log('critical',"read_feather:"+str(e))
        return pd.DataFrame()

    tickers = df.ticker.unique()
    cores = int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    pool = Pool(cores)
    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        ticker_chunk_history_df = pd.DataFrame()
        if not history_df.empty:
            ticker_chunk_history_df = history_df[history_df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run, args=(ticker_chunk_df,ticker_chunk_history_df))
        async_results.append(async_result)
    pool.close()
    log('info',"process pool start.")

    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = pd.concat([df,async_result.get()])
        else:
            log("error","result empty")
            # time.sleep(10)
            # return pd.DataFrame()
            continue
    
    
    if(not df.empty):
        log('info',"result is ready.")
        df.reset_index(drop=True,inplace=True)
        try:
            df.to_feather(processed_data_path + raw_data_files[-1])
            log('info',processed_data_path + raw_data_files[-1]+" is saved.")
        except Exception as e:
            log('critical',"to_feather:"+str(e))
        # global_df.to_csv(processed_data_path + raw_data_files[-1] + '.csv')
    else:
        log('error',"df empty")
    
    log('info',"process_data stop.")
    return df

def log(type,string):
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_process.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    now = datetime.datetime.now()
    log_time = now.strftime("%m%d%Y-%H%M%S")
    if type=='info':
        logging.info(log_time+":"+string)
    elif type=='warning':
        logging.warning(log_time+":"+string)
    elif type=='error':
        # directory_path = os.getcwd()
        # file_path = directory_path+'\Sounds\PriceNotice.wav'
        # try:
        #     playsound(file_path)
        # except Exception as e:
        #     logging.info(log_time+":"+str(e))
        logging.error(log_time+":"+string)
    elif type=='critical':
        # directory_path = os.getcwd()
        # file_path = directory_path+'\Sounds\PriceNotice.wav'
        # try:
        #     playsound(file_path)
        # except Exception as e:
        #     logging.info(log_time+":"+str(e))
        logging.critical(log_time+":"+string)

if __name__ == '__main__':
    raw_data_path='//jack-nas.home/Work/Python/RawData/'
    processed_data_path='//jack-nas.home/Work/Python/ProcessedData/'


    log('info','process_data process start.')

    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)
    else:
        log('info','path exists.')
        # delete existing files but keep the last 1 file
        files = os.listdir(processed_data_path)
        files.sort()
        for file in files[:-1]:
            os.remove(processed_data_path+file)
            log('info',file+" deleted.")

    history_df = pd.DataFrame()
    while(history_df.empty):
        history_df = process_data(pd.DataFrame())

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    # while(True):
    #     process_data(history_df)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)): 
        df = process_data(history_df)
        if df.empty:
            history_df = pd.DataFrame()
            while(history_df.empty):
                history_df = process_data(pd.DataFrame())
    
    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    log('info','process_data process exit.')