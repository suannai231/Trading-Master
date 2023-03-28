import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import time
import logging
import math
import numpy as np
from playsound import playsound

def cal_basics(df,ticker_history_df):
    if ticker_history_df.empty:
        df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
        # df['AMP'] = (df['high']-df['low'])/df['low']

        # ema5 = df['close'].ewm(span = 5, adjust = False).mean()
        ema10 = df['close'].ewm(span = 10, adjust = False).mean()
        # ema12 = df['close'].ewm(span = 12, adjust = False).mean()
        ema20 = df['close'].ewm(span = 20, adjust = False).mean()
        # ema26 = df['close'].ewm(span = 26, adjust = False).mean()
        # ema60 = df['close'].ewm(span = 60, adjust = False).mean()
        # ema120 = df['close'].ewm(span = 120, adjust = False).mean()
        # ema250 = df['close'].ewm(span = 250, adjust = False).mean()
        # std20 = df['close'].rolling(window=20).std()
        # df['EMA5'] = ema5
        df['EMA10'] = ema10
        # df['EMA12'] = ema12
        df['EMA20'] = ema20
        # df['EMA26'] = ema26
        # df['EMA60'] = ema60
        # df['EMA120'] = ema120
        # df['EMA250'] = ema250
        # df['DIF'] = ema12-ema26
        # df['DEA'] = df['DIF'].ewm(span = 9, adjust = False).mean()
        # df['STD20'] = std20
        # df['STD20_EMA5'] = df['STD20'].ewm(span = 5, adjust = False).mean()

        # LLV7 = df['low'].rolling(window=7).min()
        # LLV14 = df['low'].rolling(window=14).min()
        # LLV28 = df['low'].rolling(window=28).min()
        # HHV7 = df['high'].rolling(window=7).max()
        # HHV14 = df['high'].rolling(window=14).max()
        # HHV28 = df['high'].rolling(window=28).max()
        # WSTA=((df['close']-LLV7)/(HHV7-LLV7))*4
        # WMTA=((df['close']-LLV14)/(HHV14-LLV14))*2
        # WLTA=((df['close']-LLV28)/(HHV28-LLV28))
        # UO=100*((WSTA+WMTA+WLTA)/(4+2+1))
        # df['UO']=UO

        df_v120 = df['volume'].rolling(window=120).max()
        low = []

        for vol in df_v120:
            if not np.isnan(vol):
                low.append(df.loc[df.volume==vol,'low'].values[0])

            else:
                low.append(np.nan)


        df['DIFF120L'] = df["close"] - low


        df['HHV5_DIFF120L'] = df['DIFF120L'].rolling(window=5).max()
        df['HHV10_DIFF120L'] = df['DIFF120L'].rolling(window=10).max()
        df['HHV20_DIFF120L'] = df['DIFF120L'].rolling(window=20).max()
        df['HHV60_DIFF120L'] = df['DIFF120L'].rolling(window=60).max()
        df['HHV120_DIFF120L'] = df['DIFF120L'].rolling(window=120).max()

        if df.iloc[-1].ticker == "TBIO":
            log("info", df.iloc[-1].ticker)

        return df
    else:
        if ticker_history_df.iloc[-1].date==df.iloc[-1].date:
            index = len(ticker_history_df)-1

            ticker_history_df.loc[index,'open']==df.iloc[-1].open
            ticker_history_df.loc[index,'high']==df.iloc[-1].high
            ticker_history_df.loc[index,'low']==df.iloc[-1].low
            ticker_history_df.loc[index,'close']==df.iloc[-1].close
            ticker_history_df.loc[index,'adjclose']==df.iloc[-1].adjclose
            ticker_history_df.loc[index,'volume']==df.iloc[-1].volume
            ticker_history_df.loc[index,'change']==(df.iloc[-1].close - df.iloc[-2].close)/df.iloc[-2].close
            ticker_history_df.loc[index,'EMA10']==df.tail(10).close.mean()
            ticker_history_df.loc[index,'EMA20']==df.tail(20).close.mean()

            df_tail120 = df.tail(120)
            low = df_tail120.loc[df_tail120.volume==df_tail120.volume.max(),'low'].values[0]

            ticker_history_df.loc[index,'DIFF120L'] = df.iloc[-1].close - low

            ticker_history_df.loc[index,'HHV5_DIFF120L'] = ticker_history_df.tail(5).DIFF120L.max()
            ticker_history_df.loc[index,'HHV10_DIFF120L'] = ticker_history_df.tail(10).DIFF120L.max()
            ticker_history_df.loc[index,'HHV20_DIFF120L'] = ticker_history_df.tail(20).DIFF120L.max()
            ticker_history_df.loc[index,'HHV60_DIFF120L'] = ticker_history_df.tail(60).DIFF120L.max()
            ticker_history_df.loc[index,'HHV120_DIFF120L'] = ticker_history_df.tail(120).DIFF120L.max()

            return ticker_history_df
        else:
            log("error",df.iloc[-1].ticker+"date mismatch")
            return pd.DataFrame()

def run(ticker_chunk_df,ticker_chunk_history_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        ticker_history_df = pd.DataFrame()
        if not ticker_chunk_history_df.empty:
            ticker_history_df = ticker_chunk_history_df[ticker_chunk_history_df.ticker==ticker].reset_index(drop=True)
        df = cal_basics(df,ticker_history_df)

        if not df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,df],ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_data(history_df):
    log('info',"process_data start.")
    global global_df
    raw_data_files = os.listdir(raw_data_path)
    if len(raw_data_files) == 0:
        log('warning',"raw data not ready, sleep 1 second...")
        time.sleep(1)
        return
    # date_time = datetime.datetime.now() 
    # datetime_str = date_time.strftime("%m%d%Y-%H")
    # processed_data_file = datetime_str + '.feather'

    processed_data_files = os.listdir(processed_data_path)
    if raw_data_files[-1] in processed_data_files:
        log('warning',"error: " + raw_data_files[-1] + " existed, sleep 1 seconds...")
        time.sleep(1)
        return
    
    log('info',"processing "+raw_data_files[-1])
    try:
        df = pd.read_feather(raw_data_path + raw_data_files[-1])
        log('info',raw_data_path + raw_data_files[-1]+" loaded.")
    except Exception as e:
        log('critical',str(e))
        return

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
        directory_path = os.getcwd()
        file_path = directory_path+'\Sounds\PriceNotice.wav'
        playsound(file_path)
        logging.error(log_time+":"+string)
    elif type=='critical':
        directory_path = os.getcwd()
        file_path = directory_path+'\Sounds\PriceNotice.wav'
        playsound(file_path)
        logging.critical(log_time+":"+string)

if __name__ == '__main__':
    raw_data_path='//jack-nas.home/Work/Python/RawData/'
    processed_data_path='//jack-nas.home/Work/Python/ProcessedData/'


    log('info','process_data process start.')

    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)

    history_df = process_data(pd.DataFrame())

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    # while(True):
    #     process_data(history_df)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)): 
        process_data(history_df)
    
    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    log('info','process_data process exit.')