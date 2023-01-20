import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import time
import logging
import math

# def cal_Long(df):
#     startindex = 0
#     endindex = len(df)
#     Long = []
#     Long.append(0)

#     for i in range(startindex+1, endindex):
#         previous_close = df.close[i-1]
#         if df.close[i] >= previous_close:
#             Long.append(Long[-1] + df.volume[i])
#         else:
#             Long.append(Long[-1])
#     df['Long'] = Long
#     return df

# def cal_Short(df):
#     startindex = 0
#     endindex = len(df)
#     Short = []
#     Short.append(0)

#     for i in range(startindex+1, endindex):
#         previous_close = df.close[i-1]
#         if df.close[i] < previous_close:
#             Short.append(Short[-1] + df.volume[i])
#         else:
#             Short.append(Short[-1])
#     df['Short'] = Short
#     return df

# def cal_OBV(df):
#     startindex = 0
#     endindex = len(df)
#     OBV = []
#     OBV.append(0)

#     for i in range(startindex+1, endindex):
#         previous_close = df.close[i-1]
#         if df.close[i] >= previous_close:
#             OBV.append(OBV[-1] + df.volume[i])
#         elif df.close[i] < previous_close:
#             OBV.append(OBV[-1] - df.volume[i])
#         else:
#             OBV.append(OBV[-1])
#         # OBV_MAX.append(max(OBV))
#     df['OBV'] = OBV
#     return df

def cal_basics(df):
    df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
    df['AMP'] = (df['high']-df['low'])/df['low']

    ema5 = df['close'].ewm(span = 5, adjust = False).mean()
    ema10 = df['close'].ewm(span = 10, adjust = False).mean()
    ema20 = df['close'].ewm(span = 20, adjust = False).mean()
    ema60 = df['close'].ewm(span = 60, adjust = False).mean()
    ema120 = df['close'].ewm(span = 120, adjust = False).mean()
    ema250 = df['close'].ewm(span = 250, adjust = False).mean()
    df['EMA5'] = ema5
    df['EMA10'] = ema10
    df['EMA20'] = ema20
    df['EMA60'] = ema60
    df['EMA120'] = ema120
    df['EMA250'] = ema250

    return df

def run(ticker_chunk_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        lastindex = df.index[-1]
        if df.iloc[-1].ticker == "DIDIY":
            log("info", df.iloc[-1].ticker)
        if (11 > df['close'][lastindex] > 10) or (df['close'][lastindex]>20) or (df['close'][lastindex]<1):
            continue

        if(len(df)>250):
            df = df.iloc[len(df)-250:]
            df.reset_index(drop=True,inplace=True)

        df = cal_basics(df)

        if not df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,df],ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_data():
    log('info',"process_data start.")
    raw_data_files = os.listdir(raw_data_path)
    if len(raw_data_files) == 0:
        log('warning',"raw data not ready, sleep 10 seconds...")
        time.sleep(10)
        return
    # date_time = datetime.datetime.now() 
    # datetime_str = date_time.strftime("%m%d%Y-%H")
    # processed_data_file = datetime_str + '.feather'

    processed_data_files = os.listdir(processed_data_path)
    if raw_data_files[-1] in processed_data_files:
        log('warning',"error: " + raw_data_files[-1] + " existed, sleep 10 seconds...")
        time.sleep(10)
        return
    
    log('info',"processing "+raw_data_files[-1])
    try:
        time.sleep(1)
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
        async_result = pool.apply_async(run, args=(ticker_chunk_df,))
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
        # df.to_csv(processed_data_path + raw_data_files[-1] + '.csv')
    else:
        log('error',"df empty")
    log('info',"process_data stop.")

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
        logging.error(log_time+":"+string)
    elif type=='critical':
        logging.critical(log_time+":"+string)

if __name__ == '__main__':
    raw_data_path='//jack-nas.home/Work/Python/RawData/'
    processed_data_path='//jack-nas.home/Work/Python/ProcessedData/'


    log('info','process_data process start.')

    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)

    process_data()

    now = datetime.datetime.now()
    today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)): 
        process_data()
    
    stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    log('info','process_data process exit.')