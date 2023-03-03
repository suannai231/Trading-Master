import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
import time
import logging
import math
import numpy as np

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
    if df.iloc[-1].ticker == "LLAP":
        log("info", df.iloc[-1].ticker)
    df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
    df['AMP'] = (df['high']-df['low'])/df['low']

    ema5 = df['close'].ewm(span = 5, adjust = False).mean()
    ema10 = df['close'].ewm(span = 10, adjust = False).mean()
    ema12 = df['close'].ewm(span = 12, adjust = False).mean()
    ema20 = df['close'].ewm(span = 20, adjust = False).mean()
    ema26 = df['close'].ewm(span = 26, adjust = False).mean()
    ema60 = df['close'].ewm(span = 60, adjust = False).mean()
    ema120 = df['close'].ewm(span = 120, adjust = False).mean()
    ema250 = df['close'].ewm(span = 250, adjust = False).mean()
    std20 = df['close'].rolling(window=20).std()
    df['EMA5'] = ema5
    df['EMA10'] = ema10
    df['EMA12'] = ema12
    df['EMA20'] = ema20
    df['EMA26'] = ema26
    df['EMA60'] = ema60
    df['EMA120'] = ema120
    df['EMA250'] = ema250
    df['DIF'] = ema12-ema26
    df['DEA'] = df['DIF'].ewm(span = 9, adjust = False).mean()
    df['STD20'] = std20
    df['STD20_EMA5'] = df['STD20'].ewm(span = 5, adjust = False).mean()


    LLV7 = df['low'].rolling(window=7).min()
    LLV14 = df['low'].rolling(window=14).min()
    LLV28 = df['low'].rolling(window=28).min()
    HHV7 = df['high'].rolling(window=7).max()
    HHV14 = df['high'].rolling(window=14).max()
    HHV28 = df['high'].rolling(window=28).max()
    WSTA=((df['close']-LLV7)/(HHV7-LLV7))*4
    WMTA=((df['close']-LLV14)/(HHV14-LLV14))*2
    WLTA=((df['close']-LLV28)/(HHV28-LLV28))
    UO=100*((WSTA+WMTA+WLTA)/(4+2+1))
    df['UO']=UO

    # HHV60 = df['volume'].rolling(window=60).max()
    # VI = df.index[df.volume==HHV60].tolist()
    # VL = df[VI].low
    # DIFF= df['close']-VL
    # DIFF_EMA20=DIFF.ewm(span = 20, adjust = False).mean()

    # df['HHV60'] = df['volume'].rolling(window=60).max()  # calculate HHV(V, 60)
    # df['bar_num'] = np.arange(len(df)) + 1  # create a column of bar numbers starting from 1
    # last_bar_v_hhv_60 = df.loc[df['HHV60'].idxmax(), 'bar_num']  # find the bar number of the last occurrence of V = HHV(V, 60)
    # df['VL'] = df['low'].shift(last_bar_v_hhv_60 - 1)  # shift L by the appropriate number of bars
    # df['DIFF']= df['close']-df['VL']

    # hhv = df['volume'].rolling(window=60).max()
    # bars_last = df['volume'][::-1].eq(hhv[::-1].iloc[0]).cumsum()[::-1]
    # mask = bars_last.eq(1)
    # vl = df['low'].mask(~mask).ffill()
    # df['DIFF']= df['close']-vl

    # hhv_v = df['volume'].rolling(window=60).max()

    # mask_v = df['volume'].eq(hhv_v) & (df['volume'] != 0)
    # bars_last_v = mask_v[::-1].cumsum()[::-1]

    # mask_l = bars_last_v.eq(1)
    # vl = df['low'].shift()[mask_l].fillna(method='ffill')

    # Define the functions
    def REF(X, A):
        return X.shift(A)

    def BARSLAST(X):
        return len(X) - 1 - np.argmax(np.flip(X != 0))

    def HHV(X, N):
        return X.rolling(N).max()

    def EMA(X, N):
        return X.ewm(span=N, min_periods=N).mean()

    # assume you have a pandas DataFrame called 'data' with columns 'low' and 'volume'
    df['max_volume_60days'] = df['volume'].rolling(window=60).max()
    low = []
    for vol in df['max_volume_60days']:
        if not np.isnan(vol):
            low.append(df.loc[df.volume==vol,'low'].values[0])
        else:
            low.append(np.nan)
    df['max_volume_low_60days'] = low

    # Calculate DIFF
    df['DIFF'] = df["close"] - df['max_volume_low_60days']

    # Calculate EMA20
    df['DIFF_EMA20'] = df['DIFF'].ewm(span = 20, adjust = False).mean()

    return df

def run(ticker_chunk_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        lastindex = df.index[-1]
        # if df.iloc[-1].ticker == "DIDIY":
        #     log("info", df.iloc[-1].ticker)
        # if (11 > df['close'][lastindex] > 10) or (df['close'][lastindex]>20) or (df['close'][lastindex]<1):
        #     continue

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