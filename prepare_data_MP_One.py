#from pandas_datareader import data
# from yahoo_fin import stock_info as si
#import yfinance as yf
#import matplotlib.pyplot as plt
import pandas as pd
#import seaborn as sns
# import numpy as np
import datetime
#import time
import os
# import threading
# import chip
import wr
import shutil
# from multiprocessing import Process
import multiprocessing
from multiprocessing import Pool
# from multiprocessing import Value
from multiprocessing import Process, Manager
import socket

# run_days = 200
backward = 200
CAP_Limit = 2000000000
Price_Limit = 50

def prepare_data(df):
    origin_lastindex = df.index[-1]
    cap = df["marketCap"][origin_lastindex]
    if cap > CAP_Limit:
        return pd.DataFrame()
    elif df['close'][origin_lastindex] > Price_Limit:
        return pd.DataFrame()
    elif len(df) <= backward+2:
        return pd.DataFrame()
    else:
        # df.rename(columns={"Unnamed: 0":"date"}, inplace=True)

        startindex = df.index[0]
        endindex = len(df)
        lastindex = df.index[-1]

        df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
        df['change_5days'] = (df.close.shift(-5)- df.close)/df.close
        df['change_10days'] = (df.close.shift(-10)- df.close)/df.close
        df['change_15days'] = (df.close.shift(-15)- df.close)/df.close
        df['change_20days'] = (df.close.shift(-20)- df.close)/df.close
        df['change_25days'] = (df.close.shift(-25)- df.close)/df.close
        df['change_30days'] = (df.close.shift(-30)- df.close)/df.close
        df['change_35days'] = (df.close.shift(-35)- df.close)/df.close
        df['change_40days'] = (df.close.shift(-40)- df.close)/df.close
        df['change_45days'] = (df.close.shift(-45)- df.close)/df.close
        df['change_50days'] = (df.close.shift(-50)- df.close)/df.close
        df['change_55days'] = (df.close.shift(-55)- df.close)/df.close
        df['change_60days'] = (df.close.shift(-60)- df.close)/df.close

        shares = df['shares'][lastindex]
        df['turn'] = df.volume/shares
        df['cum_turnover'] = df['turn'].cumsum()

        ema34 = df['close'].ewm(span = 34, adjust = False).mean()
        ema120 = df['close'].ewm(span = 120, adjust = False).mean()
        df['EMA34'] = ema34
        df['EMA120'] = ema120

        MACD_dif = ema34 - ema120
        MACD_dea = MACD_dif.ewm(span = 9, adjust = False).mean()
        df['MACD_dif'] = MACD_dif
        df['MACD_dea'] = MACD_dea
                
        OBV = []
        OBV.append(0)
        for i in range(startindex+1, endindex):
            if df.close[i] > df.close[i-1]: #If the closing price is above the prior close price 
                OBV.append(OBV[-1] + df.volume[i]) #then: Current OBV = Previous OBV + Current volume
            elif df.close[i] < df.close[i-1]:
                OBV.append( OBV[-1] - df.volume[i])
            else:
                OBV.append(OBV[-1])

        df['OBV'] = OBV
        df['OBV_EMA34'] = df['OBV'].ewm(com=34).mean()
        df['OBV_DIFF'] = df['OBV']-df['OBV_EMA34']
        max_obv_diff = 0

        OBV_DIFF_RATE = []

        for i in range(startindex,endindex):
            if abs(df['OBV_DIFF'][i]) > max_obv_diff:
                max_obv_diff = abs(df['OBV_DIFF'][i])
            if max_obv_diff == 0:
                OBV_DIFF_RATE.append(0)
            else:
                OBV_DIFF_RATE.append(abs(df['OBV_DIFF'][i])/max_obv_diff)

        df["OBV_DIFF_RATE"] = OBV_DIFF_RATE
        df = wr.Cal_Daily_WR(df)
        return df

end = datetime.date.today()
raw_data_path=f"//jack-nas/Work/Python/RawData/"
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"

if __name__ == '__main__':
    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)
    # else:
        # files = os.listdir(processed_data_path)
        # for f in files:
        #     os.remove(os.path.join(processed_data_path,f))

        # files = os.listdir(processed_data_path)
        # for file in files:
        #     if os.path.isdir(processed_data_path+'/'+file):
        #         shutil.rmtree(processed_data_path+'/'+file)

    # date_list = [end - datetime.timedelta(days=x) for x in range(run_days)]

    # for i in date_list:
    #     history_processed_data_path = processed_data_path+f'/{i}'
    #     os.makedirs(history_processed_data_path,exist_ok=True)
    # with Manager() as manager:
        # df_dict = manager.dict()
    
    # raw_data_files = os.listdir(raw_data_path)
    
    df = pd.read_feather(raw_data_path + f'/{end}' + '.feather')
    tickers = df.ticker.unique()

    processed_files = os.listdir(processed_data_path)
    processed_file = str(end)+'.feather'
    if processed_file in processed_files:
        exit()
    
    cores = multiprocessing.cpu_count()
    pool = Pool(cores*3)
    async_results = []
    for ticker in tickers:
        ticker_df = df[df.ticker==ticker].reset_index(drop=True)
        async_result = pool.apply_async(prepare_data, args=(ticker_df,))
        async_results.append(async_result)
    pool.close()
    pool.join()
    del(df)
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = df.append(async_result.get())
    df.reset_index(drop=True,inplace=True)
    df.to_feather(processed_data_path + f'{end}' + '.feather')
    # print('all tickers data have been prepared.\n')

    # os.popen(f'python C:/Users/jayin/OneDrive/Code/find_topX_MP.py')

    # if socket.gethostname() == 'Jack-LC':
    #     os.popen(f'python C:/Users/jayin/OneDrive/Code/find_topX_MP_1.py')
    # else:
    #     os.popen(f'python C:/Users/jayin/OneDrive/Code/find_topX_MP_2.py')
