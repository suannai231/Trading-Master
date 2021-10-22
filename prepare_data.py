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

backward = 200
CAP_Limit = 2000000000
Price_Limit = 30

def prepare_data(df):
    origin_lastindex = df.index[-1]
    cap = df["marketCap"][origin_lastindex]
    if cap > CAP_Limit:
        #print(f'Ticker: {ticker} market cap is over 2B\n')
        return pd.DataFrame()
    elif df['close'][origin_lastindex] > Price_Limit:
        #print(f'Ticker: {ticker} close price is over 30\n')
        return pd.DataFrame()
    else:
        if len(df) <= backward+2:
            return pd.DataFrame()

        backward_startindex = len(df)-backward
        origin_endindex = len(df)
        
        df = df[backward_startindex:origin_endindex].reset_index(drop=True)
        df.rename(columns={"Unnamed: 0":"date"}, inplace=True)

        startindex = df.index[0]
        endindex = len(df)
        lastindex = df.index[-1]

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
                
        #Calculate the On Balance volume
        OBV = []
        OBV.append(0)
        for i in range(startindex+1, endindex):
            if df.close[i] > df.close[i-1]: #If the closing price is above the prior close price 
                OBV.append(OBV[-1] + df.volume[i]) #then: Current OBV = Previous OBV + Current volume
            elif df.close[i] < df.close[i-1]:
                OBV.append( OBV[-1] - df.volume[i])
            else:
                OBV.append(OBV[-1])
        #Store the OBV and OBV EMA into new columns
        df['OBV'] = OBV
        df['OBV_EMA34'] = df['OBV'].ewm(com=34).mean()
        df['OBV_DIFF'] = df['OBV']-df['OBV_EMA34']
        max_obv_diff = 0
        # obv_above_zero = 0
        OBV_DIFF_RATE = []
        # OBV_ABOVE_ZERO_DAYS = []
        for i in range(startindex,endindex):
            if abs(df['OBV_DIFF'][i]) > max_obv_diff:
                max_obv_diff = abs(df['OBV_DIFF'][i])
            if max_obv_diff == 0:
                OBV_DIFF_RATE.append(0)
            else:
                OBV_DIFF_RATE.append(abs(df['OBV_DIFF'][i])/max_obv_diff)
            # if df['OBV_DIFF'][i] > 0:
            #     obv_above_zero += 1
            # OBV_ABOVE_ZERO_DAYS.append(obv_above_zero)
        df["OBV_DIFF_RATE"] = OBV_DIFF_RATE
        # df["OBV_ABOVE_ZERO_DAYS"] = OBV_ABOVE_ZERO_DAYS

        df = wr.Cal_Daily_WR(df)

        # ss = chip.Cal_Chip_Distribution(df)
        # if not ss.empty:
        #     cum_chip = ss['Cum_Chip'][ss.index[-1]]
        #     chip_con = chip.Cal_Chip_Concentration(ss)
        #     df['Cum_Chip'] = cum_chip
        #     df['Chip_Con'] = chip_con

        # df = wr.Cal_Daily_WR(df)
        # wr120_less_than_50_days = wr.Cal_WR120_Greater_Than_X_Days(df,50)
        # wr120_less_than_80_days = wr.Cal_WR120_Greater_Than_X_Days(df,80)
        # df['WR120_Greater_Than_X_Days'] = wr120_less_than_50_days
        # df['WR120_Greater_Than_X_Days'] = wr120_less_than_80_days

        return df

def run_today_by_ticker(file):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
    # print(file)
    df = pd.read_csv(raw_data_path + file)
    save = prepare_data(df)
    # df_length = len(df)
    # for i in range(df_length):
    #     if (len(df)-i) <= backward+2:
    #         break
    #     history_df = df[0:len(df)-i].reset_index(drop=True)
    #     save = prepare_data(history_df)
    #     history_day = history_df.date[history_df.index[-1]]
    # history_processed_data_path = processed_data_path+f'/{history_day}'
    if not save.empty:
        save.to_csv(processed_data_path + f'{end}' + f'/{file}')
    return

def run_all_by_ticker(file):
    df = pd.read_csv(raw_data_path+'/'+file)
    # save = prepare_data(df)
    for i in range(len(df)):
        if (len(df)-i) <= backward+2:
            break
        df_slice = df[0:len(df)-i].reset_index(drop=True)
        save = prepare_data(df_slice)
        if not save.empty:
            history_day = save.date[save.index[-1]]
            history_processed_data_path = processed_data_path + f'{history_day}'
            save.to_csv(history_processed_data_path + f'/{file}')
    return

def run_all_by_date(date):

    i = end - date
    df = pd.read_csv(raw_data_path+'/'+file)
    
    if (len(df)-i) <= backward+2:
        break
    df_slice = df[0:len(df)-i].reset_index(drop=True)
    save = prepare_data(df_slice)
    if not save.empty:
        history_day = save.date[save.index[-1]]
        history_processed_data_path = processed_data_path + f'{history_day}'
        save.to_csv(history_processed_data_path + f'/{file}')
    return

end = datetime.date.today()
raw_data_path=f"//jack-nas/home/Drive/Python/RawData/{end}/"
processed_data_path="//jack-nas/home/Drive/Python/ProcessedData/"

raw_data_files = os.listdir(raw_data_path)

if __name__ == '__main__':
    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)
    else:
        # files = os.listdir(processed_data_path)
        # for f in files:
        #     os.remove(os.path.join(processed_data_path,f))

        files = os.listdir(processed_data_path)
        for file in files:
            if os.path.isdir(processed_data_path+'/'+file):
                shutil.rmtree(processed_data_path+'/'+file)

    for i in range(365*2-200):
        history_day = str((datetime.datetime.now() - datetime.timedelta(days=i)).date())
        history_processed_data_path = processed_data_path+f'/{history_day}'
        os.makedirs(history_processed_data_path,exist_ok=True)



    cores = multiprocessing.cpu_count()
    with Pool(cores) as p:
        p.map(run_all_by_ticker, raw_data_files)
        p.close()
        p.join()
    
    # print('all tickers data have been prepared.\n')
    os.popen(f'python C:/Users/jayin/OneDrive/Code/screen.py')

