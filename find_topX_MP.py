#from pandas_datareader import data
import multiprocessing
from yahoo_fin import stock_info as si
#import yfinance as yf
#import matplotlib.pyplot as plt
import pandas as pd
#import seaborn as sns
import numpy as np
import datetime
#import time
import os
# import threading
import chip
import wr
import shutil
# from multiprocessing import Process
from multiprocessing import Pool
# from multiprocessing import Value
from multiprocessing import Process, Manager
import socket

run_days = 365*5
topX = 20
backward = 200

EMA_Indicator = True
MACD_Indicator = True
OBV_Indicator = True
Cum_Turnover_Indicator = True
Cum_Chip_Indicator = True
Chip_Concentration_Indicator = False
WR_Indicator = True

obv_convergence = 1
obv_above_zero_days_bar = 0.7
cum_turnover_rate = 0.5
cum_chip_bar = 0.8
chip_concentration_bar = 0.4
wr_bar = 40
wr120_greater_than_50_days_bar = 0.6
wr120_greater_than_80_days_bar = 0.16

def screen(df):
    if len(df) <= backward+2:
        return pd.DataFrame()

    backward_startindex = len(df)-backward
    origin_endindex = len(df)
    
    df = df[backward_startindex:origin_endindex].reset_index(drop=True)

    startindex = df.index[0]
    endindex = len(df)
    lastindex = df.index[-1]
    
    ema = df['close'][lastindex] < df['EMA34'][lastindex]
    # if EMA_Indicator & (ema):
    #     return pd.DataFrame()

    macd = (df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0
    # if MACD_Indicator & (macd):
    #     return pd.DataFrame()
    
    obv_above_zero_days = 0
    for i in range(startindex,endindex):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
    # if OBV_Indicator & (obv_above_zero_days/backward < obv_above_zero_days_bar):
    #     return pd.DataFrame()

    OBV_DIFF_RATE = df['OBV_DIFF_RATE'][lastindex]
    # if OBV_Indicator & (df['OBV_DIFF_RATE'][lastindex] > obv_convergence):
    #     return pd.DataFrame()

    cum_turnover = df['cum_turnover'][lastindex]
    # if Cum_Turnover_Indicator & (df['cum_turnover'][lastindex] < cum_turnover_rate):
    #     return pd.DataFrame()

    cum_chip = 0
    chip_con = 1
    ss = chip.Cal_Chip_Distribution(df)
    if not ss.empty:
        cum_chip = ss['Cum_Chip'][ss.index[-1]]
        # if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
        #     return pd.DataFrame()
        chip_con = chip.Cal_Chip_Concentration(ss)
        # if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
        #     return pd.DataFrame()

    wr34 = df.WR34[lastindex]
    wr120 = df.WR120[lastindex]
    # if WR_Indicator & ((df.WR34[lastindex] > wr_bar) | (df.WR120[lastindex] > wr_bar)):
    #     return pd.DataFrame()

    wr120_larger_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_larger_than_50_days += 1
    # if WR_Indicator & (wr120_less_than_50_days/backward < wr120_greater_than_50_days_bar):
    #     return pd.DataFrame()

    wr120_larger_than_80_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 80:
            wr120_larger_than_80_days += 1
    # if WR_Indicator & (wr120_less_than_80_days/backward < wr120_greater_than_80_days_bar):
    #     return pd.DataFrame()

    ticker_data = [df.date[lastindex],df.ticker[lastindex],df.change[lastindex],df.change_5days[lastindex],df.change_10days[lastindex],df.change_15days[lastindex],df.change_20days[lastindex],df.change_25days[lastindex],df.change_30days[lastindex],df.turn[lastindex],ema,macd,obv_above_zero_days,OBV_DIFF_RATE,cum_turnover,cum_chip,chip_con,wr34,wr120,wr120_larger_than_50_days,wr120_larger_than_80_days]

    return ticker_data

def run_all_by_date(df_dict,date):
    # i = (end - date).days
    history_topX_data_path = topX_data_path + f'{date}'
    processed_data_files = os.listdir(processed_data_path)
    # isFileExists = os.path.exists(history_topX_data_path + '.csv')
    # # topX_tickers = []
    # if isFileExists:
    #     return
        # topX_data_files = os.listdir(topX_data_path + str(date))
        # df = pd.read_csv(history_topX_data_path + '.csv')
        # topX_tickers = df.ticker.values

    tickers_change_list = []
    for file in processed_data_files:
        # isTickerExists = os.path.exists(topX_data_path + str(date) + f'/{file}')
        # if not isTickerExists:
        # ticker = file[:len(file)-4]
        # if ticker not in topX_tickers:
        # df = pd.read_csv(processed_data_path + f'/{file}')
        df = df_dict[file]
        if str(date) not in df.date.values:
            continue
        if df.volume[df.date == str(date)].iloc[0]*df.close[df.date == str(date)].iloc[0] < 500000:
            continue
        change = df.change[df.date == str(date)].iloc[0]
        ticker_change_list = [df.ticker[df.date == str(date)].iloc[0],change]
        tickers_change_list.append(ticker_change_list)
    if len(tickers_change_list) == 0:
        return
    tickers_change_list_df = pd.DataFrame(tickers_change_list, columns = ['ticker','change'])
    topX_tickers_change_list_df = tickers_change_list_df.nlargest(topX,'change')
            # history_topX_data_path = topX_data_path + f'{date}'
            # isPathExists = os.path.exists(history_topX_data_path)
            # if not isPathExists:
            #     os.makedirs(history_topX_data_path,exist_ok=True)
            # save.to_csv(history_topX_data_path + f'/{file}')
    ticker_data_list = []
    for ticker in topX_tickers_change_list_df.ticker:
        file = ticker + '.csv'
        # isTickerExists = os.path.exists(screened_data_path + str(date) + f'/{file}')
        # if not isTickerExists:
        # if ticker not in topX_tickers:
        # df = pd.read_csv(processed_data_path + f'/{file}')
        df = df_dict[file]
        if str(date) not in df.date.values:
            continue
        df_slice = df[0:df[df.date == str(date)].index[0]+1].reset_index(drop=True)
        if (len(df_slice)) <= backward+2:
            continue
        # df_slice = df[0:len(df)-i].reset_index(drop=True)
        # isTickerExists = os.path.exists(history_screened_data_path + f'/{file}')
        # if not isTickerExists:
        ticker_data = screen(df_slice)

        pre_df_slice = df[0:df[df.date == str(date)].index[0]].reset_index(drop=True)
        if (len(pre_df_slice)) <= backward+2:
            continue
        # df_slice = df[0:len(df)-i].reset_index(drop=True)
        # isTickerExists = os.path.exists(history_screened_data_path + f'/{file}')
        # if not isTickerExists:
        pre_ticker_data = screen(pre_df_slice)

        if len(ticker_data) != 0:
            ticker_data_list.append(ticker_data)
            ticker_data_list.append(pre_ticker_data)
    ticker_data_list_df = pd.DataFrame(ticker_data_list, columns = ['date','ticker','change','change_5days','change_10days','change_15days','change_20days','change_25days','change_30days','turn','ema','macd','obv_above_zero_days','OBV_DIFF_RATE','cum_turnover','cum_chip','chip_con','wr34','wr120','wr120_larger_than_50_days','wr120_larger_than_80_days'])
    
    isPathExists = os.path.exists(topX_data_path)
    if not isPathExists:
        os.makedirs(topX_data_path,exist_ok=True)
    # history_screened_data_path = screened_data_path+f'{date}'
    # history_day = save.date[save.index[-1]]
    # history_screened_data_path = screened_data_path + f'{history_day}'
    ticker_data_list_df.to_csv(history_topX_data_path + '.csv')
    return

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/{end}"
topX_data_path=f"//jack-nas/Work/Python/TopX/"

if __name__ == '__main__':
    # porcessed_numbers = Value('d', 0)
    isPathExists = os.path.exists(topX_data_path)
    if not isPathExists:
        os.makedirs(topX_data_path)
        #print(data_path+"created successfully\n")
    # else:
        # files = os.listdir(topX_data_path)
        # for f in files:
        #     os.remove(os.path.join(topX_data_path,f))

        # files = os.listdir(topX_data_path)
        # for file in files:
        #     if os.path.isdir(topX_data_path+file):
        #         shutil.rmtree(topX_data_path+file)
    
    with Manager() as manager:
        df_dict = manager.dict()
        processed_data_files = os.listdir(processed_data_path)
        for file in processed_data_files:
            df = pd.read_csv(processed_data_path + f'/{file}')
            df_dict[f'{file}'] = df

        date_list = []
        if socket.gethostname() == 'Jack-LC':
            date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]
        else:
            date_list = [end - datetime.timedelta(days=x) for x in range(run_days,run_days*2)]
        # date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]
        topX_data_files = os.listdir(topX_data_path)
        # for date in date_list:
        #     p = Process(target=run_all_by_date, args=(df_dict,date))
        #     p.start()
        #     p.join()
    
        cores = multiprocessing.cpu_count()
        with Pool(cores*2) as p:
            # p.map(run_all_by_ticker, raw_data_files)
            for date in date_list:
                history_topX_data_path = topX_data_path + f'{date}'
                if history_topX_data_path in topX_data_files:
                    continue
                p.apply_async(run_all_by_date, args=(df_dict, date))
            p.close()
            p.join()
