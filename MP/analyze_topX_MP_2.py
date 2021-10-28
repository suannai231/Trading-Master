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
import akshare as ak
import math

def run_one_day(df,period):
    if df.empty:
        return pd.DataFrame()
    column = 'change_' + str(period) +'days'
    sorted_df = df.sort_values(by=[column],ascending=False,ignore_index=True)
    index = sorted_df.index[0]
    if math.isnan(sorted_df[column][index]):
        return pd.DataFrame()
    loop = True
    while loop & (index<=sorted_df.index[-1]) & (not math.isnan(sorted_df[column][index])):
        loop = False
        ticker = sorted_df['ticker'][index]
        ticker_date_str = sorted_df['date'][index]
        ticker_date = datetime.datetime.strptime(ticker_date_str,"%Y-%m-%d")
        try:
            qfq_df = ak.stock_us_daily(symbol=ticker, adjust="qfq-factor")
        except Exception as e:
            print(e)
            break
        for date in qfq_df.index:
            start = ticker_date.date()
            end = date.date()
            busdays = np.busday_count( start, end)
            if (busdays > 0) & (busdays<=period+1):
                if index != sorted_df.index[-1]:
                    loop = True
                else:
                    loop = False
                index+=1
                break
    if index == sorted_df.index[-1]+1:
        return pd.DataFrame()
    return sorted_df.iloc[index]

def run_all_days(df_list,period):
    top_change_Xdays_df = pd.DataFrame()
    cores = multiprocessing.cpu_count()
    with Pool(len(cores)) as p:
        async_result_list = []
        for df in df_list:
            async_result = p.apply_async(run_one_day, args=(df, period))
            async_result_list.append(async_result)
        p.close()
        p.join()
        for async_result in async_result_list:
            result = async_result.get()
            if not result.empty:
                top_change_Xdays_df = top_change_Xdays_df.append(result,ignore_index=True)
    if len(top_change_Xdays_df)>0:
        os.makedirs(analyzed_topX_data_path,exist_ok=True)
        top_change_Xdays_df.to_csv(analyzed_topX_data_path + f'{period}' + 'days.csv')
    return

topX_data_path=f"//jack-nas/Work/Python/TopX/"
analyzed_topX_data_path=f"//jack-nas/Work/Python/AnalyzedTopX/"

if __name__ == '__main__':
    isPathExists = os.path.exists(analyzed_topX_data_path)
    if not isPathExists:
        os.makedirs(analyzed_topX_data_path)

    df_list = []
    topX_data_files = os.listdir(topX_data_path)
    for file in topX_data_files:
        df = pd.read_csv(topX_data_path + f'/{file}')
        df_list.append(df)

    analyzed_topX_data_files = os.listdir(analyzed_topX_data_path)

    periods = range(5,61,5)
    for period in periods:
        analyzed_topX_data_file = f'{period}'+'days.csv'
        if analyzed_topX_data_file in analyzed_topX_data_files:
            continue
        run_all_days(df_list,period)

