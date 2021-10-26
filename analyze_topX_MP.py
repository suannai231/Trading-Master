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

# run_days = 365*5
# topX = 20
# backward = 200

# EMA_Indicator = True
# MACD_Indicator = True
# OBV_Indicator = True
# Cum_Turnover_Indicator = True
# Cum_Chip_Indicator = True
# Chip_Concentration_Indicator = False
# WR_Indicator = True

# obv_convergence = 1
# obv_above_zero_days_bar = 0.7
# cum_turnover_rate = 0.5
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
# wr_bar = 40
# wr120_greater_than_50_days_bar = 0.6
# wr120_greater_than_80_days_bar = 0.16

# def screen(df):
#     if len(df) <= backward+2:
#         return pd.DataFrame()

#     backward_startindex = len(df)-backward
#     origin_endindex = len(df)
    
#     df = df[backward_startindex:origin_endindex].reset_index(drop=True)

#     startindex = df.index[0]
#     endindex = len(df)
#     lastindex = df.index[-1]
    
#     ema = df['close'][lastindex] < df['EMA34'][lastindex]
#     # if EMA_Indicator & (ema):
#     #     return pd.DataFrame()

#     macd = (df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0
#     # if MACD_Indicator & (macd):
#     #     return pd.DataFrame()
    
#     obv_above_zero_days = 0
#     for i in range(startindex,endindex):
#         if df['OBV_DIFF'][i] > 0:
#             obv_above_zero_days += 1
#     # if OBV_Indicator & (obv_above_zero_days/backward < obv_above_zero_days_bar):
#     #     return pd.DataFrame()

#     OBV_DIFF_RATE = df['OBV_DIFF_RATE'][lastindex]
#     # if OBV_Indicator & (df['OBV_DIFF_RATE'][lastindex] > obv_convergence):
#     #     return pd.DataFrame()

#     cum_turnover = df['cum_turnover'][lastindex]
#     # if Cum_Turnover_Indicator & (df['cum_turnover'][lastindex] < cum_turnover_rate):
#     #     return pd.DataFrame()

#     cum_chip = 0
#     chip_con = 1
#     ss = chip.Cal_Chip_Distribution(df)
#     if not ss.empty:
#         cum_chip = ss['Cum_Chip'][ss.index[-1]]
#         # if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
#         #     return pd.DataFrame()
#         chip_con = chip.Cal_Chip_Concentration(ss)
#         # if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
#         #     return pd.DataFrame()

#     wr34 = df.WR34[lastindex]
#     wr120 = df.WR120[lastindex]
#     # if WR_Indicator & ((df.WR34[lastindex] > wr_bar) | (df.WR120[lastindex] > wr_bar)):
#     #     return pd.DataFrame()

#     wr120_larger_than_50_days = 0
#     for i in range(startindex,endindex):
#         if df.WR120[i] > 50:
#             wr120_larger_than_50_days += 1
#     # if WR_Indicator & (wr120_less_than_50_days/backward < wr120_greater_than_50_days_bar):
#     #     return pd.DataFrame()

#     wr120_larger_than_80_days = 0
#     for i in range(startindex,endindex):
#         if df.WR120[i] > 80:
#             wr120_larger_than_80_days += 1
#     # if WR_Indicator & (wr120_less_than_80_days/backward < wr120_greater_than_80_days_bar):
#     #     return pd.DataFrame()

#     ticker_data = [df.date[lastindex],df.ticker[lastindex],df.change[lastindex],df.change_5days[lastindex],df.change_10days[lastindex],df.change_15days[lastindex],df.change_20days[lastindex],df.change_25days[lastindex],df.change_30days[lastindex],df.turn[lastindex],ema,macd,obv_above_zero_days,OBV_DIFF_RATE,cum_turnover,cum_chip,chip_con,wr34,wr120,wr120_larger_than_50_days,wr120_larger_than_80_days]

#     return ticker_data

def run(df_list,day):
    top_change_Xdays_df = pd.DataFrame()
    for df in df_list:
        if df.empty:
            continue
        # top_change_Xdays_df = df.nlargest(1,'change_' + str(day) +'days')
        column = 'change_' + str(day) +'days'
        sorted_df = df.sort_values(by=[column],ascending=False,ignore_index=True)
        index = sorted_df.index[0]
        if math.isnan(sorted_df[column][index]):
            continue
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
                busdays = np.busday_count( ticker_date, date)
                # delta = date - ticker_date
                if (busdays > 0) & (busdays<=day):
                    if index != sorted_df.index[-1]:
                        loop = True
                    else:
                        loop = False
                    index+=1
                    break
        if index == sorted_df.index[-1]+1:
            continue
        top_change_Xdays_df = top_change_Xdays_df.append(sorted_df.iloc[index],ignore_index=True)
    if len(top_change_Xdays_df)>0:
        os.makedirs(analyzed_topX_data_path,exist_ok=True)
        top_change_Xdays_df.to_csv(analyzed_topX_data_path + f'{day}' + 'days.csv')
    return

# end = datetime.date.today()
# processed_data_path=f"//jack-nas/Work/Python/ProcessedData/{end}"
topX_data_path=f"//jack-nas/Work/Python/TopX/"
analyzed_topX_data_path=f"//jack-nas/Work/Python/AnalyzedTopX/"

if __name__ == '__main__':
    # porcessed_numbers = Value('d', 0)
    isPathExists = os.path.exists(analyzed_topX_data_path)
    if not isPathExists:
        os.makedirs(analyzed_topX_data_path)
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
        df_list = manager.list()
        topX_data_files = os.listdir(topX_data_path)
        for file in topX_data_files:
            df = pd.read_csv(topX_data_path + f'/{file}')
            df_list.append(df)
            # df_dict[f'{file}'] = df

        # date_list = []
        # if socket.gethostname() == 'Jack-LC':
        #     date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]
        # else:
        #     date_list = [end - datetime.timedelta(days=x) for x in range(run_days,run_days*2)]
        # date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]
        analyzed_topX_data_files = os.listdir(analyzed_topX_data_path)
        # for date in date_list:
        #     p = Process(target=run_all_by_date, args=(df_dict,date))
        #     p.start()
        #     p.join()
    
        # cores = multiprocessing.cpu_count()
        days = {5,10,15,20,25,30}
        with Pool(len(days)) as p:
            # p.map(run_all_by_ticker, raw_data_files)
            for day in days:
                analyzed_topX_data_file = f'{day}'+'days.csv'
                if analyzed_topX_data_file in analyzed_topX_data_files:
                    continue
                p.apply_async(run, args=(df_list, day))
            p.close()
            p.join()
