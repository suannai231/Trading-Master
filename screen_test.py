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

run_days = 30
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

    if EMA_Indicator & (df['close'][lastindex] < df['EMA34'][lastindex]):
        return pd.DataFrame()

    if MACD_Indicator & ((df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0):
        return pd.DataFrame()
    
    obv_above_zero_days = 0
    for i in range(startindex,endindex):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
    if OBV_Indicator & (obv_above_zero_days/backward < obv_above_zero_days_bar):
        return pd.DataFrame()

    if OBV_Indicator & (df['OBV_DIFF_RATE'][lastindex] > obv_convergence):
        return pd.DataFrame()

    if Cum_Turnover_Indicator & (df['cum_turnover'][lastindex] < cum_turnover_rate):
        return pd.DataFrame()

    ss = chip.Cal_Chip_Distribution(df)
    if not ss.empty:
        cum_chip = ss['Cum_Chip'][ss.index[-1]]
        if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
            return pd.DataFrame()
        chip_con = chip.Cal_Chip_Concentration(ss)
        if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
            return pd.DataFrame()

    if WR_Indicator & ((df.WR34[lastindex] > wr_bar) | (df.WR120[lastindex] > wr_bar)):
        return pd.DataFrame()

    wr120_less_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_less_than_50_days += 1
    if WR_Indicator & (wr120_less_than_50_days/backward < wr120_greater_than_50_days_bar):
        return pd.DataFrame()

    wr120_less_than_80_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 80:
            wr120_less_than_80_days += 1
    if WR_Indicator & (wr120_less_than_80_days/backward < wr120_greater_than_80_days_bar):
        return pd.DataFrame()

    return df

def run_all_by_date(date,file):
    df = pd.read_csv(processed_data_path + f'/{file}')
    df_slice = df[0:df[df.date == str(date)].index[0]+1].reset_index(drop=True)
    screen(df_slice)
    return

end = datetime.date.today()
processed_data_path=f"E:/Python/ProcessedData/{end}"

run_all_by_date('2021-10-19','LMFA.csv')