import pandas as pd
import numpy as np

def HHV(df,wr_days):
    high = 0
    lastindex = df.index[-1]
    if lastindex < wr_days:
        wr_days = df.index[-1]
    for i in range(df.index[-1]-wr_days,len(df)):
        if df.high[i] > high:
            high = df.high[i]
    return high

def LLV(df,wr_days):
    low = HHV(df,wr_days)
    lastindex = df.index[-1]
    if lastindex < wr_days:
        wr_days = df.index[-1]
    for i in range(df.index[-1]-wr_days,len(df)):
        if df.low[i] < low:
            low = df.low[i]
    return low

def CalWR(df,wr_days):
    if df.empty:
        return 100
    close = df.close[df.index[-1]]
    high_value = HHV(df,wr_days)
    low_value = LLV(df,wr_days)
    if high_value == low_value:
        return 100
    wr_value = (high_value - close)/(high_value-low_value)*100
    return wr_value

def Cal_Daily_WR(df):
    WR34_List = []
    WR120_List = []
    for i in range(0,len(df)):
        df_slice = df[0:len(df)-i]
        WR34 = CalWR(df_slice,34)
        WR120 = CalWR(df_slice,120)
        WR34_List.append(WR34)
        WR120_List.append(WR120)
    WR34_List.reverse()
    WR120_List.reverse()
    df['WR34'] = WR34_List
    df['WR120'] = WR120_List
    return df

# def Cal_WR120_Greater_Than_X_Days(df,x):
#     WR120_Less_Than_X_Days = 0
#     for i in range(df.index[-1]-200,len(df)):
#         if df.WR120[i] > x:
#             WR120_Less_Than_X_Days += 1
#     return WR120_Less_Than_X_Days