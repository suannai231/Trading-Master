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

def Cal_WR(df,i,wr_days):
    if df.empty:
        return df
    close = df.loc[i,'close']
    high_value = HHV(df.loc[0:i],wr_days)
    df.loc[i,str(wr_days)+'days_high_value'] = high_value
    low_value = LLV(df.loc[0:i],wr_days)
    df.loc[i,str(wr_days)+'days_low_value'] = low_value
    if high_value == low_value:
        wr_value = 100
    else:
        wr_value = (high_value - close)/(high_value-low_value)*100
    df.loc[i,'WR'+str(wr_days)] = wr_value
    return df

def Cal_Hist_WR(df,wr_days):
    if df.empty:
        return df
    last = len(df) - 1
    df = Cal_WR(df, last, wr_days)
    current = len(df)-2
    while current>=0:
        remove_index = current+1
        add_index = current-wr_days
        if (df.loc[remove_index,'close'] == df.loc[remove_index,str(wr_days)+'days_high_value']) | (df.loc[remove_index,'close'] == df.loc[remove_index,str(wr_days)+'days_low_value']):
            df = Cal_WR(df, current ,wr_days)
            high_value = df.loc[current,str(wr_days)+'days_high_value']
            low_value = df.loc[current,str(wr_days)+'days_low_value']
        else:
            high_value = df.loc[remove_index,str(wr_days)+'days_high_value']
            low_value = df.loc[remove_index,str(wr_days)+'days_low_value']

        if df.loc[add_index,'close'] > high_value:
            high_value = df.loc[add_index,'close']
        if df.loc[add_index,'close'] < low_value:
            low_value = df.loc[add_index,'close']

        df.loc[current,str(wr_days)+'days_high_value'] = high_value
        df.loc[current,str(wr_days)+'days_low_value'] = low_value
        if high_value == low_value:
            wr_value = 100
        else:
            wr_value = (high_value - df.loc[current,'close'])/(high_value-low_value)*100
            df.loc[current,'WR'+str(wr_days)] = wr_value
    return df

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