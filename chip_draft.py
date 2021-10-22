#from pandas_datareader import data
from yahoo_fin import stock_info as si
#import yfinance as yf
#import matplotlib.pyplot as plt
#import pandas as pd
#import seaborn as sns
import numpy as np
import datetime
#import time
import os
import threading

#筹码的价位分布计算
instruments=['601700.SHA']#这里尽量包含从上市日期开始到最后的数据
df=D.history_data(instruments,start_date='2005-01-02',end_date='2018-07-04',fields=['open','close','adjust_factor','turn','volume'])#获取历史数据
df['real_close']=df['close']/df['adjust_factor']#获取真实收盘价
df['real_open']=df['open']/df['adjust_factor']#获取真实开盘价
df['turn']=df['turn']/100#获取换手率
df['avg_price']=np.round(df['real_close']+df['real_open'])/2#计算每日平均成本,这里按照0.5元一个价位做分析
df=df.sort_values(by='date',ascending=False).reset_index(drop=True)#日期按降序排列
df['turn_tomo']=df['turn'].shift(1)#计算明日的换手率
df['remain_day']=1-df['turn_tomo']#计算当日的剩余筹码比例
#假设N日后，上市第一天的剩余筹码比率就是每日剩余比例的累乘即：剩余筹码比例=(1-明天换手率)*(1-后日换手率)*...*(1-最新日换手率),以此类推各日的剩余筹码
df['remain_his']=df['remain_day'].cumprod()*df['turn']
df['remain_his']=df['remain_his'].fillna(df['turn'])#最新一日的筹码就是当日的换手率
#关键统计，统计最后一天的各价位历史筹码堆积量（百分比）
ss=df.groupby('avg_price')[['remain_his']].sum().rename(columns={'remain_his':'筹码量'})
ss.head(10)
#检查一下各价位筹码总和是不是1
ss['筹码量'].sum()

#筹码理论的Winner指标
#计算end_date时某一价位的获利比例
pp=ss.reset_index()
pp[pp.avg_price<=3.5]['筹码量'].sum()
#计算end_date时收盘价的获利比例
pp[pp.avg_price<=df['real_close'].iloc[-1]]['筹码量'].sum()

#筹码理论的Cost指标
#给定累计获利比率winner_ratio,计算对应的价位，表示在此价位上winner_ratio的筹码处于获利状态
winner_ratio=0.5
for i in range(len(ss)-1):
    if ss['筹码累积量'].iloc[i] < winner_ratio and ss['筹码累积量'].iloc[i+1] > winner_ratio:
        cost=ss.index[i]
        cost
        5.5

#计算全市场各股票的winner指标
instruments=['601700.SHA','601699.SHA']#这里尽量包含从上市日期开始到最后的数据
df=D.history_data(instruments,start_date='2005-01-02',end_date='2018-02-14',fields=['open','close','adjust_factor','turn','volume'])#获取历史数据
df['real_close']=df['close']/df['adjust_factor']#获取真实收盘价
df['real_open']=df['open']/df['adjust_factor']#获取真实开盘价
df['turn']=df['turn']/100
df['avg_price']=np.round(df['real_close']+df['real_open'])/2#计算每日平均成本,这里按照0.5元一个价位做分析
df=df.sort_values(by='date',ascending=False).reset_index(drop=True)#日期按降序排列
df['turn_tomo']=df.groupby('instrument')['turn'].apply(lambdax:x.shift(1))#计算明日的换手率
df['remain_day']=1-df['turn_tomo']#计算当日的剩余筹码比例
#假设N日后，上市第一天的剩余筹码比率就是每日剩余比例的累乘即：剩余筹码比例=(1-明天换手率)*(1-后日换手率)*...*(1-最新日换手率),以此类推各日的剩余筹码
df['remain_his']=df.groupby('instrument')['remain_day'].apply(lambdax:x.cumprod())
df['remain_his']=df['remain_his']*df['turn']
df['remain_his']=df['remain_his'].fillna(df['turn'])#最新一日的筹码就是当日的换手率
#关键统计，统计最后一天的各价位历史筹码堆积量（百分比）
ss=df.groupby(['instrument','avg_price'])[['remain_his']].sum().rename(columns={'remain_his':'筹码量'}).reset_index()
real_close=df.groupby('instrument')[['real_close']].apply(lambdax:x.iloc[0]).reset_index()
pp=ss.merge(real_close,on='instrument')#计算end_date时收盘价的获利比例
winner=pp[pp.avg_price<=pp.real_close].groupby('instrument')[['筹码量']].sum().rename(columns={'筹码量':'winner'})
winner
#检查各股票各价位的筹码总和是否为1
ss.groupby('instrument')['筹码量'].sum()

#计算全市场各股票每日的winner指标
instruments=['601700.SHA','601699.SHA']#这里尽量包含从上市日期开始到最后的数据
df_all=D.history_data(instruments,start_date='2005-01-02',end_date='2018-02-14',fields=['open','close','adjust_factor','turn','volume'])#获取历史数据
def cal_winner_day(df_all):
    winner=[]
    for k in list(df_all.date):
        df=df_all[df_all.date<=k]
        df['real_close']=df['close']/df['adjust_factor']#获取真实收盘价
        df['real_open']=df['open']/df['adjust_factor']#获取真实开盘价
        df['turn']=df['turn']/100#获取换手率
        df['avg_price']=np.round(df['real_close']+df['real_open'])/2#计算每日平均成本,这里按照0.5元一个价位做分析
        df=df.sort_values(by='date',ascending=False).reset_index(drop=True)#日期按降序排列
        df['turn_tomo']=df['turn'].shift(1)#计算明日的换手率
        df['remain_day']=1-df['turn_tomo']#计算当日的剩余筹码比例#假设N日后，上市第一天的剩余筹码比率就是每日剩余比例的累乘即：剩余筹码比例=(1-明天换手率)*(1-后日换手率)*...*(1-最新日换手率),以此类推各日的剩余筹码
        df['remain_his']=df['remain_day'].cumprod()
        df['remain_his']=df['remain_his']*df['turn']
        df['remain_his']=df['remain_his'].fillna(df['turn'])#最新一日的筹码就是当日的换手率
        #关键统计，统计最后一天的各价位历史筹码堆积量（百分比）
        ss=df.groupby('avg_price')[['remain_his']].sum().rename(columns={'remain_his':'筹码量'}).reset_index()
        ss['real_close']=df['real_close'].iloc[0]#计算end_date时收盘价的获利比例
        winner_day=ss[ss.avg_price<=ss.real_close]['筹码量'].sum()
        winner.append(winner_day)
        result=pd.DataFrame({'winner':winner},index=df_all.date)
        return result
winner_all=df_all.groupby('instrument').apply(cal_winner_day)
winner_all.reset_index().sort_values(by='date',ascending=False).head()