#from pandas_datareader import data
from yahoo_fin import stock_info as si
#import yfinance as yf
#import matplotlib.pyplot as plt
import pandas as pd
#import seaborn as sns
import numpy as np
import datetime
#import time
#import os
#import threading

def Cal_Chip_Distribution(df):
    # cap = df.marketCap[df.index[-1]]
    #shares = df.shares[df.index[-1]]

    #筹码的价位分布计算
    #instruments=['601700.SHA']#这里尽量包含从上市日期开始到最后的数据
    #df=D.history_data(instruments,start_date='2005-01-02',end_date='2018-07-04',fields=['open','close','adjust_factor','turn','volume'])#获取历史数据
    #df['real_close']=df['close']/df['adjust_factor']#获取真实收盘价
    #df['real_open']=df['open']/df['adjust_factor']#获取真实开盘价
    #df['turn']=df.volume/shares#获取换手率
    
    for i in range(0,len(df)):
        if(df.turn[i]>=1):
            return pd.DataFrame()

    
    df=df.sort_values(by='date',ascending=False).reset_index(drop=True)#日期按降序排列
    df['avg_price']=np.round(df['close']+df['open'],1)/2#计算每日平均成本,这里按照0.05元一个价位做分析
    df['turn_tomo']=df['turn'].shift(1)#计算明日的换手率
    df['remain_day']=1-df['turn_tomo']#计算当日的剩余筹码比例
    #假设N日后，上市第一天的剩余筹码比率就是每日剩余比例的累乘即：剩余筹码比例True=(1-明天换手率)*(1-后日换手率)*...*(1-最新日换手率),以此类推各日的剩余筹码
    df['remain_his']=df['remain_day'].cumprod()*df['turn']
    df['remain_his']=df['remain_his'].fillna(df['turn'])#最新一日的筹码就是当日的换手率
    #关键统计，统计最后一天的各价位历史筹码堆积量（百分比）
    ss=df.groupby('avg_price')[['remain_his']].sum().rename(columns={'remain_his':'Chip'})
    #ss=ss.reset_index()
    #print("Chip:\n")
    #print(ss)
    #检查一下各价位筹码总和是不是1
    #print("筹码总和:"+str(ss['Chip'].sum()))

    #筹码理论的Cost指标
    ss['Cum_Chip']=ss['Chip'].cumsum()
    #print("Cum_Chip:\n")
    #print(ss)
    return ss

def Winner(price,ss):
    #筹码理论的Winner指标
    #计算end_date时某一价位的获利比例
    return ss[ss.avg_price<=price]['Chip'].sum()
    #计算end_date时收盘价的获利比例
    #print("收盘价的获利比例:"+str(pp[pp.avg_price<=df['close'].iloc[0]]['Chip'].sum()))

def Cost(winner_ratio,ss):
    #给定累计获利比率winner_ratio,计算对应的价位，表示在此价位上winner_ratio的筹码处于获利状态
    cost = 0
    for i in range(len(ss)-1):
        if (ss['Cum_Chip'].iloc[i] < winner_ratio) and (ss['Cum_Chip'].iloc[i+1] > winner_ratio):
            cost=ss.index[i]
    return cost

def Cal_Chip_Concentration(ss):
    if ss.empty:
        return 0
    Cost95 = Cost(0.95,ss)
    Cost5 = Cost(0.05,ss)
    CostHigh = ss.index[-1]
    CostLow = ss.index[0]
    if Cost95 != 0 and Cost5 !=0:
        concentration = (Cost95-Cost5)/(Cost95+Cost5)
    elif Cost95 == 0 and Cost5 !=0:
        concentration = (CostHigh-Cost5)/(CostHigh+Cost5)
    elif Cost95 != 0 and Cost5 ==0:
        concentration = (Cost95-CostLow)/(Cost95+CostLow)
    else:
        concentration = (CostHigh-CostLow)/(CostHigh+CostLow)
    return concentration

'''
start = datetime.datetime.now() - datetime.timedelta(days=365)
end = datetime.date.today()
ticker = "caap"
cap = si.get_quote_data(ticker)["marketCap"]
df = si.get_data(ticker,start, end)
df["marketCap"] = cap
df=df.reset_index(drop=False)
df.rename(columns={"index":"date"}, inplace=True)
CalculateConcentration(df,cap)
'''

'''
start = datetime.datetime.now() - datetime.timedelta(days=365*10)
end = datetime.date.today()
ticker = "imte"

cap = data.get_quote_yahoo(ticker)['marketCap'][0]
df = data.DataReader(ticker, "yahoo", start, end)

ss = CalculateChipDistribution(df,cap)
print(CalculateConcentration(ss))

plt.plot(ss['Chip'], ss.index, label='Chip', alpha = 0.35)
plt.plot([0,1], [df.close[df.index[0]],df.close[df.index[0]]], label='close Price', alpha = 0.35)
plt.show()
'''