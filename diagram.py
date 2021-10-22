#from pandas_datareader import data
#from yahoo_fin import stock_info as si
#import yfinance as yf
import matplotlib.pyplot as plt
import pandas as pd
#import seaborn as sns
#import numpy as np
import datetime
#import time
import os
#import threading
import chip
import gc

def savediagram(df,ticker):
    fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1,figsize=(33,13), gridspec_kw={'height_ratios': [5, 1, 1, 1, 1]})
    ss = chip.Cal_Chip_Distribution(df)
    if not ss.empty:
        chip_con = chip.Cal_Chip_Concentration(ss)
        fig.suptitle(f'{ticker}'+' chip cum:'+str(ss['Cum_Chip'][ss.index[-1]])+' chip con:'+str(chip_con), fontsize=16)
    else:
        fig.suptitle(f'{ticker}', fontsize=16)

    ax1.plot(df['close'],  label='Close Price', alpha = 0.35)
    ax1.plot(df['EMA34'],  label='EMA34', alpha = 1, color='green')
    ax1.plot(df['EMA120'],  label='EMA120', alpha = 1, color='purple')
    if not ss.empty:
        ax1.plot(ss['Chip']*len(df), ss.index, label='Chip', alpha = 0.35)
    ax1.plot([0,len(df)], [df.close[df.index[-1]],df.close[df.index[-1]]], label='close Price', alpha = 0.35)
    ax1.set_ylabel('Close')

    ax2.plot(df['OBV_DIFF'],  label='OBV_DIFF', color= 'purple', lw=1)
    ax2.plot([0,len(df)], [0,0], label='Zero', color= 'red', lw=1)
    ax2.set_ylabel('OBVD')

    ax3.plot(df['MACD_dif'],  label='MACD_DIF', color= 'green', lw=1)
    ax3.plot(df['MACD_dea'],  label='MACD_DEA', color= 'purple', lw=1)
    ax3.plot([0,len(df)], [0,0], label='Zero', color= 'red', lw=1)
    ax3.set_ylabel('MACD')

    ax4.plot(df['cum_turnover'],  label='cum_turnover', color= 'purple', lw=1)
    ax4.set_ylabel('CTURN')

    ax5.plot(df['WR34'],  label='WR34', color= 'orange', lw=1)
    ax5.plot(df['WR120'],  label='WR120', color= 'purple', lw=1)
    ax5.plot([0,len(df)],  [20,20], color= 'green', lw=1)
    ax5.plot([0,len(df)],  [50,50], color= 'red', lw=1)
    ax5.plot([0,len(df)],  [80,80], color= 'green', lw=1)
    ax5.set_ylabel('WR')

    fig.savefig(diagrampath+f'/{ticker}.png')
    ax1.cla()
    ax2.cla()
    ax3.cla()
    ax4.cla()
    ax5.cla()
    plt.clf()
    plt.close('all')
    del ss
    return


diagrampath=f"C:/Python/diagram"
isPathExists = os.path.exists(diagrampath)
if not isPathExists:
    os.makedirs(diagrampath)
    #print(diagrampath+"created successfully\n")
else:
    #print(diagrampath+" dir existed\n")
    datafiles = os.listdir(diagrampath)
    for f in datafiles:
        os.remove(os.path.join(diagrampath,f))
    #print(diagrampath+" old files deleted\n")

end = datetime.date.today()

path=f"C:/Python/data"
files = os.listdir(path)

for file in files:
    if not os.path.isdir(file):
        df = pd.read_csv(path+"\\"+file)
        ticker = os.path.splitext(file)[0]
        if not df.empty:
            savediagram(df,ticker)
        del df
        gc.collect()
            #print (f'Ticker: {ticker}.png saved\n')
        #else:
            #print (f'Ticker: {ticker}.png not saved\n')

#print("Exiting diagram Main Thread")