from yahoo_fin import stock_info as si
import yfinance as yf
import os
import multiprocessing
import akshare as ak
import pandas as pd
import datetime
from multiprocessing import Pool
import numpy as np
from pandas.core.frame import DataFrame
from datetime import timedelta

def cal_basics(df):
    # startindex = 0
    # endindex = len(df)
    # lastindex = len(df)-1

    df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
    df['change_5days'] = (df.close.shift(-5)- df.close)/df.close
    df['change_10days'] = (df.close.shift(-10)- df.close)/df.close
    df['change_15days'] = (df.close.shift(-15)- df.close)/df.close
    df['change_20days'] = (df.close.shift(-20)- df.close)/df.close
    df['change_25days'] = (df.close.shift(-25)- df.close)/df.close
    df['change_30days'] = (df.close.shift(-30)- df.close)/df.close
    df['change_35days'] = (df.close.shift(-35)- df.close)/df.close
    df['change_40days'] = (df.close.shift(-40)- df.close)/df.close
    df['change_45days'] = (df.close.shift(-45)- df.close)/df.close
    df['change_50days'] = (df.close.shift(-50)- df.close)/df.close
    df['change_55days'] = (df.close.shift(-55)- df.close)/df.close
    df['change_60days'] = (df.close.shift(-60)- df.close)/df.close

    shares = df.iloc[-1]['shares']
    df['turn'] = df.volume/shares

    ema20 = df['close'].ewm(span = 20, adjust = False).mean()
    ema60 = df['close'].ewm(span = 60, adjust = False).mean()

    df['EMA20'] = ema20
    df['EMA60'] = ema60

    return df

def screen(df):
    close = df.iloc[-1]['close']
    last_ema20 = df.iloc[-1]['EMA20']
    volume = df.iloc[-1]['volume']
    turn = df.iloc[-1]['turn']

    close2 = df.iloc[-2]['close']
    last2_ema20 = df.iloc[-2]['EMA20']
    volume2 = df.iloc[-2]['volume']

    if (close2<=last2_ema20) and (close>=last_ema20) and (turn >= 0.1):
        return df.tail(1)

    return pd.DataFrame()


end = datetime.date.today()
# data_date = '2022-03-15'
# stock_date = '2022-02-23'
# date2 = '2022-02-22'
# processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
# screened_data_path=f"//jack-nas/Work/Python/ScreenedData/"
# qfq_path = '//jack-nas/Work/Python/RawData/'
# ticker = ""

if __name__ == '__main__':
    # df = pd.read_feather(processed_data_path + data_date + '.feather')
    ticker = yf.Ticker("MCOA")
    floatShares = ticker.info['floatShares']
    # quote_data = si.get_quote_data('MCOA')
    # shares = quote_data['sharesOutstanding']
    df = si.get_data('MCOA','2022-01-01', '2022-03-25')
    df["shares"] = floatShares
    df = cal_basics(df)
    df = df[((df.index=='2022-03-23') | (df.index=='2022-03-24'))]
    # df = df[(df['date'] == stock_date) & (df['ticker'] == 'IMPP')]
    # df.set_index('date',inplace=True)
    screen(df)