import pandas as pd
import datetime
from multiprocessing import Pool
import numpy as np
from pandas.core.frame import DataFrame
from datetime import timedelta

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
data_date = '2022-03-15'
stock_date = '2022-02-23'
date2 = '2022-02-22'
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
screened_data_path=f"//jack-nas/Work/Python/ScreenedData/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    df = pd.read_feather(processed_data_path + data_date + '.feather')
    df = df[(df['ticker'] == 'IMPP') & ((df.index==stock_date) | (df.index==date2))]
    # df = df[(df['date'] == stock_date) & (df['ticker'] == 'IMPP')]
    df.set_index('date',inplace=True)
    screen(df)