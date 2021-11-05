import pandas as pd
import datetime
from multiprocessing import Pool
import numpy as np

CLOSE_ABOVE_EMA = True
MACD_DIF_ABOVE_MACD_DEA = True
TURN_MINMAX = False
TURN_2575 = False
OBV_ABOVE_ZERO_DAYS_MINMAX = False
OBV_ABOVE_ZERO_DAYS_2575 = True
OBV_DIFF_RATE_MINMAX = False
OBV_DIFF_RATE_2575 = False
CUM_TURN_RATE_MINMAX = False
CUM_TURN_RATE_2575 = True
WR34_MINMAX = False
WR34_2575 = True
WR120_MINMAX = False
WR120_2575 = True
WR120_50_MINMAX = False
WR120_50_2575 = True
WR120_80_MINMAX = False
WR120_80_2575 = True

TURN_25 = 0 # disabled
TURN_75 = 0.35  # disabled
OBV_ABOVE_ZERO_DAYS_25 = 122    # tested EH SNDL
OBV_ABOVE_ZERO_DAYS_75 = 200
OBV_DIFF_RATE_25 = 0    # disabled
OBV_DIFF_RATE_75 = 1    # disabled
CUM_TURN_RATE_25 = 0
CUM_TURN_RATE_75 = 0.2 # tested RCMT
WR34_25 = 0
WR34_75 = 28
WR120_25 = 0
WR120_75 = 28
WR120_GREATER_THAN_50_DAYS_25 = 107  # tested CEI RCMT AEHR EH
WR120_GREATER_THAN_50_DAYS_75 = 200
WR120_GREATER_THAN_80_DAYS_25 = 34 # Tested ACY CPSH AEHR
WR120_GREATER_THAN_80_DAYS_75 = 200

def screen(df):
    lastindex = df.index[-1]

    if CLOSE_ABOVE_EMA & (df['close'][lastindex] < df['EMA34'][lastindex]):
        return pd.DataFrame()

    if MACD_DIF_ABOVE_MACD_DEA & ((df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0):
        return pd.DataFrame()

    if TURN_2575 & ((df['turn'][lastindex] > TURN_75) | (df['turn'][lastindex] < TURN_25)):
        return pd.DataFrame()

    if OBV_ABOVE_ZERO_DAYS_2575 & ((df['obv_above_zero_days'][lastindex] > OBV_ABOVE_ZERO_DAYS_75) | (df['obv_above_zero_days'][lastindex] < OBV_ABOVE_ZERO_DAYS_25)):
        return pd.DataFrame()

    if OBV_DIFF_RATE_2575 & ((df['OBV_DIFF_RATE'][lastindex] > OBV_DIFF_RATE_75) | (df['OBV_DIFF_RATE'][lastindex] < OBV_DIFF_RATE_25)):
        return pd.DataFrame()

    cum_turn_rate = df['upper_cum_turn'][lastindex]/df['lower_cum_turn'][lastindex]
    if CUM_TURN_RATE_2575 & ((cum_turn_rate > CUM_TURN_RATE_75) | (cum_turn_rate < CUM_TURN_RATE_25)):
        return pd.DataFrame()

    if WR34_2575 & ((df.WR34[lastindex] > WR34_75) | (df.WR34[lastindex] < WR34_25)):
        return pd.DataFrame()

    if WR120_2575 & ((df.WR120[lastindex] > WR120_75) | (df.WR120[lastindex] < WR120_25)):
        return pd.DataFrame()

    if WR120_50_2575 & ((df['wr120_larger_than_50_days'][lastindex] < WR120_GREATER_THAN_50_DAYS_25) | (df['wr120_larger_than_50_days'][lastindex] > WR120_GREATER_THAN_50_DAYS_75)):
        return pd.DataFrame()

    if WR120_80_2575 & ((df['wr120_larger_than_80_days'][lastindex] < WR120_GREATER_THAN_80_DAYS_25) | (df['wr120_larger_than_80_days'][lastindex] > WR120_GREATER_THAN_80_DAYS_75)):
        return pd.DataFrame()

    return df

def is_qfq_in_period(df,qfq,period):
    ticker = df.loc[df.index[-1],'ticker']
    ticker_date = df.index[-1]
    for date in qfq[qfq.ticker==ticker].date:   # remove qfq
        start = ticker_date.date()
        end = date.date()
        busdays = np.busday_count( start, end)
        if (busdays > 0) & (busdays<=period+1):
            return True
        elif (busdays < 0) & (busdays>=-200):
            return True
    return False

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
screened_data_path=f"//jack-nas/Work/Python/ScreenedData/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    qfq = qfq[qfq['date'] > '2017-01-01']
    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')
    df = df[(df['date'] == '2021-09-29') & (df['ticker'] == 'HX')]
    df.set_index('date',inplace=True)
    if(is_qfq_in_period(df,qfq,60)):
        exit()
    screen(df)