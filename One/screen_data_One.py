import multiprocessing
import pandas as pd
import datetime
import os
from multiprocessing import Pool
import numpy as np

CLOSE_ABOVE_EMA = True
MACD_DIF_ABOVE_MACD_DEA = True
# TURN_MINMAX = False
# TURN_2575 = False
# OBV_ABOVE_ZERO_DAYS_MINMAX = False
# OBV_ABOVE_ZERO_DAYS_2575 = True
# OBV_DIFF_RATE_MINMAX = False
# OBV_DIFF_RATE_2575 = False
TURN_RATE = True
CUM_TURN_RATE_MINMAX = False
CUM_TURN_RATE_2575 = True
WR21 = True
WR42 = True
# WR34_MINMAX = False
# WR34_2575 = True
# WR120_MINMAX = False
# WR120_2575 = True
# WR120_50_MINMAX = False
# WR120_50_2575 = True
# WR120_80_MINMAX = False
# WR120_80_2575 = True

# TURN_25 = 0 # disabled
# TURN_75 = 0.35  # disabled
# OBV_ABOVE_ZERO_DAYS_25 = 122    # tested EH SNDL
# OBV_ABOVE_ZERO_DAYS_75 = 200
# OBV_DIFF_RATE_25 = 0    # disabled
# OBV_DIFF_RATE_75 = 1    # disabled
TURN_RATE_LOW = 0.05
CUM_TURN_RATE_25 = 0
CUM_TURN_RATE_75 = 1.84 # tested RCMT HX CHEK
WR21_HIGH = 50
WR21_LOW = 0
WR42_HIGH = 50
WR42_LOW = 0
# WR34_25 = 0
# WR34_75 = 28
# WR120_25 = 0
# WR120_75 = 28
# WR120_GREATER_THAN_50_DAYS_25 = 107  # tested CEI RCMT AEHR EH
# WR120_GREATER_THAN_50_DAYS_75 = 200
# WR120_GREATER_THAN_80_DAYS_25 = 34 # Tested ACY CPSH AEHR
# WR120_GREATER_THAN_80_DAYS_75 = 200
backward = 150
CUM_TURN_LOW = 1
INCREASE_LOW = 0.8

def screen(df):
    lastindex = df.index[-1]
    upper_cum_turn = df.loc[lastindex]['upper_cum_turn']
    lower_cum_turn = df.loc[lastindex]['lower_cum_turn']
    cum_turn = upper_cum_turn+lower_cum_turn
    # if cum_turn < CUM_TURN_LOW:
    #     return df
    
    days_high_value_str = str(backward)+'days_high_value'
    days_high_values = df.loc[lastindex][days_high_value_str]
    days_low_value_str = str(backward)+'days_low_value'
    days_low_values = df.loc[lastindex][days_low_value_str]
    increase = (days_high_values - days_low_values)/days_low_values
    if (increase < INCREASE_LOW) and (cum_turn < CUM_TURN_LOW):
        return df

    return pd.DataFrame()

    # if TURN_RATE & (df['turn'][lastindex] < TURN_RATE_LOW):
    #     return pd.DataFrame()

    # if CLOSE_ABOVE_EMA & ((df['close'][lastindex] < df['EMA5'][lastindex]) | (df['EMA5'][lastindex] < df['EMA10'][lastindex]) | (df['EMA10'][lastindex] < df['EMA20'][lastindex]) | (df['EMA5'][lastindex] < df['EMA60'][lastindex]) | (df['EMA10'][lastindex] < df['EMA60'][lastindex])):
    #     return pd.DataFrame()

    # if MACD_DIF_ABOVE_MACD_DEA & ((df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0):
    #     return pd.DataFrame()

    # if TURN_2575 & ((df['turn'][lastindex] > TURN_75) | (df['turn'][lastindex] < TURN_25)):
    #     return pd.DataFrame()

    # if OBV_ABOVE_ZERO_DAYS_2575 & ((df['obv_above_zero_days'][lastindex] > OBV_ABOVE_ZERO_DAYS_75) | (df['obv_above_zero_days'][lastindex] < OBV_ABOVE_ZERO_DAYS_25)):
    #     return pd.DataFrame()

    # if OBV_DIFF_RATE_2575 & ((df['OBV_DIFF_RATE'][lastindex] > OBV_DIFF_RATE_75) | (df['OBV_DIFF_RATE'][lastindex] < OBV_DIFF_RATE_25)):
    #     return pd.DataFrame()

    # cum_turn_rate = df['upper_cum_turn'][lastindex]/df['lower_cum_turn'][lastindex]
    # if CUM_TURN_RATE_2575 & ((cum_turn_rate > CUM_TURN_RATE_75) | (cum_turn_rate < CUM_TURN_RATE_25)):
    #     return pd.DataFrame()

    # if WR21 & ((df.WR21[lastindex] > WR21_HIGH) | (df.WR21[lastindex] < WR21_LOW)):
    #     return pd.DataFrame()

    # if WR42 & ((df.WR42[lastindex] > WR42_HIGH) | (df.WR42[lastindex] < WR42_LOW)):
    #     return pd.DataFrame()

    # if WR34_2575 & ((df.WR34[lastindex] > WR34_75) | (df.WR34[lastindex] < WR34_25)):
    #     return pd.DataFrame()

    # if WR120_2575 & ((df.WR120[lastindex] > WR120_75) | (df.WR120[lastindex] < WR120_25)):
    #     return pd.DataFrame()

    # if WR120_50_2575 & ((df['wr120_larger_than_50_days'][lastindex] < WR120_GREATER_THAN_50_DAYS_25) | (df['wr120_larger_than_50_days'][lastindex] > WR120_GREATER_THAN_50_DAYS_75)):
    #     return pd.DataFrame()

    # if WR120_80_2575 & ((df['wr120_larger_than_80_days'][lastindex] < WR120_GREATER_THAN_80_DAYS_25) | (df['wr120_larger_than_80_days'][lastindex] > WR120_GREATER_THAN_80_DAYS_75)):
    #     return pd.DataFrame()

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

def run(ticker_chunk_df,qfq):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        if ticker_df.empty:
            continue
        return_ticker_df = pd.DataFrame()
        # start_time = time.time()
        for date in ticker_df.index:
            date_ticker_df = ticker_df[ticker_df.index==date]
            # if(is_qfq_in_period(date_ticker_df,qfq,60)):
            #     continue
            result = screen(date_ticker_df)
            if not result.empty:
                return_ticker_df = return_ticker_df.append(result)
        # print("%s seconds\n" %(time.time()-start_time))
        if not return_ticker_df.empty:
            return_ticker_chunk_df = return_ticker_chunk_df.append(return_ticker_df)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
screened_data_path=f"//jack-nas/Work/Python/ScreenedData/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    screened_data_files = os.listdir(screened_data_path)
    screened_data_file = str(end) + '.csv'
    if screened_data_file in screened_data_files:
        exit()

    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')
    df = df[df['date'] > '2017-01-01']
    qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    qfq = qfq[qfq['date'] > '2017-01-01']

    tickers = df.ticker.unique()
    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/cores)))
    pool=Pool(cores)
    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run, args=(ticker_chunk_df,qfq))
        async_results.append(async_result)
    pool.close()
    del(df)
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = df.append(result)
    df.reset_index(drop=False,inplace=True)
    df.to_csv(screened_data_path + f'{end}' + '.csv')