import multiprocessing
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
# import chip
from multiprocessing import Pool

run_days = 100
backward = 200

EMA_Indicator = True
MACD_Indicator = True
turn_Indicator = True
OBV_Indicator = True
Cum_Turnover_Indicator = True
# Cum_Chip_Indicator = False
Chip_Concentration_Indicator = False
WR_Indicator = True

TURN_25 = 0.163380811
TURN_75 = 0.313519497
OBV_ABOVE_ZERO_DAYS_25 = 111.0916667
OBV_ABOVE_ZERO_DAYS_75 = 132.85
OBV_DIFF_RATE_25 = 0.456246207
OBV_DIFF_RATE_75 = 0.494789431
CUM_TURNOVER_RATE_25 = 6.427935431
CUM_TURNOVER_RATE_75 = 8.449866258
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
WR34_25 = 23.82415292
WR34_75 = 31.02580587
WR120_25 = 49.17431116
WR120_75 = 52.59873443
WR120_GREATER_THAN_50_DAYS_25 = 174.95
WR120_GREATER_THAN_50_DAYS_75 = 182.1583333
WR120_GREATER_THAN_80_DAYS_25 = 102.0666667
WR120_GREATER_THAN_80_DAYS_75 = 108.5083333

TURN_MIN = 0.083459226
TURN_MAX = 2.113499411
OBV_ABOVE_ZERO_DAYS_MIN = 102.6
OBV_ABOVE_ZERO_DAYS_MAX = 160.4666667
OBV_DIFF_RATE_MIN = 0.382331379
OBV_DIFF_RATE_MAX = 0.526553412
CUM_TURNOVER_RATE_MIN = 5.526716335
CUM_TURNOVER_RATE_MAX = 17.13358213
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
WR34_MIN = 20.77973946
WR34_MAX = 38.08395473
WR120_MIN = 42.96485185
WR120_MAX = 56.23581796
WR120_GREATER_THAN_50_DAYS_MIN = 165.3
WR120_GREATER_THAN_50_DAYS_MAX = 184.8666667
WR120_GREATER_THAN_80_DAYS_MIN = 94.4
WR120_GREATER_THAN_80_DAYS_MAX = 111.7

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

    if turn_Indicator & ((df['turn'][lastindex] > TURN_MAX) | (df['turn'][lastindex] < TURN_MIN)):
        return pd.DataFrame()

    obv_above_zero_days = 0
    for i in range(startindex,endindex):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
    if OBV_Indicator & (obv_above_zero_days > OBV_ABOVE_ZERO_DAYS_MAX) | (obv_above_zero_days < OBV_ABOVE_ZERO_DAYS_MIN):
        return pd.DataFrame()

    if OBV_Indicator & ((df['OBV_DIFF_RATE'][lastindex] > OBV_DIFF_RATE_MAX) | (df['OBV_DIFF_RATE'][lastindex] < OBV_DIFF_RATE_MIN)):
        return pd.DataFrame()

    if Cum_Turnover_Indicator & ((df['cum_turnover'][lastindex] > CUM_TURNOVER_RATE_MAX) | (df['cum_turnover'][lastindex] < CUM_TURNOVER_RATE_MIN)):
        return pd.DataFrame()

    # ss = chip.Cal_Chip_Distribution(df)
    # if not ss.empty:
    #     cum_chip = ss['Cum_Chip'][ss.index[-1]]
    #     if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
    #         return pd.DataFrame()
    #     chip_con = chip.Cal_Chip_Concentration(ss)
    #     if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
    #         return pd.DataFrame()

    if WR_Indicator & ((df.WR34[lastindex] > WR34_MAX) | (df.WR34[lastindex] < WR34_MIN)):
        return pd.DataFrame()

    if WR_Indicator & ((df.WR120[lastindex] > WR120_MAX) | (df.WR120[lastindex] < WR120_MIN)):
        return pd.DataFrame()
    
    wr120_less_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_less_than_50_days += 1
    if WR_Indicator & (wr120_less_than_50_days < WR120_GREATER_THAN_50_DAYS_MIN):
        return pd.DataFrame()

    wr120_less_than_80_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 80:
            wr120_less_than_80_days += 1
    if WR_Indicator & (wr120_less_than_80_days < WR120_GREATER_THAN_80_DAYS_MIN):
        return pd.DataFrame()

    return df[df.index==df.index[-1]]

# def run(file):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
#     df = pd.read_csv(processed_data_path+'/'+file)
#     save = screen(df)
#     if not save.empty:
#         save.to_csv(screened_data_path+'/'+file)
#     return file

def run(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        if ticker_df.empty:
            continue
        dates = ticker_df.date.unique()
        if len(dates) == 0:
            continue
        return_ticker_df = pd.DataFrame()
        for date in dates:
            date_ticker_df = ticker_df[ticker_df['date'].isin(pd.date_range(end=str(date), periods=300))].reset_index(drop=True)
            if (len(date_ticker_df)) <= backward+2:
                continue
            result = screen(date_ticker_df)
            if not result.empty:
                return_ticker_df = return_ticker_df.append(result,ignore_index=True)
        if not return_ticker_df.empty:
            return_ticker_chunk_df = return_ticker_chunk_df.append(return_ticker_df,ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
screened_data_path=f"//jack-nas/Work/Python/ScreenedData/"

if __name__ == '__main__':
    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    screened_data_files = os.listdir(screened_data_path)
    screened_data_file = str(end) + '.csv'
    if screened_data_files in screened_data_files:
        exit()

    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')
    tickers = df.ticker.unique()

    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/cores)))
    pool=Pool(cores)
    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(run, args=(ticker_chunk_df,))
        async_results.append(async_result)
    pool.close()
    del(df)
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = df.append(result,ignore_index=True)
    df.to_csv(screened_data_path + f'{end}' + '.csv')
    # os.popen(f'python C:/Code/One/find_topX_MP_One.py')