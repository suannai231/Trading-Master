import multiprocessing
# from yahoo_fin import stock_info as si
import pandas as pd
# import numpy as np
import datetime
import os
# import chip
from multiprocessing import Pool
import time

# run_days = 100
# backward = 200

CLOSE_ABOVE_EMA = True
MACD_DIF_ABOVE_MACD_DEA = True
TURN_MINMAX = False
TURN_2575 = True
OBV_ABOVE_ZERO_DAYS_MINMAX = False
OBV_ABOVE_ZERO_DAYS_2575 = True
OBV_DIFF_RATE_MINMAX = False
OBV_DIFF_RATE_2575 = True
CUM_TURNOVER_RATE_MINMAX = False
CUM_TURNOVER_RATE_2575 = True
WR34_MINMAX = False
WR34_2575 = True
WR120_MINMAX = False
WR120_2575 = True
WR120_50_MINMAX = False
WR120_50_2575 = True
WR120_80_MINMAX = False
WR120_80_2575 = True

TURN_25 = 0.001843759
TURN_75 = 0.016290666
OBV_ABOVE_ZERO_DAYS_25 = 90
OBV_ABOVE_ZERO_DAYS_75 = 174
OBV_DIFF_RATE_25 = 0.031796562
OBV_DIFF_RATE_75 = 0.34831409
CUM_TURNOVER_RATE_25 = 2.671338854
CUM_TURNOVER_RATE_75 = 7.78641416
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
WR34_25 = 40.75829257
WR34_75 = 82.56881261
WR120_25 = 69.51219738
WR120_75 = 91.76470544
WR120_GREATER_THAN_50_DAYS_25 = 166
WR120_GREATER_THAN_50_DAYS_75 = 197
WR120_GREATER_THAN_80_DAYS_25 = 64
WR120_GREATER_THAN_80_DAYS_75 = 150

TURN_MIN = 0.023769694
TURN_MAX = 4.854261389
OBV_ABOVE_ZERO_DAYS_MIN = 8
OBV_ABOVE_ZERO_DAYS_MAX = 200
OBV_DIFF_RATE_MIN = 0.000140247
OBV_DIFF_RATE_MAX = 1
CUM_TURNOVER_RATE_MIN = 1.200638886
CUM_TURNOVER_RATE_MAX = 77.99560789
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
WR34_MIN = 0
WR34_MAX = 100
WR120_MIN = 0.757581046
WR120_MAX = 100
WR120_GREATER_THAN_50_DAYS_MIN = 60
WR120_GREATER_THAN_50_DAYS_MAX = 200
WR120_GREATER_THAN_80_DAYS_MIN = 12
WR120_GREATER_THAN_80_DAYS_MAX = 200

def screen(df):
    # if len(df) <= backward+2:
    #     return pd.DataFrame()

    # backward_startindex = len(df)-backward
    # origin_endindex = len(df)
    
    # df = df[backward_startindex:origin_endindex].reset_index(drop=True)

    # startindex = df.index[0]
    # endindex = len(df)
    lastindex = df.index[-1]

    if CLOSE_ABOVE_EMA & (df['close'][lastindex] < df['EMA34'][lastindex]):
        return pd.DataFrame()

    if MACD_DIF_ABOVE_MACD_DEA & ((df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0):
        return pd.DataFrame()

    if TURN_MINMAX & ((df['turn'][lastindex] > TURN_MAX) | (df['turn'][lastindex] < TURN_MIN)):
        return pd.DataFrame()
    if TURN_2575 & ((df['turn'][lastindex] > TURN_75) | (df['turn'][lastindex] < TURN_25)):
        return pd.DataFrame()

    # obv_above_zero_days = 0
    # for i in range(startindex,endindex):
    #     if df['OBV_DIFF'][i] > 0:
    #         obv_above_zero_days += 1
    if OBV_ABOVE_ZERO_DAYS_MINMAX & (df['turn']['obv_above_zero_days'] > OBV_ABOVE_ZERO_DAYS_MAX) | (df['turn']['obv_above_zero_days'] < OBV_ABOVE_ZERO_DAYS_MIN):
        return pd.DataFrame()
    if OBV_ABOVE_ZERO_DAYS_2575 & (df['turn']['obv_above_zero_days'] > OBV_ABOVE_ZERO_DAYS_75) | (df['turn']['obv_above_zero_days'] < OBV_ABOVE_ZERO_DAYS_25):
        return pd.DataFrame()

    if OBV_DIFF_RATE_MINMAX & ((df['OBV_DIFF_RATE'][lastindex] > OBV_DIFF_RATE_MAX) | (df['OBV_DIFF_RATE'][lastindex] < OBV_DIFF_RATE_MIN)):
        return pd.DataFrame()
    if OBV_DIFF_RATE_2575 & ((df['OBV_DIFF_RATE'][lastindex] > OBV_DIFF_RATE_75) | (df['OBV_DIFF_RATE'][lastindex] < OBV_DIFF_RATE_25)):
        return pd.DataFrame()

    if CUM_TURNOVER_RATE_MINMAX & ((df['cum_turnover'][lastindex] > CUM_TURNOVER_RATE_MAX) | (df['cum_turnover'][lastindex] < CUM_TURNOVER_RATE_MIN)):
        return pd.DataFrame()
    if CUM_TURNOVER_RATE_2575 & ((df['cum_turnover'][lastindex] > CUM_TURNOVER_RATE_75) | (df['cum_turnover'][lastindex] < CUM_TURNOVER_RATE_25)):
        return pd.DataFrame()
    # ss = chip.Cal_Chip_Distribution(df)
    # if not ss.empty:
    #     cum_chip = ss['Cum_Chip'][ss.index[-1]]
    #     if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
    #         return pd.DataFrame()
    #     chip_con = chip.Cal_Chip_Concentration(ss)
    #     if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
    #         return pd.DataFrame()

    if WR34_MINMAX & ((df.WR34[lastindex] > WR34_MAX) | (df.WR34[lastindex] < WR34_MIN)):
        return pd.DataFrame()
    if WR34_2575 & ((df.WR34[lastindex] > WR34_75) | (df.WR34[lastindex] < WR34_25)):
        return pd.DataFrame()

    if WR120_MINMAX & ((df.WR120[lastindex] > WR120_MAX) | (df.WR120[lastindex] < WR120_MIN)):
        return pd.DataFrame()
    if WR120_2575 & ((df.WR120[lastindex] > WR120_75) | (df.WR120[lastindex] < WR120_25)):
        return pd.DataFrame()

    # wr120_less_than_50_days = 0
    # for i in range(startindex,endindex):
    #     if df.WR120[i] > 50:
    #         wr120_less_than_50_days += 1
    if WR120_50_MINMAX & (df['turn']['wr120_less_than_50_days'] < WR120_GREATER_THAN_50_DAYS_MIN) | (df['turn']['wr120_less_than_50_days'] > WR120_GREATER_THAN_50_DAYS_MAX):
        return pd.DataFrame()
    if WR120_50_2575 & (df['turn']['wr120_less_than_50_days'] < WR120_GREATER_THAN_50_DAYS_25) | (df['turn']['wr120_less_than_50_days'] > WR120_GREATER_THAN_50_DAYS_75):
        return pd.DataFrame()

    # wr120_less_than_80_days = 0
    # for i in range(startindex,endindex):
    #     if df.WR120[i] > 80:
    #         wr120_less_than_80_days += 1
    if WR120_80_MINMAX & (df['turn']['wr120_less_than_80_days'] < WR120_GREATER_THAN_80_DAYS_MIN) | (df['turn']['wr120_less_than_80_days'] > WR120_GREATER_THAN_80_DAYS_MAX):
        return pd.DataFrame()
    if WR120_80_2575 & (df['turn']['wr120_less_than_80_days'] < WR120_GREATER_THAN_50_DAYS_25) | (df['turn']['wr120_less_than_80_days'] > WR120_GREATER_THAN_80_DAYS_75):
        return pd.DataFrame()

    return df

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
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].set_index('date')
        if ticker_df.empty:
            continue
        dates = ticker_df.date.unique()
        if len(dates) == 0:
            continue
        return_ticker_df = pd.DataFrame()
        start_time = time.time()
        for date in dates:
            date_ticker_df = ticker_df[date]
            result = screen(date_ticker_df)
            if not result.empty:
                return_ticker_df = return_ticker_df.append(result,ignore_index=True)
        print("%s seconds\n" %(time.time()-start_time))
        if not return_ticker_df.empty:
            return_ticker_chunk_df = return_ticker_chunk_df.append(return_ticker_df,ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/SecretNum/"
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