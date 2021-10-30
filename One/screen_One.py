import multiprocessing
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
# import chip
from multiprocessing import Pool

run_days = 365
backward = 200

EMA_Indicator = True
MACD_Indicator = True
OBV_Indicator = True
Cum_Turnover_Indicator = True
# Cum_Chip_Indicator = False
Chip_Concentration_Indicator = False
WR_Indicator = True

obv_convergence = 1
obv_above_zero_days_bar = 0.62
cum_turnover_rate = 7.99
# cum_chip_bar = 0.8
# chip_concentration_bar = 0.4
wr34_bar = 27.48
wr120_bar = 50.36
wr120_greater_than_50_days_bar = 0.89
wr120_greater_than_80_days_bar = 0.52

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
    
    obv_above_zero_days = 0
    for i in range(startindex,endindex):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
    if OBV_Indicator & (obv_above_zero_days/backward < obv_above_zero_days_bar):
        return pd.DataFrame()

    if OBV_Indicator & (df['OBV_DIFF_RATE'][lastindex] > obv_convergence):
        return pd.DataFrame()

    if Cum_Turnover_Indicator & (df['cum_turnover'][lastindex] < cum_turnover_rate):
        return pd.DataFrame()

    # ss = chip.Cal_Chip_Distribution(df)
    # if not ss.empty:
    #     cum_chip = ss['Cum_Chip'][ss.index[-1]]
    #     if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
    #         return pd.DataFrame()
    #     chip_con = chip.Cal_Chip_Concentration(ss)
    #     if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
    #         return pd.DataFrame()

    if WR_Indicator & ((df.WR34[lastindex] > wr34_bar) | (df.WR120[lastindex] > wr120_bar)):
        return pd.DataFrame()

    wr120_less_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_less_than_50_days += 1
    if WR_Indicator & (wr120_less_than_50_days/backward < wr120_greater_than_50_days_bar):
        return pd.DataFrame()

    wr120_less_than_80_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 80:
            wr120_less_than_80_days += 1
    if WR_Indicator & (wr120_less_than_80_days/backward < wr120_greater_than_80_days_bar):
        return pd.DataFrame()

    return df[df.index==df.index[-1]]

# def run(file):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
#     df = pd.read_csv(processed_data_path+'/'+file)
#     save = screen(df)
#     if not save.empty:
#         save.to_csv(screened_data_path+'/'+file)
#     return file

def run(df,date_chunk):
    tickers_dates_df = pd.DataFrame()
    for date in date_chunk:
        tickers = df[df['date']==str(date)].ticker.unique()
        tickers_date_df = pd.DataFrame()
        for ticker in tickers:
            ticker_date_df = df[(df['date'].isin(pd.date_range(end=str(date), periods=300))) & (df.ticker == ticker)].reset_index(drop=True)
            if (len(ticker_date_df)) <= backward+2:
                continue
            result = screen(ticker_date_df)
            if not result.empty:
                tickers_date_df = tickers_date_df.append(result,ignore_index=True)
        if not tickers_date_df.empty:
            tickers_dates_df = tickers_dates_df.append(tickers_date_df,ignore_index=True)
        # history_screened_data_path = screened_data_path + f'{date}'
        # results.to_csv(history_screened_data_path + '.csv')
    return tickers_dates_df

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

    date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]

    cores = multiprocessing.cpu_count()
    date_chunk_list = list(chunks(date_list,cores))
    pool=Pool(cores)
    async_results = []
    for date_chunk in date_chunk_list:
        last_date_in_date_chunk = date_chunk[-1]
        extend_date_list = pd.date_range(end=str(last_date_in_date_chunk), periods=300)
        date_chunk_df = df[df['date'].isin(date_chunk) | df['date'].isin(extend_date_list)]
        async_result = pool.apply_async(run, args=(date_chunk_df,date_chunk))
        async_results.append(async_result)
    pool.close()
    del(df)
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = df.append(result,ignore_index=True)
    df.to_csv(screened_data_path + f'{end}' + '.csv')
