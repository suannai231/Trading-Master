import multiprocessing
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
import chip
from multiprocessing import Pool
import time

backward = 200

def cal_secret_num(df):
    if len(df) <= backward+2:
        return pd.DataFrame()

    backward_startindex = len(df)-backward
    origin_endindex = len(df)
    
    backward_df = df.iloc[backward_startindex:origin_endindex].copy()

    obv_above_zero_days = 0
    wr120_larger_than_50_days = 0
    wr120_larger_than_80_days = 0
    for i in backward_df.index:
        if backward_df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
        if backward_df.WR120[i] > 50:
            wr120_larger_than_50_days += 1
        if backward_df.WR120[i] > 80:
            wr120_larger_than_80_days += 1
    backward_df.loc[backward_df.index[-1],'obv_above_zero_days'] = obv_above_zero_days
    backward_df.loc[backward_df.index[-1],'wr120_larger_than_50_days'] = wr120_larger_than_50_days
    backward_df.loc[backward_df.index[-1],'wr120_larger_than_80_days'] = wr120_larger_than_80_days

    return backward_df.tail(1)

def cal_secret_num_2(df):
    if len(df) <= backward+2:
        return pd.DataFrame()
    obv_above_zero_days = 0
    wr120_larger_than_50_days = 0
    wr120_larger_than_80_days = 0
    for i in range(backward):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
        if df.WR120[i] > 50:
            wr120_larger_than_50_days += 1
        if df.WR120[i] > 80:
            wr120_larger_than_80_days += 1
    df.loc[df.index[backward-1],'obv_above_zero_days'] = obv_above_zero_days
    df.loc[df.index[backward-1],'wr120_larger_than_50_days'] = wr120_larger_than_50_days
    df.loc[df.index[backward-1],'wr120_larger_than_80_days'] = wr120_larger_than_80_days
    i = backward
    while i<len(df):
        removed_index = i-backward
        if df['OBV_DIFF'][removed_index] > 0:
            obv_above_zero_days -= 1
        if df.WR120[removed_index] > 50:
            wr120_larger_than_50_days -= 1
        if df.WR120[removed_index] > 80:
            wr120_larger_than_80_days -= 1
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1
        if df.WR120[i] > 50:
            wr120_larger_than_50_days += 1
        if df.WR120[i] > 80:
            wr120_larger_than_80_days += 1
        df.loc[df.index[i],'obv_above_zero_days'] = obv_above_zero_days
        df.loc[df.index[i],'wr120_larger_than_50_days'] = wr120_larger_than_50_days
        df.loc[df.index[i],'wr120_larger_than_80_days'] = wr120_larger_than_80_days
        i+=1
    return df[backward-1:len(df)-1]

def run(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df.loc[ticker_chunk_df.ticker==ticker].set_index('date')
        if (len(ticker_df)) <= backward+2:
            continue
        # if ticker_df.empty:
        #     continue
        # dates = ticker_df.index.unique()
        # if len(dates) == 0:
        #     continue
        # results_df = pd.DataFrame()
        start_time = time.time()
        # for date in dates:
        #     date_ticker_df = ticker_df.loc[ticker_df.index.isin(pd.date_range(end=str(date), periods=300))]
        #     result_df = cal_secret_num(date_ticker_df)
        #     if not result_df.empty:
        #         results_df = results_df.append(result_df)
        result_df = cal_secret_num_2(ticker_df)
        print("%s seconds\n" %(time.time()-start_time))
        if not result_df.empty:
            return_ticker_chunk_df = return_ticker_chunk_df.append(result_df)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
topX_data_path=f"//jack-nas/Work/Python/SecretNum/"

if __name__ == '__main__':
    isPathExists = os.path.exists(topX_data_path)
    if not isPathExists:
        os.makedirs(topX_data_path)

    topX_data_files = os.listdir(topX_data_path)
    topX_data_file = str(end) + '.feather'
    if topX_data_file in topX_data_files:
        exit()

    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')
    tickers = df.ticker.unique()

    cores = multiprocessing.cpu_count()
    ticker_chunk_list = list(chunks(tickers,int(len(tickers)/cores)))
    pool = Pool(cores)
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
            df = df.append(async_result.get())
    df.reset_index(drop=False,inplace=True)
    df.to_feather(topX_data_path + f'{end}' + '.feather')

    os.popen(f'python C:/Code/One/analyze_secret_num_MP_One.py')