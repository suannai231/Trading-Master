import multiprocessing
from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
import chip
from multiprocessing import Pool
import socket

run_days = 365*5
topX = 20
backward = 200

def cal_secret_num(df):
    if len(df) <= backward+2:
        return pd.DataFrame()

    backward_startindex = len(df)-backward
    origin_endindex = len(df)
    
    df = df[backward_startindex:origin_endindex].reset_index(drop=True)

    startindex = df.index[0]
    endindex = len(df)
    lastindex = df.index[-1]
    
    ema = df['close'][lastindex] < df['EMA34'][lastindex]

    macd = (df['MACD_dif'][lastindex]-df['MACD_dea'][lastindex]) < 0
    
    obv_above_zero_days = 0
    for i in range(startindex,endindex):
        if df['OBV_DIFF'][i] > 0:
            obv_above_zero_days += 1

    OBV_DIFF_RATE = df['OBV_DIFF_RATE'][lastindex]

    cum_turnover = df['cum_turnover'][lastindex]

    cum_chip = 0
    chip_con = 1
    ss = chip.Cal_Chip_Distribution(df)
    if not ss.empty:
        cum_chip = ss['Cum_Chip'][ss.index[-1]]
        chip_con = chip.Cal_Chip_Concentration(ss)

    wr34 = df.WR34[lastindex]
    wr120 = df.WR120[lastindex]

    wr120_larger_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_larger_than_50_days += 1

    wr120_larger_than_80_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 80:
            wr120_larger_than_80_days += 1

    ticker_data = [df.date[lastindex],df.ticker[lastindex],df.change[lastindex],df.change_5days[lastindex],df.change_10days[lastindex],df.change_15days[lastindex],df.change_20days[lastindex],df.change_25days[lastindex],df.change_30days[lastindex],df.change_35days[lastindex],df.change_40days[lastindex],df.change_45days[lastindex],df.change_50days[lastindex],df.change_55days[lastindex],df.change_60days[lastindex],df.turn[lastindex],ema,macd,obv_above_zero_days,OBV_DIFF_RATE,cum_turnover,cum_chip,chip_con,wr34,wr120,wr120_larger_than_50_days,wr120_larger_than_80_days]

    return ticker_data

def run(df):
    ticker_data_list = []
    tickers = df.ticker.unique()
    for ticker in tickers:
        ticker_df = df[df.ticker==ticker].reset_index(drop=True)
        ticker_data = cal_secret_num(ticker_df)
        if len(ticker_data) !=0:
            ticker_data_list.append(ticker_data)
    ticker_data_list_df = pd.DataFrame(ticker_data_list, columns = ['date','ticker','change','change_5days','change_10days','change_15days','change_20days','change_25days','change_30days','change_35days','change_40days','change_45days','change_50days','change_55days','change_60days','turn','ema','macd','obv_above_zero_days','OBV_DIFF_RATE','cum_turnover','cum_chip','chip_con','wr34','wr120','wr120_larger_than_50_days','wr120_larger_than_80_days'])
    return ticker_data_list_df

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
topX_data_path=f"//jack-nas/Work/Python/TopX/"

if __name__ == '__main__':
    topX_data_files = os.listdir(topX_data_path)
    topX_data_file = str(end) + '.feather'
    if topX_data_file in topX_data_files:
        exit()

    isPathExists = os.path.exists(topX_data_path)
    if not isPathExists:
        os.makedirs(topX_data_path)

    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')

    date_list = []
    if socket.gethostname() == 'Jack-LC':
        date_list = [end - datetime.timedelta(days=x) for x in range(1,run_days)]
    else:
        date_list = [end - datetime.timedelta(days=x) for x in range(run_days,run_days*2)]

    cores = multiprocessing.cpu_count()
    pool = Pool(cores*3)
    async_results = []
    for date in date_list:
        date_df = df[df.date==str(date)].reset_index(drop=True)
        topX_tickers = date_df.nlargest(topX,'change').ticker
        selected_df = df[(df['date'].isin(pd.date_range(end=str(date), periods=300))) & df['ticker'].isin(topX_tickers)].reset_index(drop=True)
        if not selected_df.empty:
            async_result = pool.apply_async(run, args=(selected_df,))
            async_results.append(async_result)
    pool.close()
    pool.join()

    del(df)
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = df.append(async_result.get())
    df.reset_index(drop=True,inplace=True)
    df.to_feather(topX_data_path + f'{end}' + '.feather')

    # os.popen(f'python C:/Users/jayin/OneDrive/Code/analyze_topX_MP.py')