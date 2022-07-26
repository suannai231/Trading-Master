import multiprocessing
import pandas as pd
import datetime
import os
import sys
from multiprocessing import Pool
import numpy as np
from datetime import timedelta

def screen(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    # ema150 = df.iloc[-1]['EMA150']
    OBV = df.iloc[-1]['OBV']
    OBV_MAX = df.iloc[-1]['OBV_MAX']
    turnover = df.iloc[-1]['volume']*close

    if (close>=ema5) and (ema5 >= ema10) and (ema10 >= ema20) and (ema10 >= ema60) and (OBV==OBV_MAX) and (turnover >= 100000):
        return True
    else:
        return False

    # return pd.DataFrame()

# def is_qfq_in_period(df,qfq,period):
#     ticker = df.loc[df.index[-1],'ticker']
#     ticker_date = df.index[-1]
#     for date in qfq[qfq.ticker==ticker].date:   # remove qfq
#         start = ticker_date.date()
#         end = date.date()
#         busdays = np.busday_count( start, end)
#         if (busdays > 0) & (busdays<=period+1):
#             return True
#         elif (busdays < 0) & (busdays>=-200):
#             return True
#     return False

def run(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        # if ticker_df.empty or (ticker_df.iloc[-1]['close'] < ticker_df.iloc[-1]['EMA20']):
        #     continue
        return_ticker_df = pd.DataFrame()
        # start_time = time.time()

        for date in ticker_df.index:
            # date2 = date - timedelta(days=1)
            # i = 0
            # while (date2 not in ticker_df.index) and (i<3):
            #     date2 = date2 - timedelta(days=1)
            #     i=i+1
            # if i == 3:
            #     continue
            date_ticker_df = ticker_df[ticker_df.index==date]
        #     # if(is_qfq_in_period(date_ticker_df,qfq,60)):
        #     #     continue
            if date_ticker_df.empty:
                continue
            result = screen(date_ticker_df)
            if result:
                date_ticker_df.loc[date,'Breakout'] += 1
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                date_ticker_df.loc[date,'Breakout_CUM'] += date_ticker_df.loc[date,'Breakout']
                date_ticker_df.loc[date,'Breakout'] = 0
        # print("%s seconds\n" %(time.time()-start_time))
        # result = screen(ticker_df)
        # if not result.empty:
        #     return_ticker_df = return_ticker_df.append(result)

        if not return_ticker_df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,return_ticker_df])
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

date_time = datetime.datetime.now() 
datetime_str = date_time.strftime("%m%d%Y-%H")

# end = datetime.date.today()
processed_data_path=f"C:/Python/ProcessedData/"
screened_data_path=f"C:/Python/ScreenedData/"
qfq_path = 'C:/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    screened_data_files = os.listdir(screened_data_path)
    screened_data_file = datetime_str + '_breakout.csv'
    if screened_data_file in screened_data_files:
        print("error: " + screened_data_file + " existed.")
        sys.exit(1)

    df = pd.read_feather(processed_data_path + datetime_str + '.feather')
    # df = df[df['date'] > '2017-01-01']
    # qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    # qfq = qfq[qfq['date'] > '2017-01-01']

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

    return_df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            return_df = pd.concat([return_df,result])
    
    return_df.reset_index(drop=False,inplace=True)
    return_df.to_csv(screened_data_path + datetime_str + '_breakout.csv')