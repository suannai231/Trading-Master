import multiprocessing
import pandas as pd
import datetime
import os
import sys
from multiprocessing import Pool
import numpy as np
from datetime import timedelta
import time
import logging

def screen(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    # ema150 = df.iloc[-1]['EMA150']
    OBV = df.iloc[-1]['OBV']
    OBV_Max = df.iloc[-1]['OBV_Max']
    turnover = df.iloc[-1]['volume']*close

    ema5_max = df.iloc[-1]['EMA5_Max']
    ema10_max = df.iloc[-1]['EMA10_Max']
    ema20_max = df.iloc[-1]['EMA20_Max']
    ema60_max = df.iloc[-1]['EMA60_Max']

    # if (close>=ema5) and (ema5 >= ema10) and (ema10 >= ema20) and (ema20 >= ema60) and (OBV>=OBV_Max*0.90) and (turnover >= 100000) \
    #     and (close >= ema5_max) and (close >= ema10_max) and (close >= ema20_max) and (close >= ema60_max):
    if (close>=ema5) and (ema5 >= ema10) and (ema10 >= ema20) and (OBV>=OBV_Max*0.90) and (turnover >= 100000) \
        and (close >= ema5_max) and (close >= ema10_max) and (close >= ema20_max):
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

        Breakout = 0
        Breakout_Cum = 0
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
                Breakout += 1
                date_ticker_df.loc[date,'Breakout'] = Breakout
                date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                Breakout_Cum += Breakout
                Breakout = 0
                # date_ticker_df.loc[date,'Breakout'] = 0

            # date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
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

if __name__ == '__main__':
    processed_data_path="//jack-nas/Work/Python/ProcessedData/"
    screened_data_path="//jack-nas/Work/Python/ScreenedData/"

    logpath = '//jack-nas/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_screen.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    while True:
        start_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)

        processed_data_files = os.listdir(processed_data_path)
        if len(processed_data_files) == 0:
            logging.warning("processed data not ready, sleep 60 seconds...")
            time.sleep(60)

        screened_data_files = os.listdir(screened_data_path)
        processed_data_files_str = processed_data_files[-1] + '_breakout.csv'
        if processed_data_files_str in screened_data_files:
            logging.warning("error: " + processed_data_files[-1] + " existed, sleep 60 seconds...")
            time.sleep(60)
            continue
        # date_time = datetime.datetime.now() 
        # datetime_str = date_time.strftime("%m%d%Y-%H")
        # end = datetime.date.today()
        logging.info("processing "+processed_data_files[-1])

        try:
            df = pd.read_feather(processed_data_path + processed_data_files[-1])
        except Exception as e:
            logging.critical(e)
            continue

        # processed_data_files = os.listdir(processed_data_path)
        # screened_data_file = datetime_str + '_breakout.csv'
        # if screened_data_file in screened_data_files:
        #     print("error: " + screened_data_file + " existed.")
        #     sys.exit(1)

        # df = pd.read_feather(processed_data_path + datetime_str + '.feather')
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
        del(df)

        return_df = pd.DataFrame()
        for async_result in async_results:
            result = async_result.get()
            if not result.empty:
                return_df = pd.concat([return_df,result])
        
        if(not return_df.empty):
            return_df.reset_index(drop=False,inplace=True)
            return_df.to_csv(screened_data_path + processed_data_files[-1] + '_breakout.csv')
            end = datetime.date.today()
            return_df.loc[(return_df.date==str(end)) & (return_df.Breakout==1),'ticker'].to_csv(screened_data_path + processed_data_files[-1] + '_breakout.txt',header=False, index=False)
            stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
            logging.info("stop time:" +stop_time)
        else:
            logging.error("df empty")