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
import math

def screen_20_60(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    ema120 = df.iloc[-1]['EMA120']
    ema250 = df.iloc[-1]['EMA250']
    OBV = df.iloc[-1]['OBV']
    OBV_Max = df.iloc[-1]['OBV_Max']
    turnover = df.iloc[-1]['volume']*close

    ema5_max = df.iloc[-1]['EMA5_Max']
    ema10_max = df.iloc[-1]['EMA10_Max']
    ema20_max = df.iloc[-1]['EMA20_Max']
    ema60_max = df.iloc[-1]['EMA60_Max']
    ema120_max = df.iloc[-1]['EMA120_Max']
    ema250_max = df.iloc[-1]['EMA250_Max']
    close_max = df.iloc[-1]['Close_Max']

    ema5_min = df.iloc[-1]['EMA5_Min']
    ema10_min = df.iloc[-1]['EMA10_Min']
    ema20_min = df.iloc[-1]['EMA20_Min']
    ema60_min = df.iloc[-1]['EMA60_Min']
    ema250_min = df.iloc[-1]['EMA250_Min']
    close_min = df.iloc[-1]['Close_Min']

    if (ema5>=ema10) and (ema20<=ema5<=ema60) and (OBV>=OBV_Max*0.8) and (turnover >= 100000) \
        and ((close-ema10)/ema10 <= 0.3) and (ema5 >= ema5_max*0.9) and ((ema5_max-ema5_min)/ema5_min <= 2):
        return True
    else:
        return False

def screen_60_120(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    ema120 = df.iloc[-1]['EMA120']
    ema250 = df.iloc[-1]['EMA250']
    OBV = df.iloc[-1]['OBV']
    OBV_Max = df.iloc[-1]['OBV_Max']
    turnover = df.iloc[-1]['volume']*close

    ema5_max = df.iloc[-1]['EMA5_Max']
    ema10_max = df.iloc[-1]['EMA10_Max']
    ema20_max = df.iloc[-1]['EMA20_Max']
    ema60_max = df.iloc[-1]['EMA60_Max']
    ema120_max = df.iloc[-1]['EMA120_Max']
    ema250_max = df.iloc[-1]['EMA250_Max']
    close_max = df.iloc[-1]['Close_Max']

    ema5_min = df.iloc[-1]['EMA5_Min']
    ema10_min = df.iloc[-1]['EMA10_Min']
    ema20_min = df.iloc[-1]['EMA20_Min']
    ema60_min = df.iloc[-1]['EMA60_Min']
    ema250_min = df.iloc[-1]['EMA250_Min']
    close_min = df.iloc[-1]['Close_Min']

    if (ema5>=ema10) and (ema10>=ema20) and (ema60<=ema5<=ema120) and (OBV>=OBV_Max*0.8) and (turnover >= 100000) \
        and ((close-ema20)/ema20 <= 0.3) and (ema5 >= ema5_max*0.9) and ((ema5_max-ema5_min)/ema5_min <= 2):
        return True
    else:
        return False

def screen_120_250(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    ema120 = df.iloc[-1]['EMA120']
    ema250 = df.iloc[-1]['EMA250']
    OBV = df.iloc[-1]['OBV']
    OBV_Max = df.iloc[-1]['OBV_Max']
    turnover = df.iloc[-1]['volume']*close

    ema5_max = df.iloc[-1]['EMA5_Max']
    ema10_max = df.iloc[-1]['EMA10_Max']
    ema20_max = df.iloc[-1]['EMA20_Max']
    ema60_max = df.iloc[-1]['EMA60_Max']
    ema120_max = df.iloc[-1]['EMA120_Max']
    ema250_max = df.iloc[-1]['EMA250_Max']
    close_max = df.iloc[-1]['Close_Max']

    ema5_min = df.iloc[-1]['EMA5_Min']
    ema10_min = df.iloc[-1]['EMA10_Min']
    ema20_min = df.iloc[-1]['EMA20_Min']
    ema60_min = df.iloc[-1]['EMA60_Min']
    ema250_min = df.iloc[-1]['EMA250_Min']
    close_min = df.iloc[-1]['Close_Min']

    if (ema5>=ema10) and (ema10>=ema20) and (ema20>=ema60) and (ema120<=ema5<=ema250) and (OBV>=OBV_Max*0.8) and (turnover >= 100000) \
        and ((close-ema60)/ema60 <= 0.3) and (ema5 >= ema5_max*0.9) and ((ema5_max-ema5_min)/ema5_min <= 2):
        return True
    else:
        return False

def screen_250(df):

    close = df.iloc[-1]['close']
    ema5 = df.iloc[-1]['EMA5']
    ema10 = df.iloc[-1]['EMA10']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    ema120 = df.iloc[-1]['EMA120']
    ema250 = df.iloc[-1]['EMA250']
    OBV = df.iloc[-1]['OBV']
    OBV_Max = df.iloc[-1]['OBV_Max']
    turnover = df.iloc[-1]['volume']*close

    ema5_max = df.iloc[-1]['EMA5_Max']
    ema10_max = df.iloc[-1]['EMA10_Max']
    ema20_max = df.iloc[-1]['EMA20_Max']
    ema60_max = df.iloc[-1]['EMA60_Max']
    ema120_max = df.iloc[-1]['EMA120_Max']
    ema250_max = df.iloc[-1]['EMA250_Max']
    close_max = df.iloc[-1]['Close_Max']

    ema5_min = df.iloc[-1]['EMA5_Min']
    ema10_min = df.iloc[-1]['EMA10_Min']
    ema20_min = df.iloc[-1]['EMA20_Min']
    ema60_min = df.iloc[-1]['EMA60_Min']
    ema250_min = df.iloc[-1]['EMA250_Min']
    close_min = df.iloc[-1]['Close_Min']

    if (ema5>=ema10) and (ema10>=ema20) and (ema20>=ema60) and (ema60>=ema120) and (ema5>=ema250) and (OBV>=OBV_Max*0.8) and (turnover >= 100000) \
        and ((close-ema120)/ema120 <= 0.3) and (ema5 >= ema5_max*0.9) and ((ema5_max-ema5_min)/ema5_min <= 2):
        return True
    else:
        return False

def run_20_60(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        return_ticker_df = pd.DataFrame()
        Breakout = 0
        Breakout_Cum = 0
        for date in ticker_df.index:
            date_ticker_df = ticker_df[ticker_df.index==date]
            if date_ticker_df.empty:
                continue
            result = screen_20_60(date_ticker_df)
            if result:
                Breakout += 1
                date_ticker_df.loc[date,'Breakout'] = Breakout
                date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                Breakout_Cum += Breakout
                Breakout = 0
        if not return_ticker_df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,return_ticker_df])
    return return_ticker_chunk_df

def run_60_120(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        return_ticker_df = pd.DataFrame()
        Breakout = 0
        Breakout_Cum = 0
        for date in ticker_df.index:
            date_ticker_df = ticker_df[ticker_df.index==date]
            if date_ticker_df.empty:
                continue
            result = screen_60_120(date_ticker_df)
            if result:
                Breakout += 1
                date_ticker_df.loc[date,'Breakout'] = Breakout
                date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                Breakout_Cum += Breakout
                Breakout = 0
        if not return_ticker_df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,return_ticker_df])
    return return_ticker_chunk_df

def run_120_250(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        return_ticker_df = pd.DataFrame()
        Breakout = 0
        Breakout_Cum = 0
        for date in ticker_df.index:
            date_ticker_df = ticker_df[ticker_df.index==date]
            if date_ticker_df.empty:
                continue
            result = screen_120_250(date_ticker_df)
            if result:
                Breakout += 1
                date_ticker_df.loc[date,'Breakout'] = Breakout
                date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                Breakout_Cum += Breakout
                Breakout = 0
        if not return_ticker_df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,return_ticker_df])
    return return_ticker_chunk_df

def run_250(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        return_ticker_df = pd.DataFrame()
        Breakout = 0
        Breakout_Cum = 0
        for date in ticker_df.index:
            date_ticker_df = ticker_df[ticker_df.index==date]
            if date_ticker_df.empty:
                continue
            result = screen_250(date_ticker_df)
            if result:
                Breakout += 1
                date_ticker_df.loc[date,'Breakout'] = Breakout
                date_ticker_df.loc[date,'Breakout_Cum'] = Breakout_Cum
                return_ticker_df = pd.concat([return_ticker_df,date_ticker_df])
            else:
                Breakout_Cum += Breakout
                Breakout = 0
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
        now = datetime.datetime.now()
        # today3pm = now.replace(hour=15,minute=5,second=0,microsecond=0)
        # if(now>today3pm):
        #     logging.info("time passed 3:05pm.")
        #     break
        start_time = now.strftime("%m%d%Y-%H%M%S")
        logging.info("start time:" + start_time)

        processed_data_files = os.listdir(processed_data_path)
        if len(processed_data_files) == 0:
            logging.warning("processed data not ready, sleep 10 seconds...")
            time.sleep(10)
            continue

        screened_data_files = os.listdir(screened_data_path)
        processed_data_files_str = processed_data_files[-1] + '_250_breakout.csv'
        if processed_data_files_str in screened_data_files:
            logging.warning("error: " + processed_data_files_str + " existed, sleep 10 seconds...")
            time.sleep(10)
            continue
        # date_time = datetime.datetime.now() 
        # datetime_str = date_time.strftime("%m%d%Y-%H")
        # end = datetime.date.today()
        logging.info("processing "+processed_data_files[-1])

        try:
            time.sleep(10)
            df = pd.read_feather(processed_data_path + processed_data_files[-1])
        except Exception as e:
            logging.critical(e)
            continue

        today = datetime.date.today()
        day1 = today - timedelta(days=1)
        day2 = today - timedelta(days=2)
        df = df.loc[(df.date == str(today)) | (df.date == str(day1)) | (df.date == str(day2))]
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
        ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
        pool=Pool(cores)
        async_results_20_60 = []
        async_results_60_120 = []
        async_results_120_250 = []
        async_results_250 = []
        for ticker_chunk in ticker_chunk_list:
            ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
            async_result_20_60 = pool.apply_async(run_20_60, args=(ticker_chunk_df,))
            async_results_20_60.append(async_result_20_60)
            async_result_60_120 = pool.apply_async(run_60_120, args=(ticker_chunk_df,))
            async_results_60_120.append(async_result_60_120)
            async_result_120_250 = pool.apply_async(run_120_250, args=(ticker_chunk_df,))
            async_results_120_250.append(async_result_120_250)
            async_result_250 = pool.apply_async(run_250, args=(ticker_chunk_df,))
            async_results_250.append(async_result_250)
        pool.close()
        del(df)

        return_df_20_60 = pd.DataFrame()
        for async_result_20_60 in async_results_20_60:
            result_20_60 = async_result_20_60.get()
            if not result_20_60.empty:
                return_df_20_60 = pd.concat([return_df_20_60,result_20_60])
        
        if(not return_df_20_60.empty):
            return_df_20_60.reset_index(drop=False,inplace=True)
            try:
                return_df_20_60.to_csv(screened_data_path + processed_data_files[-1] + '_20_60_breakout.csv')
                end = datetime.date.today()
                return_df_20_60.loc[(return_df_20_60.date==str(end)) & (return_df_20_60.breakout==1),'ticker'].to_csv(screened_data_path + processed_data_files[-1] + '_20_60_breakout.txt',header=False, index=False)
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        else:
            logging.error("return_df_20_60 empty")

        return_df_60_120 = pd.DataFrame()
        for async_result_60_120 in async_results_60_120:
            result_60_120 = async_result_60_120.get()
            if not result_60_120.empty:
                return_df_60_120 = pd.concat([return_df_60_120,result_60_120])
        
        if(not return_df_60_120.empty):
            return_df_60_120.reset_index(drop=False,inplace=True)
            try:
                return_df_60_120.to_csv(screened_data_path + processed_data_files[-1] + '_60_120_breakout.csv')
                end = datetime.date.today()
                return_df_60_120.loc[(return_df_60_120.date==str(end)) & (return_df_60_120.breakout==1),'ticker'].to_csv(screened_data_path + processed_data_files[-1] + '_60_120_breakout.txt',header=False, index=False)
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        else:
            logging.error("return_df_60_120 empty")

        return_df_120_250 = pd.DataFrame()
        for async_result_120_250 in async_results_120_250:
            result_120_250 = async_result_120_250.get()
            if not result_120_250.empty:
                return_df_120_250 = pd.concat([return_df_120_250,result_120_250])
        
        if(not return_df_120_250.empty):
            return_df_120_250.reset_index(drop=False,inplace=True)
            try:
                return_df_120_250.to_csv(screened_data_path + processed_data_files[-1] + '_120_250_breakout.csv')
                end = datetime.date.today()
                return_df_120_250.loc[(return_df_120_250.date==str(end)) & (return_df_120_250.breakout==1),'ticker'].to_csv(screened_data_path + processed_data_files[-1] + '_120_250_breakout.txt',header=False, index=False)
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        else:
            logging.error("return_df_120_250 empty")

        return_df_250 = pd.DataFrame()
        for async_result_250 in async_results_250:
            result_250 = async_result_250.get()
            if not result_250.empty:
                return_df_250 = pd.concat([return_df_250,result_250])
        
        if(not return_df_250.empty):
            return_df_250.reset_index(drop=False,inplace=True)
            try:
                return_df_250.to_csv(screened_data_path + processed_data_files[-1] + '_250_breakout.csv')
                end = datetime.date.today()
                return_df_250.loc[(return_df_250.date==str(end)) & (return_df_250.breakout==1),'ticker'].to_csv(screened_data_path + processed_data_files[-1] + '_250_breakout.txt',header=False, index=False)
            except Exception as e:
                logging.critical("to_feather:"+str(e))
        else:
            logging.error("return_df_250 empty")
        
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("stop time:" +stop_time)