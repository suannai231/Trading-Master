import multiprocessing
import pandas as pd
import datetime
import os
import sys
from multiprocessing import Pool
import numpy as np
# from datetime import timedelta
import time
import logging
import math
from yahoo_fin import stock_info as si

# processed_data_path="//jack-nas.home/Work/Python/ProcessedData/"
screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"
# raw_data_path = '//jack-nas.home/Work/Python/RawData/'
processed_data_path="C:/Python/ProcessedData/"
# screened_data_path="C:/Python/ScreenedData/"
raw_data_path = 'C:/Python/RawData/'
base_days = 13


def screen(df,lines):
    close = df.iloc[-1]['close']
    ema20 = df.iloc[-1]['EMA20']
    ema60 = df.iloc[-1]['EMA60']
    turnover = df.iloc[-1]['volume']*close
    # EMA20_High = df.iloc[-1]['EMA20_High']
    Vol_High_Price = df.iloc[-1]['Vol_High_Price']
    change = df.iloc[-1]['change']
    Year_Low = df.iloc[-1]['Year_Low']
    AMP = df.iloc[-1]['AMP']

    if(len(df)<10):
        return False
    Last_10days_df = df.iloc[len(df)-10:]
    ema20_10days_max = max(Last_10days_df.EMA20)
    # df = cal_Vol_Low_High_Price(Last_30days_df)
    # Vol_High_Price = df.iloc[-1]['Vol_High_Price']
    # ticker = df.iloc[-1]['ticker']
    # if ticker=="PETZ":
    #     print("PETZ")

    if lines=="Strong":
        if(Vol_High_Price*1.1 >= close >= Vol_High_Price*0.90) & (ema20==ema20_10days_max):
            return True
        else:
            return False
    elif lines=="Close to EMA20":
        if(close<=ema20*1.2):
            return True
        else:
            return False
    elif lines=="Up_Trend":
        if(len(df)<60):
            return False
        last_60days_df = df.iloc[len(df)-60:]
        length = len(last_60days_df.loc[(last_60days_df.EMA20>=last_60days_df.EMA60) & (last_60days_df.EMA60>=last_60days_df.EMA120)])
        if(length==60):
            return True
        else:
            return False
    elif lines=="turnover":
        if(turnover >= 100000):
            return True
        else:
            return False
    elif lines=="change":
        if(change >= 0):
            return True
        else:
            return False
    elif lines=="Year_Low":
        if(close<=Year_Low*1.7):
            return True
        else:
            return False
    elif lines=="AMP":
        if(AMP<=0.1):
            return True
        else:
            return False
    elif lines=="OBV":
        if(len(df)<60):
            return False
        last_60days_df = df.iloc[len(df)-60:]
        obv_max = max(last_60days_df.OBV)
        obv_ema10 = df.iloc[-1]['obv_ema10']
        obv_ema20 = df.iloc[-1]['obv_ema20']
        obv_ema30 = df.iloc[-1]['obv_ema30']
        OBV = df.iloc[-1]['OBV']
        if((OBV>=obv_ema10>=obv_ema20>=obv_ema30) & (OBV>=obv_max*0.9)):
            return True
        else:
            return False

    # elif lines=="STD_Vol":
    #     if (min_STD_Vol*1.4 >= STD_Vol > min_STD_Vol):
    #         return True
    #     else:
    #         return False
    # elif lines=="STD_Close":
    #     if STD_Close == min_STD_Close:
    #         return True
    #     else:
    #         return False
    return False

def run(ticker_chunk_df):
    if ticker_chunk_df.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
        # FityTwo_Week_Low_ticker_df = FityTwo_Week_Low_chunk_df.loc[FityTwo_Week_Low_chunk_df.ticker==ticker]
        # if FityTwo_Week_Low_ticker_df.empty:
        #     continue
        # else:
        #     FityTwo_Week_Low = FityTwo_Week_Low_ticker_df.iloc[-1]['FityTwo_Week_Low']
        # return_ticker_df = pd.DataFrame()
        # Breakout = 0
        # Wait_Cum = 0
        # df = ticker_df.iloc[len(ticker_df)-base_days:]
        today_df = df.iloc[[-1]]
        # for date in df.index:
        #     date_ticker_df = df[df.index==date]
        #     if date_ticker_df.empty:
        #         continue

        #     AMP_result = screen(date_ticker_df,"AMP")
            
        #     if AMP_result:
        Turnover = screen(df,"turnover")
        # STD_Close = screen(df,"STD_Close")
        Strong = screen(df,"Strong")
        Close_to_EMA20 = screen(df,"Close to EMA20")
        change = screen(df,"change")
        Year_Low = screen(df,'Year_Low')
        AMP = screen(df,'AMP')
        Up_Trend = screen(df,"Up_Trend")
        OBV = screen(df,"OBV")
        # if (Turnover & Strong & Close_to_EMA20 & change & AMP & Up_Trend):
        if(OBV & Turnover & Strong & Close_to_EMA20 & change & AMP & Year_Low):
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
                # break

        
    return return_ticker_chunk_df

def save(return_df,async_results,processed_data_file):
    df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        if not result.empty:
            df = pd.concat([df,result])
    
    if(not df.empty):
        df.reset_index(drop=False,inplace=True)
        try:
            df.to_csv(screened_data_path + processed_data_file + '.csv')
            end = datetime.date.today()
            df = df.loc[df.date==str(end),'ticker']
            df.to_csv(screened_data_path + processed_data_file + '.txt',header=False, index=False)
            return_df = pd.concat([return_df,df])
        except Exception as e:
            logging.critical("return_df to_csv:"+str(e))
    else:
        logging.error("return_df empty")
    return return_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == '__main__':
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_screen.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    now = datetime.datetime.now()
    today830am = now.replace(hour=8,minute=30,second=0,microsecond=0)
    today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    # FityTwo_Week_Low_df = pd.DataFrame()
    # while(FityTwo_Week_Low_df.empty):
    #     now = datetime.datetime.now()
    #     # today3pm = now.replace(hour=15,minute=5,second=0,microsecond=0)
    #     # if(now>today3pm):
    #     #     logging.info("time passed 3:05pm.")
    #     #     break
    #     FityTwo_Week_Low_File_time = now.strftime("%m%d%Y")
    #     start_time = now.strftime("%m%d%Y-%H%M%S")
    #     logging.info("start time:" + start_time)
    #     # FityTwo_Week_Low_File = os.listdir(raw_data_path+ start_time + "_FityTwo_Week_Low_df.feather")
    #     # if len(FityTwo_Week_Low_File) == 0:
    #     #     logging.warning("processed data not ready, sleep 10 seconds...")
    #     #     time.sleep(10)
    #     #     continue
    #     try:
    #         # time.sleep(1)
    #         FityTwo_Week_Low_df = pd.read_feather(raw_data_path + FityTwo_Week_Low_File_time + "_FityTwo_Week_Low_df.feather")
    #     except Exception as e:
    #         logging.warning("processed data not ready, sleep 10 seconds...")
    #         time.sleep(10)
    #         continue
    
    # while((now.weekday() <= 4) & (today830am <= datetime.datetime.now() <= today3pm)):
    while(True):
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
        processed_data_files_str = processed_data_files[-1] + '_AMP.txt'
        if processed_data_files_str in screened_data_files:
            logging.warning("error: " + processed_data_files_str + " existed, sleep 10 seconds...")
            time.sleep(10)
            continue
        # date_time = datetime.datetime.now() 
        # datetime_str = date_time.strftime("%m%d%Y-%H")
        # end = datetime.date.today()
        logging.info("processing "+processed_data_files[-1])

        try:
            time.sleep(1)
            df = pd.read_feather(processed_data_path + processed_data_files[-1])
        except Exception as e:
            logging.critical(e)
            continue
        # df = df.loc[df.date>"2022-01-01"]
        # today = datetime.date.today()
        # day1 = today - timedelta(days=1)
        # day2 = today - timedelta(days=2)
        # day3 = today - timedelta(days=3)
        # df = df.loc[(df.date == str(today)) | (df.date == str(day1)) | (df.date == str(day2)) | (df.date == str(day3))]
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
        cores = multiprocessing.cpu_count()/2
        ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
        pool=Pool(cores)
        # async_results_60_120 = []
        # async_results_120_250 = []
        # async_results_250 = []
        async_results_AMP = []
        for ticker_chunk in ticker_chunk_list:
            ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
            # FityTwo_Week_Low_chunk_df = FityTwo_Week_Low_df[FityTwo_Week_Low_df['ticker'].isin(ticker_chunk)]
            async_result_AMP = pool.apply_async(run, args=(ticker_chunk_df,))
            async_results_AMP.append(async_result_AMP)
        pool.close()
        del(df)
        return_df = pd.DataFrame()

        return_df = save(return_df,async_results_AMP,processed_data_files[-1]+"_AMP")
        # if(not return_df.empty):
        #     try:
        #         return_df.to_csv(screened_data_path + processed_data_files[-1] + '_all.txt',header=False, index=False)
        #     except Exception as e:
        #         logging.critical("return_df to_csv:"+str(e))
        # else:
        #     logging.error("return_df empty")
        stop_time = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
        logging.info("stop time:" +stop_time)