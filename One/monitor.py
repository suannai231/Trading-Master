import multiprocessing
import pandas as pd
import datetime
import os
from multiprocessing import Pool
import time
import logging
import math
import numpy as np
from playsound import playsound
import pyttsx3

def screen(df1,df2):
    close1 = df1.iloc[-1].close
    close2 = df2.iloc[-1].close
    rate = (close1-close2)/close2
    if rate >= 0.02:
        return True
    return False

def run(ticker_chunk_df1,ticker_chunk_df2):
    if ticker_chunk_df1.empty or ticker_chunk_df2.empty:
        return pd.DataFrame()
    tickers = ticker_chunk_df1.ticker.unique()
    if len(tickers) == 0:
        return pd.DataFrame()
    ticker_chunk_df1.set_index('date',inplace=True)
    return_ticker_chunk_df = pd.DataFrame()
    for ticker in tickers:
        df1 = ticker_chunk_df1[ticker_chunk_df1.ticker==ticker]
        df2 = ticker_chunk_df2[ticker_chunk_df2.ticker==ticker]
        # if (not df.empty) and df.index[-1]!=datetime.date.today():
        #     log("error",ticker+" date error.")
        #     continue
        try:
            Volatile = screen(df1,df2)
        except Exception as e:
            log('critical',str(e))
            return pd.DataFrame()
        if(Volatile):
            i=0
            while(i<10):
                play('Bell.wav')
                speak(ticker)
                i+=1
            
            today_df = df1.iloc[[-1]]
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,today_df])
            log("info",ticker)
        
    return return_ticker_chunk_df

def screen_data():
    log('info',"screen_data start.")
    old_screened_data_files_str = ''
    while True:
        screened_data_files = os.listdir(screened_data_path)
        if len(screened_data_files) == 0:
            # log('warning',"screened data not ready, sleep 1 second...")
            time.sleep(1)
            continue

        moving_data_files = os.listdir(moving_data_path)
        screened_data_files_str = screened_data_files[-1] + '.txt'
        if screened_data_files_str == old_screened_data_files_str:
            log("warning",screened_data_files_str+" checked,sleep 10 seconds.")
            # speak(screened_data_files_str+" checked,sleep 10 seconds.")
            time.sleep(10)
            continue
        
        if screened_data_files_str in moving_data_files:
            # log('warning',"warning: " + processed_data_files_str + " existed, sleep 10 second...")
            time.sleep(10)
            continue

        log('info',"processing "+screened_data_files[-1])
        try:
            time.sleep(1)
            df1 = pd.read_feather(screened_data_path + screened_data_files[-1])
            df2 = pd.read_feather(screened_data_path + screened_data_files[-2])
            log('info',screened_data_path + screened_data_files[-1] + " loaded.")
            log('info',screened_data_path + screened_data_files[-2] + " loaded.")
        except Exception as e:
            log('critical',str(e)+" sleep 10 seconds.")
            time.sleep(10)
            continue
        # df=df[df.date!=str(datetime.date.today())]
        tickers = df1.ticker.unique()
        cores = int(multiprocessing.cpu_count())
        ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
        pool=Pool(cores)

        async_results = []
        for ticker_chunk in ticker_chunk_list:
            ticker_chunk_df1 = df1[df1['ticker'].isin(ticker_chunk)]
            ticker_chunk_df2 = df2[df2['ticker'].isin(ticker_chunk)]
            # sharesOutstanding_chunk_df = sharesOutstanding_df[sharesOutstanding_df['ticker'].isin(ticker_chunk)]
            async_result = pool.apply_async(run, args=(ticker_chunk_df1,ticker_chunk_df2))
            async_results.append(async_result)
        pool.close()
        log('info',"process pool start.")

        df = pd.DataFrame()
        for async_result in async_results:
            result = async_result.get()
            if not result.empty:
                df = pd.concat([df,result])
        
        if(not df.empty):
            df.reset_index(drop=False,inplace=True)
            try:
                df.to_csv(moving_data_path + screened_data_files[-1] + '.csv')
                df.ticker.to_csv(moving_data_path + screened_data_files[-1] + '.txt',header=False, index=False)
                log('info',moving_data_path + screened_data_files[-1] +" is saved.")
            except Exception as e:
                log('critical',"df to_csv:"+str(e))
        else: 
            log('info',"df empty")
            # time.sleep(10)
        old_screened_data_files_str = screened_data_files_str

    log('info',"screen_data stop.")

def chunks(lst, n): 
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def play(sound):
    directory_path = os.getcwd()
    file_path = directory_path+'\\Sounds\\' + sound
    try:
        playsound(file_path)
    except Exception as e:
        log("critical",str(e))

def log(type,string):
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_moving.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

    now = datetime.datetime.now()
    log_time = now.strftime("%m%d%Y-%H%M%S")
    if type=='info':
        logging.info(log_time+":"+string)
    elif type=='warning':
        logging.warning(log_time+":"+string)
    elif type=='error':
        directory_path = os.getcwd()
        file_path = directory_path+'\Sounds\PriceNotice.wav'
        try:
            playsound(file_path)
        except Exception as e:
            logging.info(log_time+":"+str(e))
        logging.error(log_time+":"+string)
    elif type=='critical':
        directory_path = os.getcwd()
        file_path = directory_path+'\Sounds\PriceNotice.wav'
        try:
            playsound(file_path)
        except Exception as e:
            logging.info(log_time+":"+str(e))
        logging.critical(log_time+":"+string)

def speak(ticker):
    # initialize the text-to-speech engine
    engine = pyttsx3.init()

    # set the rate and volume of the voice
    engine.setProperty('rate', 150)
    engine.setProperty('volume', 1)

    # ask the user for input
    # word = input(ticker)

    # speak the word
    engine.say(ticker)
    engine.runAndWait()

if __name__ == '__main__':
    moving_data_path = "//jack-nas.home/Work/Python/moving/"
    screened_data_path="//jack-nas.home/Work/Python/ScreenedData/"

    log('info',"screen_data process start.")
    speak("screen_data process start.")

    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)

    screen_data()

    # now = datetime.datetime.now()
    # today8am = now.replace(hour=8,minute=0,second=0,microsecond=0)
    # today3pm = now.replace(hour=15,minute=0,second=0,microsecond=0)

    # while((now.weekday() <= 4) & (today8am <= datetime.datetime.now() <= today3pm)):
    #     screen_data()

    # now = datetime.datetime.now()
    # stop_time = now.strftime("%m%d%Y-%H%M%S")
    # log('info',"screen_data process exit.")