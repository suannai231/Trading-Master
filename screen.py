#from pandas_datareader import data
import multiprocessing
from yahoo_fin import stock_info as si
#import yfinance as yf
#import matplotlib.pyplot as plt
import pandas as pd
#import seaborn as sns
import numpy as np
import datetime
#import time
import os
# import threading
import chip
import wr
import shutil
# from multiprocessing import Process
from multiprocessing import Pool
# from multiprocessing import Value

run_days = 20
backward = 200

EMA_Indicator = True
MACD_Indicator = True
OBV_Indicator = True
Cum_Turnover_Indicator = True
Cum_Chip_Indicator = True
Chip_Concentration_Indicator = True
WR_Indicator = True

obv_convergence = 1
obv_above_zero_days_bar = 0.8
cum_turnover_rate = 1.8
cum_chip_bar = 0.8
chip_concentration_bar = 0.4
wr_bar = 40
wr120_greater_than_50_days_bar = 0.7
# wr120_greater_than_80_days_bar = 0.6

def screen(df):
    if len(df) <= backward+2:
        #print(f'Ticker: {ticker} length is less than {backward}\n')
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

    ss = chip.Cal_Chip_Distribution(df)
    if not ss.empty:
        cum_chip = ss['Cum_Chip'][ss.index[-1]]
        if Cum_Chip_Indicator & (cum_chip < cum_chip_bar):
            return pd.DataFrame()
        chip_con = chip.Cal_Chip_Concentration(ss)
        if Chip_Concentration_Indicator & (chip_con > chip_concentration_bar):
            return pd.DataFrame()

    if WR_Indicator & ((df.WR34[lastindex] > wr_bar) | (df.WR120[lastindex] > wr_bar)):
        return pd.DataFrame()

    wr120_less_than_50_days = 0
    for i in range(startindex,endindex):
        if df.WR120[i] > 50:
            wr120_less_than_50_days += 1
    if WR_Indicator & (wr120_less_than_50_days/backward < wr120_greater_than_50_days_bar):
        return pd.DataFrame()

    # wr120_less_than_80_days = wr.Cal_WR120_Greater_Than_X_Days(df,80)
    # if WR_Indicator & (wr120_less_than_80_days/120 < wr120_greater_than_80_days_bar):
    #     return pd.DataFrame()
    #Create buy and sell columns
    #x = buy_sell(df)
    #df['Buy_Signal_Price'] = x[0]
    #df['Sell_Signal_Price'] = x[1]

    return df

# class myThread (threading.Thread):   #继承父类threading.Thread
#     def __init__(self, ticker):
#         threading.Thread.__init__(self)
#         self.ticker = ticker
#     def run(self):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
#         #print ("Starting Thread " + self.letter + "\n")   
#         df = pd.read_csv(path+"/"+self.ticker+'.csv')
#         df.rename(columns={"Unnamed: 0":"date"}, inplace=True)

#         for i in range(len(df)):
#             if (len(df)-i) <= backward+2:
#                 return
#             df = df[0:len(df)-i].reset_index(drop=True)
#             save = screen(df)
#             history_day = df.date[df.index[-1]]
#             history_data_path = data_path+f'/{history_day}'
#             if not save.empty:
#                 save.to_csv(history_data_path+f'/{self.ticker}.csv')
#         return

#print("Starting screen Main Thread")

# def info(title):
#     print(title)
#     print('module name:', __name__)
#     print('parent process:', os.getppid())
#     print('process id:', os.getpid())

def run(file):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
    #print ("Starting Thread " + self.letter + "\n")   
    # print('process ' + str(os.getpid()) + ' started./n')
    # info('function run')
    df = pd.read_csv(processed_data_path+'/'+file)
    save = screen(df)
    if not save.empty:
        save.to_csv(screened_data_path+'/'+file)

    # df_length = len(df)
    # for i in range(df_length):
    #     if (len(df)-i) <= backward+2:
    #         break
    #     history_df = df[0:len(df)-i].reset_index(drop=True)
    #     save = screen(history_df)
    #     history_day = history_df.date[history_df.index[-1]]
    #     history_screened_data_path = screened_data_path+f'/{history_day}'
    #     if not save.empty:
    #         save.to_csv(history_screened_data_path+'/'+file)
    # print('process ' + str(os.getpid()) + ' terminated./n')
    # processed_numbers +=1
    return file

def run_all_by_date(date):
    if str(date) == '2021-10-12':
        print('2021-10-12')
    i = (end - date).days
    processed_data_files = os.listdir(processed_data_path)
    for file in processed_data_files:
        if file == 'HX.csv':
            print('HX.csv')
        df = pd.read_csv(processed_data_path + f'/{file}')
        if (len(df)-i) <= backward+2:
            continue
        df_slice = df[0:len(df)-i].reset_index(drop=True)
        history_day = df_slice.date[df_slice.index[-1]]
        history_screened_data_path = screened_data_path + f'{history_day}'
        isPathExists = os.path.exists(history_screened_data_path)
        if not isPathExists:
            os.makedirs(history_screened_data_path,exist_ok=True)
        isTickerExists = os.path.exists(history_screened_data_path + f'/{file}')
        if not isTickerExists:
            save = screen(df_slice)
            if not save.empty:
                # history_screened_data_path = screened_data_path+f'{date}'
                # history_day = save.date[save.index[-1]]
                # history_screened_data_path = screened_data_path + f'{history_day}'
                save.to_csv(history_screened_data_path + f'/{file}')
    return

end = datetime.date.today()
processed_data_path=f"//jack-nas/home/Drive/Python/ProcessedData/{end}"
screened_data_path=f"//jack-nas/home/Drive/Python/ScreenedData/"

if __name__ == '__main__':
    # porcessed_numbers = Value('d', 0)
    isPathExists = os.path.exists(screened_data_path)
    if not isPathExists:
        os.makedirs(screened_data_path)
        #print(data_path+"created successfully\n")
    # else:
        # files = os.listdir(screened_data_path)
        # for f in files:
        #     os.remove(os.path.join(screened_data_path,f))

        # files = os.listdir(screened_data_path)
        # for file in files:
        #     if os.path.isdir(screened_data_path+file):
        #         shutil.rmtree(screened_data_path+file)

    date_list = [end - datetime.timedelta(days=x) for x in range(run_days)]

    cores = multiprocessing.cpu_count()
    with Pool(cores) as p:
        # p.map(run_all_by_ticker, raw_data_files)
        p.map(run_all_by_date, date_list)
        p.close()
        p.join()


        # files = os.listdir(screened_data_path)
        # for file in files:
        #     if os.path.isdir(screened_data_path+'/'+file):
        #         shutil.rmtree(screened_data_path+'/'+file)
        #print(data_path+" dir existed\n")
        # datafiles = os.listdir(data_path)
        # for f in datafiles:
        #     os.remove(os.path.join(data_path,f))
        #print(data_path+" old files deleted\n")

    # for i in range(365*2-200):
    #     history_day = str((datetime.datetime.now() - datetime.timedelta(days=i)).date())
    #     history_screened_data_path = screened_data_path+f'/{history_day}'
    #     os.makedirs(history_screened_data_path,exist_ok=True)

    
    # print(len(files))
    # df = pd.read_csv(path+"/"+'HX.csv')
    # df = df[0:len(df)-18].reset_index(drop=True)
    # save = screen(df)

    # cores = multiprocessing.cpu_count()
    # with Pool(cores) as p:
    #     p.map(run, files)
    #     p.close()
    #     p.join()
    
    # print('all tickers data have been screened.\n')

# threads = []
# for file in files:
#     if not os.path.isdir(file):
#         ticker = os.path.splitext(file)[0]
#         thread = myThread(ticker)
#         threads.append(thread)
# for t in threads:
#     t.start()
# for t in threads:
#     t.join()

# threads = []
# for i in range(0,26):
#     thread = myThread(chr(65+i))
#     threads.append(thread)
#     thread.start()
# for t in threads:
#     t.join()

# os.popen(f'python C:/Users/jayin/OneDrive/Code/diagram.py')

#print("Exiting screen Main Thread")

'''
def buy_sell(signal):
    lastindex = signal.index[-1]
    cap = signal["marketCap"][lastindex]
    sigPriceBuy = []
    sigPriceSell = []
    flag = -1 #A flag for the trend upward/downward
    #Loop through the length of the data set
    for i in range(0,len(signal)):
        if(i <= backward):
            sigPriceBuy.append(np.nan)
            sigPriceSell.append(np.nan)
            continue
        #if OBV > OBV_EMA  and flag != 1 then buy else sell
        OBV_diff = signal['OBV_DIFF'][i]
        OBV_EMA34_DIFF = signal['OBV_EMA34'][i]-signal['OBV_EMA34'][i-1]

        MACD_Above_0 = (signal['MACD_dif'][i] >= 0) | (signal['MACD_dea'][i] >= 0)
        MACD_Below_0 = (signal['MACD_dif'][i] < 0) & (signal['MACD_dea'][i] < 0)

        normal_turnover = signal.volume[i]*signal['close'][i]/cap < 0.3
        high_turnover = signal.volume[i]*signal['close'][i]/cap > 2

        j = 0
        lower_vol = 0
        higher_vol = 0
        ready = False;
        while j<backward:
            if signal['close'][i-backward+j] <= signal['close'][i]:
                lower_vol += signal['volume'][i-backward+j]
            else:
                higher_vol += signal['volume'][i-backward+j]
            j+=1
        if lower_vol > higher_vol:
            ready = True
        else:
            ready = False

        print("OBV_EMA34_DIFF="+str(OBV_EMA34_DIFF)+"\n")
        if MACD_Above_0 & (OBV_diff >= 0) & (OBV_EMA34_DIFF<160000) & ready & normal_turnover & (flag != 1):
            sigPriceBuy.append(signal['close'][i])
            sigPriceSell.append(np.nan)
            flag = 1
        elif ((OBV_diff < 0) | MACD_Below_0 | high_turnover) & (flag != 0):
            sigPriceSell.append(signal['close'][i])
            sigPriceBuy.append(np.nan)
            flag = 0
        else: 
            sigPriceBuy.append(np.nan)
            sigPriceSell.append(np.nan)

    return (sigPriceBuy, sigPriceSell)
'''