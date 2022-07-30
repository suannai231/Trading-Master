import pandas as pd
import datetime
import os
import sys
import multiprocessing
from multiprocessing import Pool

# backward = 180
CAP_Limit = 10000000000
Price_Limit = 9.5

def cal_Max(df):
    # startindex = 0
    # endindex = len(df)
    EMA5 = []
    EMA5_Max = []
    EMA10 = []
    EMA10_Max = []
    EMA20 = []
    EMA20_Max = []
    EMA60 = []
    EMA60_Max = []
    OBV_Max = []
    # for i in range(startindex, endindex):
    #     EMA5.append(df.EMA5[i])
    #     EMA10.append(df.EMA10[i])
    #     EMA20.append(df.EMA20[i])
    #     EMA60.append(df.EMA60[i])

    # EMA5_Max.append(max(EMA5))
    # EMA10_Max.append(max(EMA10))
    # EMA20_Max.append(max(EMA20))
    # EMA60_Max.append(max(EMA60))

    # EMA5_Max.append(max(df.loc[0:60,'EMA5']))
    # EMA10_Max.append(max(df.loc[0:60,'EMA10']))
    # EMA20_Max.append(max(df.loc[0:60,'EMA20']))
    # EMA60_Max.append(max(df.loc[0:60,'EMA25']))

    for startindex in range(0,len(df)-59):
        endindex = startindex + 59
        EMA5_Max.append(max(df.loc[startindex:endindex,'EMA5']))
        EMA10_Max.append(max(df.loc[startindex:endindex,'EMA10']))
        EMA20_Max.append(max(df.loc[startindex:endindex,'EMA20']))
        EMA60_Max.append(max(df.loc[startindex:endindex,'EMA60']))
        OBV_Max.append(max(df.loc[startindex:endindex,'OBV']))

    df.loc[59:len(df)-1,'EMA5_Max'] = EMA5_Max
    df.loc[59:len(df)-1,'EMA10_Max'] = EMA10_Max
    df.loc[59:len(df)-1,'EMA20_Max'] = EMA20_Max
    df.loc[59:len(df)-1,'EMA60_Max'] = EMA60_Max
    df.loc[59:len(df)-1,'OBV_Max'] = OBV_Max
    return df.loc[59:]

def cal_OBV(df):
    startindex = 0
    endindex = len(df)

    OBV = []
    OBV.append(0)
    # OBV_MAX = []
    # OBV_MAX.append(0)

    for i in range(startindex+1, endindex):
        high = df.high[i-1]
        low = df.low[i-1]
        mid = (high+low)/2
        if df.close[i] > mid:
            OBV.append(OBV[-1] + df.volume[i])
        elif df.close[i] < mid:
            OBV.append( OBV[-1] - df.volume[i])
        else:
            OBV.append(OBV[-1])
        # OBV_MAX.append(max(OBV))

    df['OBV'] = OBV
    # df['OBV_MAX'] = OBV_MAX

    return df

def cal_basics(df):

    lastindex = len(df)-1

    df['Breakout'] = 0
    df['Breakout_Cum'] = 0
    df['Wait'] = 0
    df['Wait_Cum'] = 0

    df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
    df['change_1days'] = (df.close.shift(-1)- df.close)/df.close
    df['change_2days'] = (df.close.shift(-2)- df.close)/df.close
    df['change_3days'] = (df.close.shift(-3)- df.close)/df.close
    df['change_4days'] = (df.close.shift(-4)- df.close)/df.close
    df['change_5days'] = (df.close.shift(-5)- df.close)/df.close
    df['change_6days'] = (df.close.shift(-6)- df.close)/df.close
    df['change_7days'] = (df.close.shift(-7)- df.close)/df.close
    df['change_8days'] = (df.close.shift(-8)- df.close)/df.close
    df['change_9days'] = (df.close.shift(-9)- df.close)/df.close
    df['change_10days'] = (df.close.shift(-10)- df.close)/df.close
    df['change_11days'] = (df.close.shift(-11)- df.close)/df.close
    df['change_12days'] = (df.close.shift(-12)- df.close)/df.close
    df['change_13days'] = (df.close.shift(-13)- df.close)/df.close
    df['change_14days'] = (df.close.shift(-14)- df.close)/df.close
    df['change_15days'] = (df.close.shift(-15)- df.close)/df.close
    df['change_16days'] = (df.close.shift(-16)- df.close)/df.close
    df['change_17days'] = (df.close.shift(-17)- df.close)/df.close
    df['change_18days'] = (df.close.shift(-18)- df.close)/df.close
    df['change_19days'] = (df.close.shift(-19)- df.close)/df.close
    df['change_20days'] = (df.close.shift(-20)- df.close)/df.close

    # shares = df.loc[lastindex,'shares']
    # df['turn'] = df.volume/shares

    ema5 = df['close'].ewm(span = 5, adjust = False).mean()
    ema10 = df['close'].ewm(span = 10, adjust = False).mean()
    ema20 = df['close'].ewm(span = 20, adjust = False).mean()
    ema60 = df['close'].ewm(span = 60, adjust = False).mean()
    ema150 = df['close'].ewm(span = 150, adjust = False).mean()
    df['EMA5'] = ema5
    df['EMA10'] = ema10
    df['EMA20'] = ema20
    df['EMA60'] = ema60
    df['EMA150'] = ema150

    return df

def run(ticker_chunk_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        lastindex = df.index[-1]
        # cap = df["marketCap"][lastindex]
        # if cap > CAP_Limit:
        #     continue
        if df['close'][lastindex] > Price_Limit:
            continue
        elif(len(df)<=60):
            print(ticker+" length is less than 60 business days.")
            continue

        df = cal_basics(df)
        # df = df.iloc[len(df)-60:].reset_index(drop=True)
        df = cal_OBV(df)
        df = cal_Max(df)

        if not df.empty:
            return_ticker_chunk_df = pd.concat([return_ticker_chunk_df,df],ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

date_time = datetime.datetime.now() 
datetime_str = date_time.strftime("%m%d%Y-%H")

# end = datetime.date.today()
raw_data_path=f"C:/Python/RawData/"
processed_data_path=f"C:/Python/ProcessedData/"

if __name__ == '__main__':
    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)

    processed_files = os.listdir(processed_data_path)
    processed_file = datetime_str + '.feather'
    if processed_file in processed_files:
        print("error: " + processed_file + " existed.")
        sys.exit(1)
    
    df = pd.read_feather(raw_data_path + datetime_str + '.feather')
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
            df = pd.concat([df,async_result.get()])
    df.reset_index(drop=True,inplace=True)
    if(not df.empty):
        df.to_feather(processed_data_path + datetime_str + '.feather')
        df.to_csv(processed_data_path + datetime_str + '.csv')
        # os.popen(f'python C:/Code/One/screen_data_One_Wait.py')
        os.popen(f'python C:/Code/One/screen_data_One_Breakout.py')
    else:
        print("df empty")