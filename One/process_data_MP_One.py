import pandas as pd
import datetime
import os
import wr_helper
import multiprocessing
from multiprocessing import Pool
import time

backward = 150
CAP_Limit = 2000000000
Price_Limit = 50

def cal_cum_turnover(df):
    if len(df) <= backward+2:
        return pd.DataFrame()
    current = len(df) - 1
    # start_time = time.time()
    while current >= 0:
        
        # upper_cum_turn = 0
        # lower_cum_turn = 0

        # for i in range(current-backward,current):
        #     if df.loc[i,'close'] <= df.loc[current-1,'close']:
        #         lower_cum_turn += df.loc[i,'turn']
        #     else:
        #         upper_cum_turn += df.loc[i,'turn']

        if(current-backward>=0):
            start = current-backward
        else:
            start = 0
        
        lower_cum_turn = df.loc[start:current].loc[df.close <= df.loc[current,'close'],'turn'].sum()
        upper_cum_turn = df.loc[start:current].loc[df.close > df.loc[current,'close'],'turn'].sum()

        df.loc[current,'upper_cum_turn'] = upper_cum_turn
        df.loc[current,'lower_cum_turn'] = lower_cum_turn
        current-=1
    # print("--- %s seconds ---" % (time.time() - start_time))
    return df

# def cal_secret_num(df):
#     if len(df) <= backward+2:
#         return pd.DataFrame()
#     obv_above_zero_days = 0
#     wr120_larger_than_50_days = 0
#     wr120_larger_than_80_days = 0
#     for i in range(backward):
#         if df['OBV_DIFF'][i] > 0:
#             obv_above_zero_days += 1
#         if df.WR120[i] > 50:
#             wr120_larger_than_50_days += 1
#         if df.WR120[i] > 80:
#             wr120_larger_than_80_days += 1
#         df.loc[i,'obv_above_zero_days'] = obv_above_zero_days
#         df.loc[i,'wr120_larger_than_50_days'] = wr120_larger_than_50_days
#         df.loc[i,'wr120_larger_than_80_days'] = wr120_larger_than_80_days
#     i = backward
#     while i<len(df):
#         removed_index = i-backward
#         if df.loc[removed_index,'OBV_DIFF'] > 0:
#             obv_above_zero_days -= 1
#         if df.loc[removed_index,'WR120'] > 50:
#             wr120_larger_than_50_days -= 1
#         if df.loc[removed_index,'WR120'] > 80:
#             wr120_larger_than_80_days -= 1
#         if df.loc[i,'OBV_DIFF'] > 0:
#             obv_above_zero_days += 1
#         if df.loc[i,'WR120'] > 50:
#             wr120_larger_than_50_days += 1
#         if df.loc[i,'WR120'] > 80:
#             wr120_larger_than_80_days += 1
#         df.loc[i,'obv_above_zero_days'] = obv_above_zero_days
#         df.loc[i,'wr120_larger_than_50_days'] = wr120_larger_than_50_days
#         df.loc[i,'wr120_larger_than_80_days'] = wr120_larger_than_80_days
#         i+=1
#     return df

def cal_basics(df):
    startindex = 0
    endindex = len(df)
    lastindex = len(df)-1

    df['change'] = (df.close - df.close.shift(1))/df.close.shift(1)
    df['change_5days'] = (df.close.shift(-5)- df.close)/df.close
    df['change_10days'] = (df.close.shift(-10)- df.close)/df.close
    df['change_15days'] = (df.close.shift(-15)- df.close)/df.close
    df['change_20days'] = (df.close.shift(-20)- df.close)/df.close
    df['change_25days'] = (df.close.shift(-25)- df.close)/df.close
    df['change_30days'] = (df.close.shift(-30)- df.close)/df.close
    df['change_35days'] = (df.close.shift(-35)- df.close)/df.close
    df['change_40days'] = (df.close.shift(-40)- df.close)/df.close
    df['change_45days'] = (df.close.shift(-45)- df.close)/df.close
    df['change_50days'] = (df.close.shift(-50)- df.close)/df.close
    df['change_55days'] = (df.close.shift(-55)- df.close)/df.close
    df['change_60days'] = (df.close.shift(-60)- df.close)/df.close

    shares = df.loc[lastindex,'shares']
    df['turn'] = df.volume/shares
    # df['cum_turnover'] = df['turn'].cumsum()

    # ema5 = df['close'].ewm(span = 5, adjust = False).mean()
    # ema10 = df['close'].ewm(span = 10, adjust = False).mean()
    # ema20 = df['close'].ewm(span = 20, adjust = False).mean()
    # ema60 = df['close'].ewm(span = 60, adjust = False).mean()
    # ema12 = df['close'].ewm(span = 12, adjust = False).mean()
    # ema26 = df['close'].ewm(span = 26, adjust = False).mean()
    # ema34 = df['close'].ewm(span = 34, adjust = False).mean()
    # ema120 = df['close'].ewm(span = 120, adjust = False).mean()
    # df['EMA5'] = ema5
    # df['EMA10'] = ema10
    # df['EMA20'] = ema20
    # df['EMA60'] = ema60
    # df['EMA12'] = ema12
    # df['EMA26'] = ema26
    # df['EMA34'] = ema34
    # df['EMA120'] = ema120

    # MACD_dif = ema12 - ema26
    # MACD_dea = MACD_dif.ewm(span = 9, adjust = False).mean()
    # df['MACD_dif'] = MACD_dif
    # df['MACD_dea'] = MACD_dea
            
    # OBV = []
    # OBV.append(0)
    # for i in range(startindex+1, endindex):
    #     if df.loc[i,'close'] > df.loc[i-1,'close']: #If the closing price is above the prior close price 
    #         OBV.append(OBV[-1] + df.loc[i,'volume']) #then: Current OBV = Previous OBV + Current volume
    #     elif df.close[i] < df.close[i-1]:
    #         OBV.append(OBV[-1] - df.loc[i,'volume'])
    #     else:
    #         OBV.append(OBV[-1])

    # df['OBV'] = OBV
    # df['OBV_EMA34'] = df['OBV'].ewm(com=34).mean()
    # df['OBV_DIFF'] = df['OBV'] - df['OBV_EMA34']
    # max_obv_diff = 0

    # OBV_DIFF_RATE = []

    # for i in range(startindex,endindex):
    #     if abs(df.loc[i,'OBV_DIFF']) > max_obv_diff:
    #         max_obv_diff = abs(df.loc[i,'OBV_DIFF'])
    #     if max_obv_diff == 0:
    #         OBV_DIFF_RATE.append(0)
    #     else:
    #         OBV_DIFF_RATE.append(abs(df.loc[i,'OBV_DIFF'])/max_obv_diff)

    # df["OBV_DIFF_RATE"] = OBV_DIFF_RATE
    return df

def run(ticker_chunk_df):
    return_ticker_chunk_df = pd.DataFrame()
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        df = ticker_chunk_df[ticker_chunk_df.ticker==ticker].reset_index(drop=True)
        lastindex = df.index[-1]
        cap = df["marketCap"][lastindex]
        if cap > CAP_Limit:
            continue
        elif df['close'][lastindex] > Price_Limit:
            continue
        elif len(df) <= backward+2:
            continue
        # start_time = time.time()
        df = cal_basics(df)
        df = wr_helper.Cal_Hist_WR(df,backward)
        # df = wr_helper.Cal_Hist_WR(df,21)
        # df = wr_helper.Cal_Hist_WR(df,42)
        # df = cal_secret_num(df)
        df = cal_cum_turnover(df)
        # print("%s seconds\n" %(time.time()-start_time))
        if not df.empty:
            return_ticker_chunk_df = return_ticker_chunk_df.append(df,ignore_index=True)
    return return_ticker_chunk_df

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

end = datetime.date.today()
raw_data_path=f"//jack-nas/Work/Python/RawData/"
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"

if __name__ == '__main__':
    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)

    processed_files = os.listdir(processed_data_path)
    processed_file = str(end)+'.feather'
    if processed_file in processed_files:
        exit()
    
    df = pd.read_feather(raw_data_path + f'{end}' + '.feather')
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
    df.reset_index(drop=True,inplace=True)
    df.to_feather(processed_data_path + f'{end}' + '.feather')

    os.popen(f'python C:/Code/One/analyze_data_MP_One.py')