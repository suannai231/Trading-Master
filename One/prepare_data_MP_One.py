import pandas as pd
import datetime
import os
import wr
import multiprocessing
from multiprocessing import Pool

backward = 200
CAP_Limit = 2000000000
Price_Limit = 50

def prepare_data(df):
    origin_lastindex = df.index[-1]
    cap = df["marketCap"][origin_lastindex]
    if cap > CAP_Limit:
        return pd.DataFrame()
    elif df['close'][origin_lastindex] > Price_Limit:
        return pd.DataFrame()
    elif len(df) <= backward+2:
        return pd.DataFrame()
    else:
        startindex = df.index[0]
        endindex = len(df)
        lastindex = df.index[-1]

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

        shares = df['shares'][lastindex]
        df['turn'] = df.volume/shares
        df['cum_turnover'] = df['turn'].cumsum()

        ema34 = df['close'].ewm(span = 34, adjust = False).mean()
        ema120 = df['close'].ewm(span = 120, adjust = False).mean()
        df['EMA34'] = ema34
        df['EMA120'] = ema120

        MACD_dif = ema34 - ema120
        MACD_dea = MACD_dif.ewm(span = 9, adjust = False).mean()
        df['MACD_dif'] = MACD_dif
        df['MACD_dea'] = MACD_dea
                
        OBV = []
        OBV.append(0)
        for i in range(startindex+1, endindex):
            if df.close[i] > df.close[i-1]: #If the closing price is above the prior close price 
                OBV.append(OBV[-1] + df.volume[i]) #then: Current OBV = Previous OBV + Current volume
            elif df.close[i] < df.close[i-1]:
                OBV.append( OBV[-1] - df.volume[i])
            else:
                OBV.append(OBV[-1])

        df['OBV'] = OBV
        df['OBV_EMA34'] = df['OBV'].ewm(com=34).mean()
        df['OBV_DIFF'] = df['OBV']-df['OBV_EMA34']
        max_obv_diff = 0

        OBV_DIFF_RATE = []

        for i in range(startindex,endindex):
            if abs(df['OBV_DIFF'][i]) > max_obv_diff:
                max_obv_diff = abs(df['OBV_DIFF'][i])
            if max_obv_diff == 0:
                OBV_DIFF_RATE.append(0)
            else:
                OBV_DIFF_RATE.append(abs(df['OBV_DIFF'][i])/max_obv_diff)

        df["OBV_DIFF_RATE"] = OBV_DIFF_RATE
        df = wr.Cal_Daily_WR(df)
        return df

end = datetime.date.today()
raw_data_path=f"//jack-nas/Work/Python/RawData/"
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"

if __name__ == '__main__':
    processed_files = os.listdir(processed_data_path)
    processed_file = str(end)+'.feather'
    if processed_file in processed_files:
        exit()

    isPathExists = os.path.exists(processed_data_path)
    if not isPathExists:
        os.makedirs(processed_data_path)
    
    df = pd.read_feather(raw_data_path + f'{end}' + '.feather')
    tickers = df.ticker.unique()

    cores = multiprocessing.cpu_count()
    pool = Pool(cores*3)
    async_results = []
    for ticker in tickers:
        ticker_df = df[df.ticker==ticker].reset_index(drop=True)
        async_result = pool.apply_async(prepare_data, args=(ticker_df,))
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
    df.to_feather(processed_data_path + f'{end}' + '.feather')