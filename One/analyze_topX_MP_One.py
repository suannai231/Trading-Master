from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
from multiprocessing import Pool
import math

def drop_qfq(df,qfq,period):
    if df.empty:
        return
    column = 'change_' + str(period) +'days'
    df.dropna(subset=[column],inplace=True)
    sorted_df = df.sort_values(by=[column],ascending=False,ignore_index=True)
    i = sorted_df.index[0]
    if math.isnan(sorted_df[column][i]):
        return
    while (i <= sorted_df.index[-1]) & (not math.isnan(sorted_df[column][i])):
        ticker = sorted_df['ticker'][i]
        ticker_date = sorted_df['date'][i]

        for date in qfq[qfq.ticker==ticker].date:
            start = ticker_date.date()
            end = date.date()
            busdays = np.busday_count( start, end)
            if (busdays > 0) & (busdays<=period+1):
                sorted_df.drop(index=i,inplace=True)
                break
        
        if i in sorted_df.index:
            if sorted_df['cum_turnover'][i] < 1: #cum_turnover < 1 drop
                sorted_df.drop(index=i,inplace=True)

        if i in sorted_df.index:
            if sorted_df[column][i] < 1: #change X days period < 1 drop
                sorted_df.drop(index=i,inplace=True)
        
        i+=1
    if i == sorted_df.index[-1]+1:
        return
    sorted_df.reset_index(drop=True,inplace=True)
    sorted_df.to_csv(analyzed_topX_data_path + f'{period}' + 'days.csv')
    return 

end = datetime.date.today()
topX_data_path=f"//jack-nas/Work/Python/TopX/"
analyzed_topX_data_path=f"//jack-nas/Work/Python/AnalyzedTopX/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    analyzed_topX_data_files = os.listdir(analyzed_topX_data_path)
    periods = range(5,61,5)
    for period in periods:
        analyzed_topX_data_file = f'{period}'+'days.csv'
        if analyzed_topX_data_file in analyzed_topX_data_files:
            exit()
    
    isPathExists = os.path.exists(analyzed_topX_data_path)
    if not isPathExists:
        os.makedirs(analyzed_topX_data_path)
    
    df = pd.read_feather(topX_data_path + f'{end}' + '.feather')
    qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    
    pool = Pool(len(periods))
    for period in periods:
        pool.apply_async(drop_qfq,args=(df,qfq,period))
    pool.close()
    pool.join()