from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
from multiprocessing import Pool
import math

def drop_qfq(df,qfq,period):
    if df.empty:
        return pd.DataFrame()
    column = 'change_' + str(period) +'days'
    df.dropna(subset=[column],inplace=True)
    if df.empty:
        return pd.DataFrame()
    df.reset_index(drop=True,inplace=True)
    sorted_df = df.sort_values(by=[column],ascending=False,ignore_index=True)

    for i in sorted_df.index:
        ticker = sorted_df['ticker'][i]
        ticker_date = sorted_df['date'][i]

        if (sorted_df['cum_turnover'][i] < 1) | (sorted_df[column][i] < 1): #cum_turnover < 1 drop
            sorted_df.drop(index=i,inplace=True)
            continue

        for date in qfq[qfq.ticker==ticker].date:
            start = ticker_date.date()
            end = date.date()
            busdays = np.busday_count( start, end)
            if (busdays > 0) & (busdays<=period+1):
                sorted_df.drop(index=i,inplace=True)
                break
    
    if not sorted_df.empty:
        sorted_df = sorted_df.reset_index(drop=True)[0:50]
        obv_above_zero_days_avg = sorted_df['obv_above_zero_days'].mean()
        cum_turnover_avg = sorted_df['cum_turnover'].mean()
        wr34_avg = sorted_df['wr34'].mean()
        wr120_avg = sorted_df['wr120'].mean()
        wr120_larger_than_50_days_avg = sorted_df['wr120_larger_than_50_days'].mean()
        wr120_larger_than_80_days_avg = sorted_df['wr120_larger_than_80_days'].mean()
        obv_above_zero_days_std = sorted_df['obv_above_zero_days'].std()
        cum_turnover_std = sorted_df['cum_turnover'].std()
        wr34_std = sorted_df['wr34'].std()
        wr120_std = sorted_df['wr120'].std()
        wr120_larger_than_50_days_std = sorted_df['wr120_larger_than_50_days'].std()
        wr120_larger_than_80_days_std = sorted_df['wr120_larger_than_80_days'].std()
        lst = [[column,obv_above_zero_days_avg,cum_turnover_avg,wr34_avg,wr120_avg,wr120_larger_than_50_days_avg,wr120_larger_than_80_days_avg,obv_above_zero_days_std,cum_turnover_std,wr34_std,wr120_std,wr120_larger_than_50_days_std,wr120_larger_than_80_days_std]]
        statistics_df = pd.DataFrame(lst,columns=['period','obv_above_zero_days','cum_turnover','wr34','wr120','wr120_larger_than_50_days','wr120_larger_than_80_days','obv_above_zero_days_std','cum_turnover_std','wr34_std','wr120_std','wr120_larger_than_50_days_std','wr120_larger_than_80_days_std'])
        sorted_df.to_csv(analyzed_topX_data_path + f'{period}' + 'days.csv')
        statistics_df.to_csv(analyzed_topX_data_path + f'{period}' + 'days_statistics.csv')
    return statistics_df

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
    
    async_results = []
    pool = Pool(len(periods))
    for period in periods:
        async_result = pool.apply_async(drop_qfq,args=(df,qfq,period))
        async_results.append(async_result)
    pool.close()
    pool.join()

    statistics_df = pd.DataFrame()
    for async_result in async_results:
        result = async_result.get()
        statistics_df = statistics_df.append(result)
    statistics_df.to_csv(analyzed_topX_data_path + 'statistics.csv')