from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
from multiprocessing import Pool
# import math

def analyze(sorted_df,qfq,period):
    for i in sorted_df.index:
        ticker = sorted_df['ticker'][i]
        ticker_date = sorted_df['date'][i]

        for date in qfq[qfq.ticker==ticker].date:   # remove qfq
            start = ticker_date.date()
            end = date.date()
            busdays = np.busday_count( start, end)
            if (busdays > 0) & (busdays<=period+1):
                sorted_df.drop(index=i,inplace=True)
                break
    
    if not sorted_df.empty:
        clean_df = sorted_df.reset_index(drop=True)
        del sorted_df
        clean_df.to_csv(analyzed_data_path + f'{period}' + 'days.csv')
        clean_df.describe().to_csv(analyzed_data_path + f'{period}' + 'days_describe.csv')
        describe_df = clean_df.describe()
        column = 'change_'+str(period)+'days'
        describe_df['period'] = period
        describe_df[column+'_per_day'] = describe_df[column]/period
    return describe_df

end = datetime.date.today()
processed_data_path=f"//jack-nas/Work/Python/ProcessedData/"
analyzed_data_path=f"//jack-nas/Work/Python/AnalyzedData/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(analyzed_data_path)
    if not isPathExists:
        os.makedirs(analyzed_data_path)

    analyzed_topX_data_files = os.listdir(analyzed_data_path)
    periods = range(5,61,5)
    for period in periods:
        analyzed_topX_data_file = f'{period}'+'days.csv'
        if analyzed_topX_data_file in analyzed_topX_data_files:
            exit()

    df = pd.read_feather(processed_data_path + f'{end}' + '.feather')
    df = df[df['date'] > '2017-01-01']
    qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    qfq = qfq[qfq['date'] > '2017-01-01']
    
    async_results = []
    pool = Pool(len(periods))
    for period in periods:
        column = 'change_' + str(period) +'days'
        selected_df = df.dropna(subset=[column],inplace=False)
        selected_df = selected_df.loc[df[column] > period*0.2]
        if selected_df.empty:
            continue
        selected_df.reset_index(drop=True,inplace=True)
        sorted_df = selected_df.sort_values(by=[column],ascending=False,ignore_index=True)
        if sorted_df.empty:
            continue
        async_result = pool.apply_async(analyze,args=(sorted_df,qfq,period))
        async_results.append(async_result)
    pool.close()
    pool.join()

    results_df = pd.DataFrame()
    for async_result in async_results:
        result_df = async_result.get()
        results_df = results_df.append(result_df)
    results_df.to_csv(analyzed_data_path + 'all periods.csv')
    # results_df.describe().to_csv(analyzed_data_path + 'all periods_describe.csv')
    os.popen(f'python C:/Code/One/screen_data_One.py')