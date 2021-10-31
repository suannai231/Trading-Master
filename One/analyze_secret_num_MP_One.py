from yahoo_fin import stock_info as si
import pandas as pd
import numpy as np
import datetime
import os
from multiprocessing import Pool
# import math

def clean(sorted_df,qfq,period):
    for i in sorted_df.index:
        ticker = sorted_df['ticker'][i]
        ticker_date = sorted_df['date'][i]

        for date in qfq[qfq.ticker==ticker].date:
            start = ticker_date.date()
            end = date.date()
            busdays = np.busday_count( start, end)
            if (busdays > 0) & (busdays<=period+1):
                sorted_df.drop(index=i,inplace=True)
                break
    
    if not sorted_df.empty:
        clean_df = sorted_df.reset_index(drop=True)
        del sorted_df
        # change_avg = sorted_df['change'].mean()
        # change_5days_avg = sorted_df['change_5days'].mean()
        # change_10days_avg = sorted_df['change_10days'].mean()
        # change_15days_avg = sorted_df['change_15days'].mean()
        # change_20days_avg = sorted_df['change_20days'].mean()
        # change_25days_avg = sorted_df['change_25days'].mean()
        # change_30days_avg = sorted_df['change_30days'].mean()
        # change_35days_avg = sorted_df['change_35days'].mean()
        # change_40days_avg = sorted_df['change_40days'].mean()
        # change_45days_avg = sorted_df['change_45days'].mean()
        # change_50days_avg = sorted_df['change_50days'].mean()
        # change_55days_avg = sorted_df['change_55days'].mean()
        # change_60days_avg = sorted_df['change_60days'].mean()
        # turn_avg = sorted_df['turn'].mean()
        # obv_above_zero_days_avg = sorted_df['obv_above_zero_days'].mean()
        # obv_diff_rate_avg = sorted_df['OBV_DIFF_RATE'].mean()
        # cum_turnover_avg = sorted_df['cum_turnover'].mean()
        # wr34_avg = sorted_df['wr34'].mean()
        # wr120_avg = sorted_df['wr120'].mean()
        # wr120_larger_than_50_days_avg = sorted_df['wr120_larger_than_50_days'].mean()
        # wr120_larger_than_80_days_avg = sorted_df['wr120_larger_than_80_days'].mean()
        # change_std = sorted_df['change'].std()
        # change_5days_std = sorted_df['change_5days'].std()
        # change_10days_std = sorted_df['change_10days'].std()
        # change_15days_std = sorted_df['change_15days'].std()
        # change_20days_std = sorted_df['change_20days'].std()
        # change_25days_std = sorted_df['change_25days'].std()
        # change_30days_std = sorted_df['change_30days'].std()
        # change_35days_std = sorted_df['change_35days'].std()
        # change_40days_std = sorted_df['change_40days'].std()
        # change_45days_std = sorted_df['change_45days'].std()
        # change_50days_std = sorted_df['change_50days'].std()
        # change_55days_std = sorted_df['change_55days'].std()
        # change_60days_std = sorted_df['change_60days'].std()
        # turn_std = sorted_df['turn'].std()
        # obv_above_zero_days_std = sorted_df['obv_above_zero_days'].std()
        # cum_turnover_std = sorted_df['cum_turnover'].std()
        # wr34_std = sorted_df['wr34'].std()
        # wr120_std = sorted_df['wr120'].std()
        # wr120_larger_than_50_days_std = sorted_df['wr120_larger_than_50_days'].std()
        # wr120_larger_than_80_days_std = sorted_df['wr120_larger_than_80_days'].std()
        # lst = [[column,change_avg,change_5days_avg,change_10days_avg,change_15days_avg,change_20days_avg,change_25days_avg,change_30days_avg,change_35days_avg,change_40days_avg,change_45days_avg,change_50days_avg,change_55days_avg,change_60days_avg,turn_avg,obv_above_zero_days_avg,obv_diff_rate_avg,cum_turnover_avg,wr34_avg,wr120_avg,wr120_larger_than_50_days_avg,wr120_larger_than_80_days_avg,change_std,change_5days_std,change_10days_std,change_15days_std,change_20days_std,change_25days_std,change_30days_std,change_35days_std,change_40days_std,change_45days_std,change_50days_std,change_55days_std,change_60days_std,turn_std,obv_above_zero_days_std,cum_turnover_std,wr34_std,wr120_std,wr120_larger_than_50_days_std,wr120_larger_than_80_days_std]]
        # statistics_df = pd.DataFrame(lst,columns=['period','change_avg','change_5days_avg','change_10days_avg','change_15days_avg','change_20days_avg','change_25days_avg','change_30days_avg','change_35days_avg','change_40days_avg','change_45days_avg','change_50days_avg','change_55days_avg','change_60days_avg','turn_avg','obv_above_zero_days','OBV_DIFF_RATE','cum_turnover','wr34','wr120','wr120_larger_than_50_days','wr120_larger_than_80_days','change_std','change_5days_std','change_10days_std','change_15days_std','change_20days_std','change_25days_std','change_30days_std','change_35days_std','change_40days_std','change_45days_std','change_50days_std','change_55days_std','change_60days_std','turn_std','obv_above_zero_days_std','cum_turnover_std','wr34_std','wr120_std','wr120_larger_than_50_days_std','wr120_larger_than_80_days_std'])
        clean_df.to_csv(analyzed_secret_num_data_path + f'{period}' + 'days.csv')
        clean_df.describe().to_csv(analyzed_secret_num_data_path + f'{period}' + 'days_describe.csv')
        # statistics_df.to_csv(analyzed_secret_num_data_path + f'{period}' + 'days_statistics.csv')
        describe_df = clean_df.describe()
        describe_df['period'] = period
    return describe_df

end = datetime.date.today()
secret_num_data_path=f"//jack-nas/Work/Python/SecretNum/"
analyzed_secret_num_data_path=f"//jack-nas/Work/Python/AnalyzedSecretNum/"
qfq_path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(analyzed_secret_num_data_path)
    if not isPathExists:
        os.makedirs(analyzed_secret_num_data_path)

    analyzed_topX_data_files = os.listdir(analyzed_secret_num_data_path)
    periods = range(5,61,5)
    for period in periods:
        analyzed_topX_data_file = f'{period}'+'days.csv'
        if analyzed_topX_data_file in analyzed_topX_data_files:
            exit()

    df = pd.read_feather(secret_num_data_path + f'{end}' + '.feather')
    qfq = pd.read_feather(qfq_path+f'{end}'+'_qfq.feather')
    
    async_results = []
    pool = Pool(len(periods))
    for period in periods:
        column = 'change_' + str(period) +'days'
        selected_df = df.dropna(subset=[column],inplace=False)
        selected_df = selected_df.loc[(df['cum_turnover'] > 1) & (df[column] > 1)]
        if selected_df.empty:
            continue
        selected_df.reset_index(drop=True,inplace=True)
        sorted_df = selected_df.sort_values(by=[column],ascending=False,ignore_index=True)[0:1000]
        if sorted_df.empty:
            continue
        async_result = pool.apply_async(clean,args=(sorted_df,qfq,period))
        async_results.append(async_result)
    pool.close()
    pool.join()

    results_df = pd.DataFrame()
    for async_result in async_results:
        result_df = async_result.get()
        results_df = results_df.append(result_df)
    results_df.to_csv(analyzed_secret_num_data_path + 'all periods.csv')
    # results_df.describe().to_csv(analyzed_secret_num_data_path + 'all periods_describe.csv')