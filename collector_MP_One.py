from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import datetime
import os
import multiprocessing
from multiprocessing import Pool
from multiprocessing import Manager

days=365*10

def run(ticker,df_list):
    shares = -1
    try:
        quote_data = si.get_quote_data(ticker)
        shares = quote_data['sharesOutstanding']
    except Exception as e:
        if (str(e) == "'sharesOutstanding'") | (str(e) == 'Invalid response from server.  Check if ticker is\n                              valid.'):
            try:
                info = yf.Ticker(ticker).info
                shares = info['sharesOutstanding']
            except Exception as e:
                if str(e) == "'sharesOutstanding'":
                    return 0
                elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
                    return -1
                else:
                    return 0
        elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            return 0
    if (shares is None):
        return 0
    else:
        if int(shares) < 1:
            return 0
    try:
        df = si.get_data(ticker,start, end)
    except Exception as e:
        if (str(e) == "'timestamp'") | (str(e) == "'NoneType' object is not subscriptable"):
            return 0
        elif str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            return 0
    if df.empty:
        return 0
    df["shares"] = shares
    df["marketCap"] = df["close"]*shares
    df_list.append(df)
    return 1

start = datetime.datetime.now() - datetime.timedelta(days)
end = datetime.date.today()

path = '//jack-nas/Work/Python/RawData/'

if __name__ == '__main__':
    isPathExists = os.path.exists(path)
    if not isPathExists:
        os.makedirs(path)

    nasdaq = si.tickers_nasdaq()
    other = si.tickers_other()
    tickers = nasdaq + other #+ dow + sp500
    files = os.listdir(path)
    file = str(end)+'.csv'
    if file in files:
        exit()

    with Manager() as manager:
        df_list = manager.list()
        cores = multiprocessing.cpu_count()
        i = 0
        Loop = True
        while Loop:
            Loop = False
            if cores-i != 0:
                with Pool(cores-i) as p:
                    async_result_list = []
                    for ticker in tickers:
                        async_result = p.apply_async(run, args=(ticker,df_list))
                        async_result_list.append(async_result)
                    p.close()
                    p.join()
                    
                    success_num = 0
                    for async_result in async_result_list:
                        result = async_result.get()
                        if result == -1:
                            i+=1
                            Loop = True
                            print(str(cores-i)+' processes didn\'t work, restarting...\n')
                            break
                        success_num += result
                                       
                    if Loop == False:
                        save = pd.DataFrame()
                        for df in df_list:
                            save = save.append(df, ignore_index=True)
                        df.to_csv(path + f'{end}' + '.csv')
                        print(str(success_num)+' tickers raw data have been saved.\n')
    # os.popen(f'python C:/Users/jayin/OneDrive/Code/prepare_data_MP.py')