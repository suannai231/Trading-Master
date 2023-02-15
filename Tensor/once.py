import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import os
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import logging
import datetime
from yahoo_fin import stock_info as si

gpus = tf.config.experimental.list_physical_devices('GPU')
for gpu in gpus:
  tf.config.experimental.set_memory_growth(gpu, True)

days=365*5
end = datetime.date.today()
start = end - datetime.timedelta(days)

model_path='//jack-nas.home/Work/Python/Models/'
picture_path='//jack-nas.home/Work/Python/Pictures/'
raw_data_path='//jack-nas.home/Work/Python/RawData/'

def log(type,string):
    logpath = '//jack-nas.home/Work/Python/'
    logfile = logpath + datetime.datetime.now().strftime("%m%d%Y") + "_train.log"
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
    
    now = datetime.datetime.now()
    log_time = now.strftime("%m%d%Y-%H%M%S")
    if type=='info':
        logging.info(log_time+":"+string)
    elif type=='warning':
        logging.warning(log_time+":"+string)
    elif type=='error':
        logging.error(log_time+":"+string)
    elif type=='critical':
        logging.critical(log_time+":"+string)

def get_stock_history(ticker):
    df = pd.DataFrame()
    try:
        df = si.get_data(ticker,start,end,index_as_date=False)
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            # log("error","get_stock_history " + ticker + " " + str(e))
            return pd.DataFrame()
    # df.index.name = 'date'
    # if (not df.empty) and df.index[-1]!=datetime.date.today():
    #     log("error",ticker+" date error")
    #     return pd.DataFrame()
    return df

def get_stock_realtime(ticker):
    df = pd.DataFrame()
    try:
        close = float(si.get_live_price(ticker))
        stock_volume = si.get_quote_table('AAPL')['Volume']
        # quote_table = si.get_quote_table(ticker)
        # open = float(quote_table['Open'])
        # low = float(quote_table["Day's Range"].split(" - ")[0])
        # high = float(quote_table["Day's Range"].split(" - ")[1])
        # volume = int(quote_table['Volume'])
        open = np.nan
        low = np.nan
        high = np.nan
        volume = stock_volume
        d = {'date':end, 'open':open,'high':high,'low':low,'close':close,'adjclose':close,'volume':volume,'ticker':ticker}
        # df=pd.DataFrame(d,index=[str(end)])
        df=pd.DataFrame(d,index=[str(end)])
        df.date=pd.to_datetime(df.date)
        # df.index.name = 'date'
        # if (not df.empty) and df.index[-1]!=str(datetime.date.today()):
        #     log("error",ticker+" date error")
        #     return pd.DataFrame()
    except Exception as e:
        if str(e).startswith('HTTPSConnectionPool') | str(e).startswith("('Connection aborted.'"):
            return -1
        else:
            # log("error","get_stock_realtime " + ticker + " " + str(e))
            return pd.DataFrame()
    # if ticker=="NEXA":
    #     log("info",ticker)
    return df

# Function to preprocess data using MinMaxScaler
def preprocess_data(data):
    scaler = MinMaxScaler()
    data = scaler.fit_transform(data)
    return data, scaler

def train(ticker,df):
    # Select features
    features = ['close', 'volume']
    data = df[features].values

    # Preprocess data
    data, scaler = preprocess_data(data)

    # Set lookback window size
    lookback = 20

    # Split data into training and testing sets
    train_size = int(len(data) * 0.8)
    train_data = data[:train_size]
    test_data = data[train_size:]

    # Create X and y for LSTM
    def create_dataset(dataset, lookback):
        X, y = [], []
        for i in range(len(dataset)-lookback-1):
            X.append(dataset[i:(i+lookback), :])
            y.append(dataset[i + lookback, 0])
        return np.array(X), np.array(y)

    X_train, y_train = create_dataset(train_data, lookback)
    X_test, y_test = create_dataset(test_data, lookback)

    # Define LSTM model
    model = Sequential()
    model.add(LSTM(64, input_shape=(X_train.shape[1], X_train.shape[2]), return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(32))
    model.add(Dropout(0.2))
    model.add(Dense(1))

    # Compile model
    model.compile(loss='mean_squared_error', optimizer='adam')

    # Train model
    model.fit(X_train, y_train, epochs=100, batch_size=32, verbose=1)

    # Evaluate model
    train_score = model.evaluate(X_train, y_train, verbose=0)
    test_score = model.evaluate(X_test, y_test, verbose=0)
    print(f'Train Score: {train_score:.2f}')
    print(f'Test Score: {test_score:.2f}')

    # Predict prices
    # train_predict = model.predict(X_train)
    test_predict = model.predict(X_test)

    # Inverse transform predictions and actual prices
    # train_predict = scaler.inverse_transform(np.concatenate((train_predict, X_train[:, -1, 1:]), axis=1))[:, 0]
    test_predict = scaler.inverse_transform(np.concatenate((test_predict, X_test[:, -1, 1:]), axis=1))[:, 0]
    train_actual = scaler.inverse_transform(np.concatenate((y_train.reshape(-1, 1), X_train[:, -1, 1:]), axis=1))[:, 0]
    test_actual = scaler.inverse_transform(np.concatenate((y_test.reshape(-1, 1), X_test[:, -1, 1:]), axis=1))[:, 0]

    # Plot actual and predicted prices
    plt.figure(figsize=(16, 8))
    # plt.plot(df['date'], train_actual, label='Actual (train)')
    # plt.plot(df['date'][lookback+1:lookback+1+len(train_predict)], train_predict, label='Predicted (train)')
    plt.plot(df['date'][train_size+lookback+1:train_size+lookback+1+len(test_predict)], test_predict, label='Predicted (test)')
    plt.plot(df['date'][train_size+lookback+1:train_size+lookback+1+len(test_actual)], test_actual, label='Actual (test)')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Stock Prices')
    plt.legend()
    plt.savefig(picture_path+ticker+".png",dpi=300)
    plt.clf()

def train_old(ticker_chunk_df):
    tickers= ticker_chunk_df.ticker.unique()
    picture_files = os.listdir(picture_path)
    # model_files = os.listdir(model_path)
    for ticker in tickers:
        picture_file = ticker+".png"
        if picture_file in picture_files:
            continue
        else:
        # if not os.path.exists(picture_file):
            data = ticker_chunk_df.set_index("date",drop=True)
            # data = data.drop(columns=["ticker"])
            data = data.iloc[:,[3,5]].values

            # Split the data into training and testing sets
            train_data = data[:int(0.8*len(data))]
            test_data = data[int(0.8*len(data)):]

            # Normalize the data
            scaler = MinMaxScaler()
            train_data = scaler.fit_transform(train_data)
            test_data = scaler.transform(test_data)

            # Prepare the training and testing data
            def prepare_data(data, lookback):
                X, Y = [], []
                for i in range(len(data) - lookback - 1):
                    X.append(data[i:(i+lookback), 0])
                    Y.append(data[(i+lookback), 0])
                return np.array(X), np.array(Y)

            lookback = 20
            X_train, Y_train = prepare_data(train_data, lookback)
            X_test, Y_test = prepare_data(test_data, lookback)

            # Define the model
            model = Sequential()
            model.add(LSTM(50, input_shape=(lookback, 1)))
            model.add(Dropout(0.2))
            model.add(Dense(1))
            model.compile(loss='mean_squared_error', optimizer='adam')

            # Train the model
            model.fit(X_train.reshape(X_train.shape[0], X_train.shape[1], 1), Y_train, epochs=100, batch_size=32)

            # Evaluate the model on the test data
            predictions = model.predict(X_test.reshape(X_test.shape[0], X_test.shape[1], 1))
            predictions = scaler.inverse_transform(predictions)
            Y_test = scaler.inverse_transform(Y_test.reshape(-1, 1))
            plt.plot(predictions, label='Predictions')
            plt.plot(Y_test, label='Actual')
            plt.legend()
            plt.savefig(picture_path+ticker+".png",dpi=300)
            plt.clf()
            # plt.show()

if __name__ == '__main__':
    ticker = "FUSN"
    try:
        sh_df=get_stock_history(ticker)
        sr_df=get_stock_realtime(ticker)
        sr_df.reset_index(inplace=True,drop=True)
        stock_concat_df = pd.concat([sh_df,sr_df])
        stock_concat_df.reset_index(inplace=True,drop=True)
    except Exception as e:
        log('critical',str(e))
        exit()

    train(ticker,stock_concat_df)
    # tickers = df.ticker.unique()
    # cores = 2 #int(multiprocessing.cpu_count())
    # ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    # pool = Pool(cores)
    # async_results = []
    # for ticker_chunk in ticker_chunk_list:
    #     ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
    #     async_result = pool.apply_async(train, args=(ticker_chunk_df,))
    #     async_results.append(async_result)
    # pool.close()
    # del(df)
    # log('info',"process pool start.")
    # pool.join()
    # log('info',"process pool stop.")
  