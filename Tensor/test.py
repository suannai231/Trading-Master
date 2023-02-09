import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import os
import math
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import multiprocessing
from multiprocessing import Pool
import logging
import datetime
import time

gpus = tf.config.experimental.list_physical_devices('GPU')
for gpu in gpus:
  tf.config.experimental.set_memory_growth(gpu, True)

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
        
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def train(ticker_chunk_df):
    tickers= ticker_chunk_df.ticker.unique()
    for ticker in tickers:
        picture_file = picture_path+ticker+".png"
        if not os.path.exists(picture_file):
            ticker_df = ticker_chunk_df[ticker_chunk_df.ticker==ticker]
            ticker_df = ticker_df.set_index("date",drop=True)
            ticker_df = ticker_df.drop(columns=["ticker"])

            # Prepare the data by extracting close price and volume
            features = ticker_df[["close", "volume"]].values
            labels = ticker_df["close"].values

            # Normalize the data
            scaler = MinMaxScaler()
            features = scaler.fit_transform(features)

            # Split the data into training and testing sets
            split = int(0.8 * features.shape[0])
            train_features = features[:split, :]
            train_labels = labels[:split]
            test_features = features[split:, :]
            test_labels = labels[split:]

            # Reshape the data for LSTM
            train_features = train_features.reshape(train_features.shape[0], 1, train_features.shape[1])
            test_features = test_features.reshape(test_features.shape[0], 1, test_features.shape[1])

            model_file = model_path+ticker+".h5"
            if os.path.exists(model_file):
                model = keras.models.load_model(model_file)
            else:
                # Create the LSTM model
                model = Sequential()
                model.add(LSTM(50, return_sequences=True, input_shape=(train_features.shape[1], train_features.shape[2])))
                model.add(LSTM(50, return_sequences=False))
                model.add(Dense(1))

                # Compile the model
                model.compile(loss="mean_squared_error", optimizer="adam")

                # Train the model
                model.fit(train_features, train_labels, epochs=100, batch_size=32, verbose=0)
                model.save(model_file)


            # Invert the normalization to get the actual close prices
            # predictions = scaler.inverse_transform(predictions.reshape(-1, 1))
            # test_labels = scaler.inverse_transform(test_labels.reshape(-1, 1))


            # Make predictions on the test data
            predictions = model.predict(test_features)
            # Plot the actual close price and the predicted close price
            plt.plot(test_labels, label='Actual close Price')
            plt.plot(predictions, label='Predicted close Price')
            plt.legend()
            plt.savefig(picture_path+ticker+".png")
            plt.clf()
            # plt.show()

if __name__ == '__main__':
    log('info',"process_data start.")
    raw_data_files = os.listdir(raw_data_path)
    while len(raw_data_files) == 0:
        log('warning',"raw data not ready, sleep 10 seconds...")
        time.sleep(10)
    
    log('info',"processing "+raw_data_files[-1])
    try:
        df = pd.read_feather(raw_data_path + raw_data_files[-1])
        log('info',raw_data_path + raw_data_files[-1]+" loaded.")
    except Exception as e:
        log('critical',str(e))
        exit()

    tickers = df.ticker.unique()
    cores = 12 #int(multiprocessing.cpu_count())
    ticker_chunk_list = list(chunks(tickers,math.ceil(len(tickers)/cores)))
    pool = Pool(cores)
    async_results = []
    for ticker_chunk in ticker_chunk_list:
        ticker_chunk_df = df[df['ticker'].isin(ticker_chunk)]
        async_result = pool.apply_async(train, args=(ticker_chunk_df,))
        async_results.append(async_result)
    pool.close()
    del(df)
    log('info',"process pool start.")
    pool.join()
    log('info',"process pool stop.")
  