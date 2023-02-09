import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import os
import math
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split


raw_data_path='//jack-nas.home/Work/Python/RawData/'
raw_data_files = os.listdir(raw_data_path)
df = pd.read_feather(raw_data_path + raw_data_files[-1])
# Prepare the data for modeling
df = df[df.ticker=="MARK"]
df = df.set_index("date",drop=True)
df = df.drop(columns=["ticker"])

# Prepare the data by extracting close price and volume
features = df[["close", "volume"]].values
labels = df["close"].values

# Normalize the data
scaler = MinMaxScaler()
features = scaler.fit_transform(features)

# Split the data into training and testing sets
split = int(0.7 * features.shape[0])
train_features = features[:split, :]
train_labels = labels[:split]
test_features = features[split:, :]
test_labels = labels[split:]

# Reshape the data for LSTM
train_features = train_features.reshape(train_features.shape[0], 1, train_features.shape[1])
test_features = test_features.reshape(test_features.shape[0], 1, test_features.shape[1])

# Create the LSTM model
model = Sequential()
model.add(LSTM(50, return_sequences=True, input_shape=(train_features.shape[1], train_features.shape[2])))
model.add(LSTM(50, return_sequences=False))
model.add(Dense(1))

# Compile the model
model.compile(loss="mean_squared_error", optimizer="adam")

# Train the model
model.fit(train_features, train_labels, epochs=100, batch_size=32, verbose=0)

# Make predictions on the test data
predictions = model.predict(test_features)

# Invert the normalization to get the actual close prices
predictions = scaler.inverse_transform(predictions.reshape(-1, 1))
test_labels = scaler.inverse_transform(test_labels.reshape(-1, 1))

# Plot the actual close price and the predicted close price
plt.plot(test_labels, label='Actual close Price')
plt.plot(predictions, label='Predicted close Price')
plt.legend()
plt.show()