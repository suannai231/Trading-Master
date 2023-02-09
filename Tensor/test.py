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

data = df[['Close', 'Volume']]

# Normalize the values of the two features
scaler = MinMaxScaler()
data[['Close', 'Volume']] = scaler.fit_transform(data[['Close', 'Volume']])

# Split the data into training, validation, and test sets
train_data, test_data = train_test_split(data, test_size=0.2, shuffle=False)
train_data, val_data = train_test_split(train_data, test_size=0.2, shuffle=False)

# Convert the close prices into labels that can be used to train the neural network
train_labels = train_data.pop('Close')
val_labels = val_data.pop('Close')
test_labels = test_data.pop('Close')

# Define the neural network architecture
model = tf.keras.Sequential([
    tf.keras.layers.Dense(64, activation='relu', input_shape=(1,)),
    tf.keras.layers.Dense(32, activation='relu'),
    tf.keras.layers.Dense(1)
])

# Compile the model
model.compile(optimizer='adam', loss='mean_squared_error')

# Train the model
history = model.fit(train_data, train_labels, epochs=50, batch_size=32, validation_data=(val_data, val_labels))

# Make predictions on the test data
predictions = model.predict(test_data)

# Un-normalize the predicted values to get the actual close prices
predictions = scaler.inverse_transform(predictions)

# Plot the actual close prices and the predicted close prices
plt.plot(range(len(test_labels)), test_labels, label='Actual Close Price')
plt.plot(range(len(test_labels)), predictions, label='Predicted Close Price')
plt.legend()
plt.show()