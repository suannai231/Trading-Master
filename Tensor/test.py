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

data = df[['close', 'volume']]

# Normalize the values of the close and volume features
scaler = MinMaxScaler()
data = scaler.fit_transform(data)

# Split the data into training, validation, and test sets
train_data, test_data = train_test_split(data, test_size=0.2, shuffle=False)
train_data, val_data = train_test_split(train_data, test_size=0.2, shuffle=False)

# Convert the data into numpy arrays
train_data = np.array(train_data)
val_data = np.array(val_data)
test_data = np.array(test_data)

# Split the data into features and labels
train_features = train_data[:, :-1]
train_labels = train_data[:, -1]
val_features = val_data[:, :-1]
val_labels = val_data[:, -1]
test_features = test_data[:, :-1]
test_labels = test_data[:, -1]

# Define the neural network architecture
model = tf.keras.Sequential([
    tf.keras.layers.Dense(64, activation='relu', input_shape=(2,)),
    tf.keras.layers.Dense(32, activation='relu'),
    tf.keras.layers.Dense(1)
])

# Compile the model
model.compile(optimizer='adam', loss='mean_squared_error')

# Train the model
history = model.fit(train_features, train_labels, epochs=50, batch_size=32, validation_data=(val_features, val_labels))

# Make predictions on the test data
predictions = model.predict(test_features)

# Un-normalize the predicted values
predictions = scaler.inverse_transform(predictions)

# Plot the actual and predicted values
plt.plot(test_labels, label='Actual')
plt.plot(predictions, label='Predicted')
plt.legend()
plt.show()