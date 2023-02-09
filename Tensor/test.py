import os
import numpy as np
import pandas as pd
import tensorflow as tf
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM

print("TensorFlow version:", tf.__version__)
print("cuDNN enabled:", tf.test.is_built_with_cuda())

# os.environ["CUDA_VISIBLE_DEVICES"] = "1" # change 0 to the number of the GPU you want to use
print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))

from tensorflow.compat.v1 import GPUOptions
from tensorflow.compat.v1 import ConfigProto

config = ConfigProto()
config.gpu_options.allow_growth = True
config.gpu_options.per_process_gpu_memory_fraction = 0.9

# Load the stock data
# df = pd.read_csv('stock_data.csv')
raw_data_path='//jack-nas.home/Work/Python/RawData/'
raw_data_files = os.listdir(raw_data_path)
df = pd.read_feather(raw_data_path + raw_data_files[-1])
# Prepare the data for modeling
df = df[df.ticker=="UBX"]
data = df.set_index("date",drop=True)
data = data.drop(columns=["ticker"])
# Load the stock data and drop any rows with missing values
# data = pd.read_csv('stock_data.csv').dropna()

# Normalize the data using MinMaxScaler
scaler = MinMaxScaler()
data = scaler.fit_transform(data)

# Split the data into training and test sets
train_data = data[:int(data.shape[0] * 0.8), :]
test_data = data[int(data.shape[0] * 0.8):, :]

# Create a function to generate time-step data for the LSTM model
def generate_data(data, time_step):
    X, Y = [], []
    for i in range(data.shape[0] - time_step - 1):
        X.append(data[i: i + time_step, :])
        Y.append(data[i + time_step, -1])
    return np.array(X), np.array(Y)

time_step = 30
X_train, Y_train = generate_data(train_data, time_step)
X_test, Y_test = generate_data(test_data, time_step)

# Define the LSTM model
model = Sequential()
model.add(LSTM(units=128, input_shape=(X_train.shape[1], X_train.shape[2])))
model.add(Dense(1))

# Compile the model and set the optimizer to use GPU acceleration
model.compile(loss='mean_squared_error', optimizer=tf.keras.optimizers.RMSprop(0.001))

# Train the model on the training data
model.fit(X_train, Y_train, epochs=100, batch_size=32, verbose=2)

# Evaluate the model on the test data
score = model.evaluate(X_test, Y_test)
print(f'Test MSE: {score}')

# Make predictions on the test data
predictions = model.predict(X_test)

# Plot the prediction results
plt.plot(Y_test, label='True')
plt.plot(predictions, label='Predicted')
plt.legend()
plt.show()