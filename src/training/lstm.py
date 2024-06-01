import tensorflow as tf
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense, LSTM, GRU, SimpleRNN, Lambda
import tensorflow.keras.backend as K
from tensorflow.keras.optimizers import RMSprop, Adam
from tensorflow.keras.models import load_model

import keras

import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, Dense, Concatenate
import tensorflow as tf



def create_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(50, input_shape=(input_shape)))
    model.add(Dense(1))
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mean_squared_error')
    return model




# # Assuming df is your DataFrame with all features and target 'price'
# df = pd.read_csv("data/processed/groupby/stations_final_2023.csv")

# # Separate time series features and non-time series features
# time_series_features = ['station_id', 'month', 'day', 'hour', 'ctx-4, ctx-3, ctx-2, ctx-1']  # Example time series features
# non_time_series_features = ['altitud']  # Example non-time series features

# # Prepare the inputs
# X_time_series = df[time_series_features].values.reshape((len(df), len(time_series_features), 1))
# X_non_time_series = df[non_time_series_features].values
# # pred
# y = df['price'].values

# # Split the data into training and test sets
# X_train_time_series, X_test_time_series, X_train_non_time_series, X_test_non_time_series, y_train, y_test = train_test_split(
#     X_time_series, X_non_time_series, y, test_size=0.2, random_state=42)

# # Define the LSTM model for time series features
# input_time_series = Input(shape=(len(time_series_features), 1))
# x_time_series = LSTM(50, activation='relu')(input_time_series)
# x_time_series = Dense(10, activation='relu')(x_time_series)

# # Define the dense model for non-time series features
# input_non_time_series = Input(shape=(len(non_time_series_features),))
# x_non_time_series = Dense(50, activation='relu')(input_non_time_series)
# x_non_time_series = Dense(10, activation='relu')(x_non_time_series)

# # Concatenate both parts
# combined = Concatenate()([x_time_series, x_non_time_series])

# # Add a final dense layer for the output
# output = Dense(1)(combined)

# # Define the final model
# model = Model(inputs=[input_time_series, input_non_time_series], outputs=output)
# model.compile(optimizer='adam', loss='mse')

# # Train the model
# history = model.fit([X_train_time_series, X_train_non_time_series], y_train, epochs=50, batch_size=32, validation_split=0.2)

# # Evaluate the model
# loss = model.evaluate([X_test_time_series, X_test_non_time_series], y_test)
# print(f'Test loss: {loss}')

# # Make predictions
# predictions = model.predict([X_test_time_series, X_test_non_time_series])


# lstm_filepath = "lstm.h5"

# # Create a ModelCheckpoint callback
# checkpoint = ModelCheckpoint(lstm_filepath, monitor='val_loss', verbose=1, save_best_only=True, mode='min')

# # Create an EarlyStopping callback
# early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)


# model.save(lstm_filepath)