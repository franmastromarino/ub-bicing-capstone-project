import tensorflow as tf
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense, LSTM, GRU, SimpleRNN, Lambda
import tensorflow.keras.backend as K
from tensorflow.keras.optimizers import RMSprop
from tensorflow.keras.models import load_model

import keras

import matplotlib.pyplot as plt


lstm_units= 12
dense_units = 6

LSTM_model = keras.models.Sequential([
    keras.layers.LSTM(units=lstm_units, input_shape=[None, n_features], return_sequences=False, dropout=0.1, recurrent_dropout=0.1),
    keras.layers.Dense(dense_units, activation='relu'),
    keras.layers.Dense(output_size, activation='linear')
])


LSTM_model.compile(optimizer='rmsprop', loss='mae')


lstm_filepath = os.path.join(results_path, "lstm.h5")

# Create a ModelCheckpoint callback
checkpoint = ModelCheckpoint(lstm_filepath, monitor='val_loss', verbose=1, save_best_only=True, mode='min')

# Create an EarlyStopping callback
early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

# Train the model
lstm_history = LSTM_model.fit(
    X_train, 
    y_train, 
    epochs=100, 
    validation_data=(X_test, y_test), 
    batch_size=20, 
    shuffle=False, 
    callbacks=[early_stopping, checkpoint]
)
LSTM_model.save(lstm_filepath)