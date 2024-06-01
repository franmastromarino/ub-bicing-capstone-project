"""
Requirements: mlflow
pip install databricks-cli
TODO: Add requirements to requirements.txt file
TODO: Model name should appear in databricks
"""
#Import libraries

import subprocess
import mlflow
from training_utils import eval_metrics, plot_real_vs_prediction


import numpy as np
import pandas as pd
import os

import matplotlib.pyplot as plt

import sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor


from sklearn.linear_model import LinearRegression
from sklearn.svm import LinearSVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

#This is not used, could be used for the subprocess
my_username = "ulisesreytorne@gmail.com"
databricks_base_url = f'/Users/{my_username}'
my_password = "Loscinco5!"
print("El usuario y la contraseña son:")
print(my_username)
print(my_password)


# Define the command as a list of arguments
command = ["databricks", "configure", "--host", "https://community.cloud.databricks.com/"]

# Use subprocess to execute the command with username and password
process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE)

process.communicate(input=f"{my_username}\n{my_password}\n".encode())

mlflow.set_tracking_uri("databricks")

mlflow.set_experiment(f'{databricks_base_url}/bicing')


def train_model(x, y, xt, yt, model, **model_kwargs):
  """
  TODO: Write docstring
  """

  # Start to log an experiment
  # A name can be set to distinguish the experiments: run_name='myname'
  with mlflow.start_run():

    #print(f'Starting experiment with learning_rate={learning_rate}, n_estimators={n_estimators}, max_depth={max_depth}')

    # Log the parameters we will use to create the model to MLFlow
    for kwarg in model_kwargs:
        mlflow.log_param(kwarg, model_kwargs[kwarg])


    # Fit the model to the data
    model.fit(x, y)

    # Predict the test data
    yp = model.predict(xt)

    #Save it in a dataframe
    prediction_df = pd.DataFrame(yp, columns=["percentage_docks_available"])
    prediction_df.index.name="index"

    #local_path = f"predictions_lr_{learning_rate}_ne_{n_estimators}_md_{max_depth}.csv"

    local_path = f"predictions_model_{model}.csv"

    prediction_df.to_csv(local_path)

    mlflow.log_artifact(local_path)


    # Check the metrics (real vs predicted)
    rmse_test, mae_test, r2_test = eval_metrics(yt, yp)

    # Log the metrics to MLFlow
    mlflow.log_metric("rmse", rmse_test)
    mlflow.log_metric("mae", mae_test)
    mlflow.log_metric("r2", r2_test)
    mlflow.log_artifact(local_path)

    # Create a figure with the pred vs actual and log it to mlflow
    plot_real_vs_prediction(yt, yp)


# Load the data

path = "../../data/processed/groupby/stations_final_2023.csv"
df = pd.read_csv(path)
df.dropna(inplace=True)
df.rename(columns=lambda x: x.replace('ctx_', 'ctx-'), inplace=True)

X = df[['month', 'day', 'hour', 'ctx-4', 'ctx-3', 'ctx-2', 'ctx-1']]
# X = df[['ctx-4', 'ctx-3', 'ctx-2', 'ctx-1']] 
y = df['percentage_docks_available']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


models = [
    LinearRegression(),
    LinearSVR(),
    DecisionTreeRegressor(),
    RandomForestRegressor(),
    GradientBoostingRegressor()

]

for model in models:
  train_model(X_train, y_train, X_test, y_test, model)