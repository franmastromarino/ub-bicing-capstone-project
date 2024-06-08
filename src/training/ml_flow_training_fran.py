"""
Requirements: mlflow
pip install databricks-cli
TODO: Add requirements to requirements.txt file
"""
#Import libraries
from sklearn.pipeline import Pipeline
from sklearn.discriminant_analysis import StandardScaler
from training_utils import eval_metrics, plot_real_vs_prediction
# from lstm import create_lstm_model
# import mlflow
from mlflow_utils import configure_databricks, setup_mlflow

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer

from sklearn.linear_model import LinearRegression
from sklearn.svm import LinearSVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor

MODELS = [
    LinearRegression(),
    LinearSVR(),
    DecisionTreeRegressor(),
    RandomForestRegressor(),
    GradientBoostingRegressor(),
    #"simple_lstm"
]

FEATURES = [
  ['percentage_docks_available', 'weekday', 'month', 'hour', 'post_code', 'ctx-4', 'ctx-3', 'ctx-2', 'ctx-1', 'altitude', 'laboral_day'],
]

DATASET = pd.read_csv('./data/processed/groupby/stations_final_2024.csv')

class CustomColumnsOneHotEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, columns_to_onehot=None, sparse_output=False):
        """
        Custom transformer that applies one-hot encoding to specified columns.
        
        Parameters:
        - columns_to_onehot: List of column names to encode.
        - sparse_output: If False, the output will be a dense numpy array.
        """
        self.columns_to_onehot = columns_to_onehot
        self.sparse_output = sparse_output
        self.one_hot_encoder = None

    def fit(self, X, y=None):
        """
        Fit the OneHotEncoder to the DataFrame using specified columns.
        
        Parameters:
        - X: DataFrame containing the data to fit.
        - y: Not used, present for compatibility with sklearn's fit method.
        
        Returns:
        - self: Returns the instance itself.
        """
        if self.columns_to_onehot is not None:
            self.one_hot_encoder = OneHotEncoder(sparse_output=self.sparse_output)
            self.one_hot_encoder.fit(X[self.columns_to_onehot])
        return self

    def transform(self, X):
        """
        Transform the DataFrame by applying one-hot encoding to specified columns and concatenating
        the result back to the original DataFrame.

        Parameters:
        - X: DataFrame containing the data to transform.

        Returns:
        - X_transformed: The transformed DataFrame with original data and new one-hot encoded columns.
        """
        if self.one_hot_encoder is not None and self.columns_to_onehot is not None:
            # Perform one-hot encoding on the specified columns
            one_hot_variables = self.one_hot_encoder.transform(X[self.columns_to_onehot])
            
            if self.sparse_output:
                # Convert sparse matrix to a DataFrame ensuring we maintain the index of X
                encoded_df = pd.DataFrame.sparse.from_spmatrix(one_hot_variables, index=X.index, columns=self.one_hot_encoder.get_feature_names_out())
            else:
                # Convert dense array to DataFrame ensuring we maintain the index of X
                encoded_df = pd.DataFrame(one_hot_variables, index=X.index, columns=self.one_hot_encoder.get_feature_names_out())
            
            # Concatenate the encoded DataFrame with the original DataFrame minus the encoded columns
            X_transformed = pd.concat([X.drop(self.columns_to_onehot, axis=1), encoded_df], axis=1)
        else:
            X_transformed = X
        return X_transformed

DATASET = DATASET[FEATURES[0]]

# Define target and features
y = DATASET['percentage_docks_available']
X = DATASET.drop('percentage_docks_available', axis=1)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
  
pipeline = Pipeline([
  ('onehot', CustomColumnsOneHotEncoder(columns_to_onehot=['weekday', 'month', 'hour', 'post_code'])),
  ('scaler', StandardScaler()),
  ('imputer', SimpleImputer(strategy='mean'))  # Impute any missing values
])

# Fit the pipeline to the training data
pipeline.fit(X_train)

# Transformar los conjuntos de datos
X_train_transformed = pipeline.transform(X_train)
X_test_transformed = pipeline.transform(X_test)

# Dictionary to store model metrics
model_metrics = []

# Train and evaluate each model
for model in MODELS:
    model_name = type(model).__name__
    print(f"Training {model_name}")
    model.fit(X_train_transformed, y_train)
    y_pred = model.predict(X_test_transformed)

    # Calculate evaluation metrics
    rmse, mae, r2 = eval_metrics(y_test, y_pred)
    model_metrics.append({
        'Model': model_name,
        'RMSE': rmse,
        'MAE': mae,
        'R2 Score': r2
    })

    print(f"RMSE: {rmse}, MAE: {mae}, R2: {r2}")

# Save the model metrics to a CSV file
metrics_df = pd.DataFrame(model_metrics)
metrics_df.to_csv('model_performance_metrics.csv', index=False)