import os
import numpy as np
import subprocess
import mlflow
import matplotlib.pyplot as plt
import pandas as pd

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.base import BaseEstimator, TransformerMixin

def eval_metrics(actual, pred):
    """
    eval metrics
    """
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2

def plot_real_vs_prediction(actual, pred):
    """
    Figure comparing ground truth vs prediction"""
    # Create a new figure
    fig = plt.figure()

    # Plot the real values and the predictions as two different lines
    plt.scatter(actual, pred, s=0.1)
    #plt.plot(pred, label='predictions')
    plt.plot([0,1], [0,1], linestyle='--', color='red', alpha=.8)

    # Set names and activate the legend
    plt.ylabel('Prediction')
    plt.xlabel('Real')
    plt.title('Real vs Prediction')

def get_absolute_path(relative_path):
    # Convert relative path to absolute path
    absolute_path = os.path.abspath(relative_path)
    return absolute_path

# Define the command as a list of arguments
def configure_databricks(username, password):
    command = ["databricks", "configure", "--host", "https://community.cloud.databricks.com/"]
    # Use subprocess to execute the command with username and password
    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE)
    process.communicate(input=f"{username}\n{password}\n".encode())

# Set up MLflow tracking
def setup_mlflow(username):
    databricks_base_url = f'/Users/{username}'
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(f'{databricks_base_url}/bicing')
    
class ColumnsOneHotEncoder(BaseEstimator, TransformerMixin):
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
            self.columns_to_onehot = [col for col in self.columns_to_onehot if col in X.columns]
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