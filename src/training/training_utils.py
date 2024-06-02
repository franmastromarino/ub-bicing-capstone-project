import matplotlib.pyplot as plt
import mlflow
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

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
    plt.plot(actual, pred)
    #plt.plot(pred, label='predictions')

    # Set names and activate the legend
    plt.ylabel('Prediction')
    plt.xlabel('Real')
    plt.title('Real vs Prediction')
    plt.legend(loc='lower right')

    # Save the figure to mlflow
    mlflow.log_figure(fig, 'my_figure.png')

    # Close the figure so it is not displayed in the output cell
    plt.close(fig)