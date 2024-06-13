from datetime import datetime
import pandas as pd


def streamlit_read(station_id, date, csv_results, csv_predictions):
    """
    station_id = 

    date = 'YYYY-MM-DD'

    csv_results: csv with 2024 data
    csv_predictions: csv with 2024 predictions
    """
    # Load the data
    df_results = pd.read_csv(csv_results)
    df_predictions = pd.read_csv(csv_predictions)

    date = datetime.strptime(date, '%Y-%m-%d')
    
    # Extract the month, day, and hour
    month, day, hour = date.month, date.day, date.hour
    
    # Filter the data
    df_results = df_results[(df_results['station_id'] == station_id) & (df_results['month'] == month) & (df_results['day'] == day) & (df_results['hour'] == hour)]

    return prediction
