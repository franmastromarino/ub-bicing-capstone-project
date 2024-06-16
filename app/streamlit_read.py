from datetime import datetime
import pandas as pd

def streamlit_read(station_id, datetime_str, csv_input = 'data/processed/groupby/stations_final_2024.csv', csv_predictions = 'data/processed/groupby/pred_stations_final_2024.csv'):
    """
    station_id = id of the station

    datetime_str = 'YYYY-MM-DD HH'

    csv_input: csv with 2024 data
    csv_predictions: csv with 2024 predictions
    """
    # Load the data
    df_input = pd.read_csv(csv_input)
    df_predictions = pd.read_csv(csv_predictions)

    # Convert the datetime string to a datetime object
    date = datetime.strptime(datetime_str, '%Y-%m-%d %H')
    
    # Extract the month, day, and hour
    month, day, hour = date.month, date.day, date.hour
    
    # Filter the data
    df_filtered = df_input[(df_input['station_id'] == station_id) & 
                           (df_input['month'] == month) & 
                           (df_input['day'] == day) & 
                           (df_input['hour'] == hour)]

    # Get the prediction based on df_filtered['index']
    index = df_filtered['index'].values[0]
    prediction = df_predictions.loc[index, 'percentage_docks_available']

    return prediction


if __name__ == "__main__":
    # Example usage
    station_id = 1
    datetime_str = '2024-01-01 00'
    csv_input = 'data/processed/groupby/stations_final_2024.csv'
    csv_predictions = 'data/processed/groupby/pred_stations_final_2024.csv'

    prediction = streamlit_read(station_id, datetime_str, csv_input, csv_predictions)
    print(prediction)
