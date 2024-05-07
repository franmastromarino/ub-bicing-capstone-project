import pandas as pd
import dask.dataframe as dd

path_to_csv_file_stations_historical = './data/processed/groupby/stations_v5.csv'
path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'

# Function to calculate the percentage of docks available and drop unnecessary columns
def calculate_percentage_docks_available(df):
    df['percentage_docks_available'] = (df['num_docks_available'] / df['capacity']) * 100
    # Drop the columns as they are no longer needed
    df = df.drop(['num_docks_available', 'capacity'], axis=1)
    return df

# Load dataframe
df_historical = dd.read_csv(path_to_csv_file_stations_historical)

# Order dataframe by 'station_id' 'month' 'day' 'hour'
df_sorted = df_historical.sort_values(by=['station_id', 'month', 'day', 'hour'])

# If you need to save the result to CSV
df_sorted.compute().to_csv('./data/processed/groupby/stations_v6.csv', index=False)