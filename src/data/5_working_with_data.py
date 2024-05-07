import pandas as pd
import dask.dataframe as dd

path_to_csv_file_stations_historical = './data/processed/groupby/stations_v4.csv'
path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'

# Function to calculate the percentage of docks available and drop unnecessary columns
def calculate_percentage_docks_available(df):
    df['percentage_docks_available'] = (df['num_docks_available'] / df['capacity']) * 100
    # Drop the columns as they are no longer needed
    df = df.drop(['num_docks_available', 'capacity'], axis=1)
    return df

# Load dataframes
df_historical = dd.read_csv(path_to_csv_file_stations_historical)
df_stations = dd.read_csv(path_to_csv_file_stations)

# Ensure the 'station_id' column is treated as integer in both dataframes for a proper merge
df_historical['station_id'] = df_historical['station_id'].astype(int)
df_stations['station_id'] = df_stations['station_id'].astype(int)

# Merge dataframes on 'station_id'
df_merged = dd.merge(df_historical, df_stations[['station_id', 'capacity']], on='station_id', how='inner')

# Calculate the percentage of docks available and drop the original columns
df_result = calculate_percentage_docks_available(df_merged)

# Optionally, you can compute the result to see the first few rows or export it
df_result = df_result.compute().reset_index()  # Only if you want to bring the entire dataset into memory, be cautious with large data
print(df_result.head())

# If you need to save the result to CSV
df_result.to_csv('./data/processed/groupby/stations_v5.csv', index=False)