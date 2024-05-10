import pandas as pd
import dask.dataframe as dd

path_to_csv_file_stations_historical = './data/processed/groupby/stations_v6.csv'

# Load dataframe
df_historical = dd.read_csv(path_to_csv_file_stations_historical)

# Create context columns
df_historical['ctx-4'] = df_historical['percentage_docks_available'].shift(4)
df_historical['ctx-3'] = df_historical['percentage_docks_available'].shift(3)
df_historical['ctx-2'] = df_historical['percentage_docks_available'].shift(2)
df_historical['ctx-1'] = df_historical['percentage_docks_available'].shift(1)


# If you need to save the result to CSV
df_historical.compute().to_csv('./data/processed/groupby/stations_v7.csv', index=False)