import pandas as pd
import dask.dataframe as dd

path_to_csv_file_stations_historical = './data/processed/groupby/stations_v7.csv'

# Load dataframe
df_historical = dd.read_csv(path_to_csv_file_stations_historical)

# Function to pick one row every 5 rows using pandas within each partition
def pick_every_fifth(df):
    return df.iloc[5::5]

# Apply the function to each partition
df_historical = df_historical.map_partitions(pick_every_fifth)

# Move 'percentage_docks_available' to the end of the DataFrame
# Getting list of columns without 'percentage_docks_available'
cols = df_historical.columns.tolist()
cols.remove('percentage_docks_available')
# Adding 'percentage_docks_available' at the end
cols.append('percentage_docks_available')
df_historical = df_historical[cols]

# Compute the result and save to CSV
df_historical.compute().to_csv('./data/processed/groupby/stations_v8.csv', index=False)