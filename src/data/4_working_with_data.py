import pandas as pd
import dask.dataframe as dd

path_to_csv_folder = './data/raw/historical/*.csv'

def extract_month_day(df):
    df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s')
    df['month'] = df['last_reported'].dt.strftime('%m')
    df['day'] = df['last_reported'].dt.strftime('%d')
    df['hour'] = df['last_reported'].dt.strftime('%H')
    return df

dtype = {
    'station_id': 'object', 
    'num_bikes_available': 'object',
    'num_bikes_available_types.mechanical': 'object',
    'num_bikes_available_types.ebike': 'object',
    'num_docks_available': 'object',
    'last_reported': 'object',  
    'is_charging_station': 'object',  
    'status': 'object',
    'is_installed': 'object',
    'is_renting': 'object',  
    'is_returning': 'object', 
    'traffic': 'object',  
    'last_updated': 'object',  
    'ttl': 'object'  
}

drop_columns    = [
   'num_bikes_available', 
   'num_bikes_available_types.mechanical',
   'num_bikes_available_types.ebike',
   'is_installed',
   'is_renting',
   'is_returning',
   'is_charging_station',
   'status',
   'last_updated',
   'ttl',
]

ddf = dd.read_csv(path_to_csv_folder, na_values = ['NAN'], dtype = dtype).dropna(how='any').reset_index().drop(drop_columns, axis=1)
ddf['num_docks_available'] = ddf['num_docks_available'].astype('Int64')
ddf['last_reported'] = ddf['last_reported'].astype('Float64')
ddf = ddf.map_partitions(extract_month_day)
ddf = ddf.drop(['last_reported'], axis=1)

new_ddf = ddf.groupby(['station_id', 'month', 'day' ,'hour']).agg({'num_docks_available': 'mean'}).reset_index().compute()
new_ddf.to_csv('./data/processed/groupby/stations.csv', index=False)

print(new_ddf.head())