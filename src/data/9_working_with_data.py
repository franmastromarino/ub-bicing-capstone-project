import pandas as pd
import dask.dataframe as dd

# Define paths
path_to_csv_folder = './data/raw/historical/*.csv'
path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'
path_to_final_csv = './data/processed/groupby/stations_final.csv'

# Define data types and columns to drop
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
drop_columns = [
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

def load_and_preprocess_data():
    ddf = dd.read_csv(path_to_csv_folder, na_values=['NAN'], dtype=dtype)
    ddf = ddf.dropna(how='any').reset_index().drop(drop_columns, axis=1)
    ddf['num_docks_available'] = ddf['num_docks_available'].astype('Int64')
    ddf['last_reported'] = ddf['last_reported'].astype('Float64')
    return ddf.map_partitions(extract_month_day)

def extract_month_day(df):
    df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s')
    df['month'] = df['last_reported'].dt.strftime('%m')
    df['day'] = df['last_reported'].dt.strftime('%d')
    df['hour'] = df['last_reported'].dt.strftime('%H')
    return df

def calculate_percentage_docks_available(df):
    df['percentage_docks_available'] = (df['num_docks_available'] / df['capacity'])
    return df.drop(['num_docks_available', 'capacity'], axis=1)

def group_and_aggregate(ddf):
    ddf = ddf.map_partitions(extract_month_day)
    ddf = ddf.drop(['last_reported'], axis=1)
    return ddf.groupby(['station_id', 'month', 'day', 'hour']).agg({'num_docks_available': 'mean'}).reset_index()

def merge_with_station_info(ddf):
    ddf['station_id'] = ddf['station_id'].astype(int)  # Ensure station_id is integer for the merge
    df_stations = dd.read_csv(path_to_csv_file_stations)
    df_stations['station_id'] = df_stations['station_id'].astype(int)
    merged_df = dd.merge(ddf, df_stations[['station_id', 'capacity']], on='station_id', how='inner')
    return merged_df

def add_percentage_docks_available(ddf):
    ddf = ddf.map_partitions(calculate_percentage_docks_available)
    return ddf

# def add_context_columns(df):
#     df['ctx-4'] = df['percentage_docks_available'].shift(4)
#     df['ctx-3'] = df['percentage_docks_available'].shift(3)
#     df['ctx-2'] = df['percentage_docks_available'].shift(2)
#     df['ctx-1'] = df['percentage_docks_available'].shift(1)
#     return df

def add_context_columns(df):
    # Aplicar map_overlap para manejar correctamente los contextos cuando se necesita acceso a filas de particiones adyacentes
    df = df.map_overlap(
        lambda df: df.assign(
            ctx_4=df['percentage_docks_available'].shift(4),
            ctx_3=df['percentage_docks_available'].shift(3),
            ctx_2=df['percentage_docks_available'].shift(2),
            ctx_1=df['percentage_docks_available'].shift(1)
        ),
        before=4,  # Necesitamos 4 filas anteriores en cada partición
        after=0    # No necesitamos filas posteriores
    )
    return df

def downsample_and_rearrange(df):
    df = df.map_partitions(lambda x: x.iloc[5::5])
    cols = df.columns.tolist()
    cols.remove('percentage_docks_available')
    cols.append('percentage_docks_available')
    return df[cols]

def save_to_csv(df):
    df.compute().to_csv(path_to_final_csv, index=False)

# Process workflow
ddf = load_and_preprocess_data()
ddf = group_and_aggregate(ddf)
ddf = merge_with_station_info(ddf)
ddf = add_percentage_docks_available(ddf)
ddf = add_context_columns(ddf)
ddf = downsample_and_rearrange(ddf)
save_to_csv(ddf)