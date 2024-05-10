import pandas as pd
import dask.dataframe as dd

# Define paths
path_to_csv_folder = './data/raw/historical/*.csv'
path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'
path_to_final_csv = './data/processed/groupby/stations_final.csv'

def dataset_load():
    # Define data types and columns to drop
    dtype = {
        'station_id': 'Int64',  # Assuming station_id has no non-integer values
        'num_bikes_available': 'Int64',
        'num_bikes_available_types.mechanical': 'Int64',
        'num_bikes_available_types.ebike': 'Int64',
        'num_docks_available': 'Int64',
        'last_reported': 'Int64',  # If this column has no non-integer values, else consider 'object'
        'is_charging_station': 'boolean',  # Will convert to boolean later
        'status': 'category',
        'is_installed': 'boolean',  # Will convert to boolean later
        'is_renting': 'boolean',  # Will convert to boolean later
        'is_returning': 'boolean',  # Will convert to boolean later
        'traffic': 'object',  # Assuming this is categorical
        'last_updated': 'Int64',  # If this column has no non-integer values, else consider 'object'
        'ttl': 'Int64'  # Using Pandas' nullable integer
    }
    return dd.read_csv(path_to_csv_folder, na_values=['NAN'], dtype=dtype)

def dataset_preprocess(ddf):

    def extract_month_day(df):
        df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s')
        df['month'] = df['last_reported'].dt.month
        df['day'] = df['last_reported'].dt.day
        df['hour'] = df['last_reported'].dt.hour
        return df
    
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
    ddf = ddf.drop(drop_columns, axis=1)
    ddf = ddf.dropna(subset=['last_reported'])
    dff = ddf.map_partitions(extract_month_day)
    return dff.groupby(['station_id', 'month', 'day', 'hour']).agg({'num_docks_available': 'mean'}).reset_index()

def dataset_merge(ddf):
    df_stations = dd.read_csv(path_to_csv_file_stations, dtype={'station_id': 'Int64'})
    return dd.merge(ddf, df_stations[['station_id', 'capacity']], on='station_id', how='inner')

def dataset_add_percentage_docks_available(ddf):
    
    def calculate_percentage_docks_available(df):
        df['percentage_docks_available'] = (df['num_docks_available'] / df['capacity'])
        return df.drop(['num_docks_available', 'capacity'], axis=1)
    
    return ddf.map_partitions(calculate_percentage_docks_available)

def dataset_add_ctx(df):
    # Define a custom function to apply to each group
    def apply_shifts(group):
        group = group.sort_values(by=['month', 'day', 'hour'])
        return group.assign(
            ctx_4=group['percentage_docks_available'].shift(4),
            ctx_3=group['percentage_docks_available'].shift(3),
            ctx_2=group['percentage_docks_available'].shift(2),
            ctx_1=group['percentage_docks_available'].shift(1)
        )
    
    # Group by 'station_id', then apply the shifting function to each group
    return df.map_partitions(
        lambda partition: partition.groupby('station_id').apply(apply_shifts)
    )

def dataset_rearrange(df):
    df = df.map_partitions(lambda x: x.iloc[5::5])
    cols = df.columns.tolist()
    cols.remove('percentage_docks_available')
    cols.append('percentage_docks_available')
    return df[cols]

def dataset_order_by_station_id_date(df):
    return df.sort_values(by=['station_id', 'month', 'day', 'hour'])

def dataset_save_to_csv(df):
    df.compute().reset_index(drop=True).to_csv(path_to_final_csv, index=True, index_label='index')

# Process workflow
ddf = dataset_load()
ddf = dataset_preprocess(ddf)
ddf = dataset_merge(ddf)
ddf = dataset_add_percentage_docks_available(ddf)
ddf = dataset_order_by_station_id_date(ddf)  # Reorder data first
ddf = dataset_add_ctx(ddf)  # Apply context columns on the correctly ordered data
ddf = dataset_rearrange(ddf)
dataset_save_to_csv(ddf)  # Save the final DataFram