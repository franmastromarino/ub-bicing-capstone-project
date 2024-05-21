import pandas as pd
import dask.dataframe as dd
import numpy as np

# Define paths
path_to_csv_folder = './data/raw/historical/2024/*.csv'
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

def dataset_preprocess(dataset):

    def extract_month_day(partition):
        partition['last_reported'] = pd.to_datetime(partition['last_reported'], unit='s')
        partition['month'] = partition['last_reported'].dt.month
        partition['day'] = partition['last_reported'].dt.day
        partition['hour'] = partition['last_reported'].dt.hour
        return partition
    
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
    dataset = dataset.drop(drop_columns, axis=1)
    dataset = dataset.dropna(subset=['last_reported'])
    dataset = dataset.map_partitions(extract_month_day)
    return dataset.groupby(['station_id', 'month', 'day', 'hour']).agg({'num_docks_available': 'mean'}).reset_index()

def dataset_merge(dataset):
    df_stations = dd.read_csv(path_to_csv_file_stations, dtype={'station_id': 'Int64'})
    return dd.merge(dataset, df_stations[['station_id', 'capacity']], on='station_id', how='inner')

def dataset_add_percentage_docks_available(dataset):
    
    def calculate_percentage_docks_available(partition):
        partition['percentage_docks_available'] = (partition['num_docks_available'] / partition['capacity']).round(16)
        return partition.drop(['num_docks_available', 'capacity'], axis=1)
    
    return dataset.map_partitions(calculate_percentage_docks_available)

def dataset_order_by_station_id_date(dataset):
    return dataset.sort_values(by=['station_id', 'month', 'day', 'hour'])

def dataset_delete_first_row_by_station(dataset):
    # Define a custom function to apply to each group
    def apply_delete(group):
        return group.iloc[1:]
    
    # Group by 'station_id', then apply the shifting function to each group
    return dataset.map_partitions(
        lambda partition: partition.groupby('station_id').apply(apply_delete)
    )


def dataset_add_ctx(dataset):
    # Define a custom function to apply to each group
    def apply_shifts(group):
        group = group.sort_values(by=['month', 'day', 'hour'])
        group = group.assign(
            ctx_4=group['percentage_docks_available'].shift(4),
            ctx_3=group['percentage_docks_available'].shift(3),
            ctx_2=group['percentage_docks_available'].shift(2),
            ctx_1=group['percentage_docks_available'].shift(1)
        )
        # Fill na values of every station first four hours -> https://snipboard.io/yX2Q4d.jpg
        group[['ctx_4', 'ctx_3', 'ctx_2', 'ctx_1']] = group[['ctx_4', 'ctx_3', 'ctx_2', 'ctx_1']].ffill().bfill()
        return group
    
    # Group by 'station_id', then apply the shifting function to each group
    return dataset.map_partitions(
        lambda partition: partition.groupby('station_id').apply(apply_shifts)
    )

def dataset_rearrange(dataset):
    cols = dataset.columns.tolist()
    cols.remove('percentage_docks_available')
    cols.append('percentage_docks_available')
    return dataset[cols]

def dataset_select_every_5_hours(dataset):

    # TODO: Select every 5 hours
    
    return dataset

def dataset_save_to_csv(dataset):
    dataset.compute().reset_index(drop=True).to_csv(path_to_final_csv, index=True, index_label='index')

def dataset_create_index(dataset):
    return dataset.reset_index(drop=True)

# Process workflow
dataset = dataset_load()
dataset = dataset_preprocess(dataset)
dataset = dataset_merge(dataset)
dataset = dataset_add_percentage_docks_available(dataset)
dataset = dataset_order_by_station_id_date(dataset)  # Reorder data first
dataset = dataset_add_ctx(dataset)  # Apply context columns on the correctly ordered data
dataset = dataset_create_index(dataset)
dataset = dataset_delete_first_row_by_station(dataset)
#dataset = dataset_rearrange(dataset)
# TODO: dataset = dataset_select_every_5_hours(dataset)
dataset_save_to_csv(dataset)  # Save the final DataFram