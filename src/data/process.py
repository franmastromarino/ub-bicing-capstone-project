import pandas as pd
import dask.dataframe as dd

# Define paths
YEAR = 2024
PATH_TO_CSV_HISTORY = './data/raw/historical/' + str(YEAR) + '/*.csv'
PATH_TO_CSV_SAVE = './data/processed/groupby/stations_final_' + str(YEAR) + '.csv'
    
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
    return dd.read_csv(PATH_TO_CSV_HISTORY, na_values=['NAN'], dtype=dtype)

def dataset_preprocess(dataset):

    def extract_month_day(partition):
        datetime = pd.to_datetime(partition['last_reported'], unit='s')
        partition['laboral_day'] = datetime.dt.weekday < 5
        partition['weekday'] = datetime.dt.day_name()
        partition['month'] = datetime.dt.month
        partition['day'] = datetime.dt.day
        partition['hour'] = datetime.dt.hour
        partition['year'] = datetime.dt.year
        return partition
    
    def delete_row_out_of_date(partition):
        return partition[partition['year'] == YEAR]
    
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
    dataset = dataset.map_partitions(delete_row_out_of_date)
    return dataset.groupby(['station_id', 'month', 'day', 'hour']).agg({'weekday': 'first', 'laboral_day': 'first', 'num_docks_available': 'mean'}).reset_index()

def dataset_merge(dataset):
    path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'
    df_stations = dd.read_csv(path_to_csv_file_stations, dtype={'station_id': 'Int64'})
    return dd.merge(dataset, df_stations[['station_id', 'altitude', 'post_code', 'capacity']], on='station_id', how='inner')

def dataset_add_percentage_docks_available(dataset):
    
    def calculate_percentage_docks_available(partition):
        # Calculate the percentage of docks available and round to 16 decimal places and max 1
        partition['percentage_docks_available'] = (partition['num_docks_available'] / partition['capacity']).round(16).clip(0, 1)
        return partition.drop(['num_docks_available', 'capacity'], axis=1)
    
    return dataset.map_partitions(calculate_percentage_docks_available)

def dataset_order_by_station_id_date(dataset):
    return dataset.sort_values(by=['station_id', 'month', 'day', 'hour'])

def dataset_add_ctx(dataset):
    # Define a custom function to apply to each group
    def calculate_contexts(group):
        group = group.sort_values(by=['month', 'day', 'hour'])
        group = group.assign(
            ctx_4=group['percentage_docks_available'].shift(4),
            ctx_3=group['percentage_docks_available'].shift(3),
            ctx_2=group['percentage_docks_available'].shift(2),
            ctx_1=group['percentage_docks_available'].shift(1)
        )
        # Fill na values of every station first four hours
        group[['ctx_4', 'ctx_3', 'ctx_2', 'ctx_1']] = group[['ctx_4', 'ctx_3', 'ctx_2', 'ctx_1']].ffill().bfill()
         # Rename columns to use 'ctx-' prefix
        group = group.rename(columns={'ctx_4': 'ctx-4', 'ctx_3': 'ctx-3', 'ctx_2': 'ctx-2', 'ctx_1': 'ctx-1'})
        return group
    
    # Group by 'station_id', then apply the shifting function to each group
    return dataset.map_partitions(
        lambda partition: partition.groupby('station_id').apply(calculate_contexts)
    )

def dataset_rearrange(dataset):
    cols = dataset.columns.tolist()
    cols.remove('percentage_docks_available')
    cols.append('percentage_docks_available')
    return dataset[cols]

def dataset_select_every_5_hours(dataset):
    return dataset.compute().iloc[::5]

def dataset_save_to_csv(dataset):
    dataset.reset_index(drop=True).to_csv(PATH_TO_CSV_SAVE, index=True, index_label='index')

def dataset_create_index(dataset):
    return dataset.reset_index(drop=True)

# Process workflow
dataset = dataset_load()
dataset = dataset_preprocess(dataset)
dataset = dataset_merge(dataset)
dataset = dataset_add_percentage_docks_available(dataset)
dataset = dataset_order_by_station_id_date(dataset)
dataset = dataset_add_ctx(dataset)
dataset = dataset_create_index(dataset)
dataset = dataset_rearrange(dataset)
dataset = dataset_select_every_5_hours(dataset)
dataset_save_to_csv(dataset)