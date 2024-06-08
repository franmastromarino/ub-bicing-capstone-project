import pandas as pd
import dask.dataframe as dd

# Define paths
PATH_TO_CSV_HISTORY = './data/other/metadata_sample_submission_2024.csv'
PATH_TO_CSV_SAVE = './data/processed/groupby/metadata_sample_submission_2024_features.csv'
    
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
    return dd.read_csv(PATH_TO_CSV_HISTORY, dtype=dtype)


def dataset_preprocess(dataset):
    # Combine 'year', 'month', 'day', and 'hour' into a datetime
    # Assuming year is fixed at 2024 for all entries
    datetime = dd.to_datetime(dataset['month'].astype(str) + '-' + 
                                     dataset['day'].astype(str) + '-' + 
                                     '2024' + ' ' + 
                                     dataset['hour'].astype(str) + ':00',
                                     format='%m-%d-%Y %H:%M')
    
    # Determine if it is a laboral (work) day
    dataset['laboral_day'] = datetime.dt.weekday < 5
    
    # Get the name of the weekday
    dataset['weekday'] = datetime.dt.day_name()

    return dataset

def dataset_merge(dataset):
    path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'
    df_stations = dd.read_csv(path_to_csv_file_stations, dtype={'station_id': 'Int64'})
    return dd.merge(dataset, df_stations[['station_id', 'altitude', 'post_code']], on='station_id', how='inner')

def dataset_save_to_csv(dataset):
    # Ensure the directory exists before saving
    import os
    directory = os.path.dirname(PATH_TO_CSV_SAVE)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Compute the Dask DataFrame, converting it to a pandas DataFrame
    dataset = dataset.compute()  # Be careful with large datasets

    # Save to CSV without an additional index column
    dataset.to_csv(PATH_TO_CSV_SAVE, index=False)  # index=False prevents adding a new index column
    
# Process workflow
dataset = dataset_load()
dataset = dataset_preprocess(dataset)
dataset = dataset_merge(dataset)
dataset_save_to_csv(dataset)