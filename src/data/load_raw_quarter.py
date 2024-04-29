import os
import glob
import dask.dataframe as dd
from tqdm import tqdm

dtype = {
    'station_id': 'float64',  # Assuming station_id has no non-integer values
    'num_bikes_available': 'float64',
    'num_bikes_available_types.mechanical': 'float64',
    'num_bikes_available_types.ebike': 'float64',
    'num_docks_available': 'float64',
    'last_reported': 'float64',  # If this column has no non-integer values, else consider 'object'
    'is_charging_station': 'object',  # Will convert to boolean later
    'status': 'object',
    'is_installed': 'object',  # Will convert to boolean later
    'is_renting': 'object',  # Will convert to boolean later
    'is_returning': 'object',  # Will convert to boolean later
    'traffic': 'object',  # Assuming this is categorical
    'last_updated': 'float64',  # If this column has no non-integer values, else consider 'object'
    'ttl': 'float64'  # Using Pandas' nullable integer
}

path_to_csv_folder = '../data/raw/historical/'

def load_raw_quarter():
    # List of month names in Catalan
    i2m = list(zip(range(1, 13), ['Gener', 'Febrer', 'Marc']))
    files = {}
    # Loop through each year and month to download and extract data
    for year in [2023, 2022, 2021, 2020]:
        for month, month_name in i2m:
            file_name = f'{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.csv'
            file_path = os.path.join(path_to_csv_folder, file_name)
            files[os.path.basename(file_path)] = dd.read_csv(file_path, dtype=dtype)
    return files
