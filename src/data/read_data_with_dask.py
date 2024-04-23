import os
import glob
import dask.dataframe as dd

from tqdm import tqdm

# Especifica el patrón de comodín para los archivos CSV
path_to_csv_folder = './data/raw/historical/*.csv'

dtype = {
    'station_id': 'Int64',  # Assuming station_id has no non-integer values
    'num_bikes_available': 'Int64',
    'num_bikes_available_types.mechanical': 'Int64',
    'num_bikes_available_types.ebike': 'Int64',
    'num_docks_available': 'Int64',
    'last_reported': 'Int64',  # If this column has no non-integer values, else consider 'object'
    'is_charging_station': 'object',  # Will convert to boolean later
    'status': 'object',
    'is_installed': 'object',  # Will convert to boolean later
    'is_renting': 'object',  # Will convert to boolean later
    'is_returning': 'object',  # Will convert to boolean later
    'traffic': 'object',  # Assuming this is categorical
    'last_updated': 'Int64',  # If this column has no non-integer values, else consider 'object'
    'ttl': 'Int64'  # Using Pandas' nullable integer
}

list_of_files = sorted(glob.glob(path_to_csv_folder))


files = {}
for file_path in tqdm(list_of_files):
  files[os.path.basename(file_path)] = dd.read_csv(file_path, dtype=dtype)


with open("src/data/read_data_with_dask_result.txt", "w") as f:
  
  for index, file_name in enumerate(files):

    print(file_name)
    
    try:
        f.write(f'START {file_name}'.center(100, "=") + "\n")
        
        null_count = files[file_name].isnull().sum().compute()

        f.write(str(null_count) + "\n")
        
        f.write(f'END {file_name}'.center(100, "=") + "\n")

    except Exception as e:
        
        f.write(file_name + "\n")
        
        f.write(str(e) + "\n")
        
        f.write(f'END WITH ERROR {file_name}'.center(100, "=") + "\n")