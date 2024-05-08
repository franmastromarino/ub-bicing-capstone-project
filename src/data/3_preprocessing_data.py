import os
import sys
import glob
import pandas as pd
from tqdm import tqdm
import dask.dataframe as dd
from rich.table import Table
from rich.console import Console


path_to_csv_folder = "./data/raw/historical/*.csv"

path = './data/'
path_to_csv_folder = "./data/raw/historical/*.csv"
path_to_write_csv_folder = "./data/processed/historical"
path_to_stations_file = './data/raw/Informacio_Estacions_Bicing.csv'
path_to_processed_stations_file = './data/processed/stations.csv'

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

dtype_stations = {
    'station_id': 'Int64',
    'name' : 'object',
    'physical_configuration' : 'object',
    'lat' : 'object',
    'lon' : 'object',
    'name' : 'object',
}

def valid_dtype_per_column(df, column, dtype):
  data_type = tipo_de_dato_columna = df[column].dtype

  if dtype == 'Int64' and data_type == 'Int64' : return [True, data_type]
  if dtype == 'boolean' and data_type == 'boolean' : return [True, data_type]

  raise Exception(f'{column} {data_type}')

def print_table(list_of_dataframes):
  
  table = Table(title = "DataFrames")

  table.add_column("File")
  table.add_column("Column")
  table.add_column("Type")

  for filename in list_of_dataframes:
    columns = list_of_dataframes[filename].columns
    first = True
    for column in columns:
      column_dtype = str(list_of_dataframes[filename][column].dtype)
      if first:
        table.add_row(filename, column, column_dtype)
        first = False
      else:
        table.add_row("", column, column_dtype)

  console = Console()
  console.print(table)

def convert_timestamp_to_datetype(list_of_dataframes):

  for filename in tqdm(list_of_dataframes):
    list_of_dataframes[filename]['last_reported'] = dd.to_datetime(list_of_dataframes[filename]['last_reported'], unit='s')
    list_of_dataframes[filename]['last_updated'] = dd.to_datetime(list_of_dataframes[filename]['last_updated'], unit='s')
  
  return list_of_dataframes

def drop_column(list_of_dataframes, drop_columns):

  for filename in tqdm(list_of_dataframes):
    columns_to_drop = [col for col in drop_columns if col in list_of_dataframes[filename].columns]
    list_of_dataframes[filename] = list_of_dataframes[filename].drop(columns=columns_to_drop)
  
  return list_of_dataframes

def merge_with_station_database(list_of_dataframes, stations_dataframe):
  for filename in tqdm(list_of_dataframes):
    list_of_dataframes[filename] = list_of_dataframes[filename].merge(stations_dataframe, on='station_id', how='left')
  
  return list_of_dataframes

def save_file(list_of_dataframes):
  files_in_directory = os.listdir(path_to_write_csv_folder)

  for filename in (list_of_dataframes):
    name_file = filename.rstrip('.csv')
    print(name_file)
    files_with_base_name = [archivo for archivo in files_in_directory if archivo.startswith(name_file)]
    if files_with_base_name : continue
    else : list_of_dataframes[filename].to_csv(f'{path_to_write_csv_folder}/{name_file}-*.csv', index=False, header=True)


list_of_files = sorted(glob.glob(path_to_csv_folder))

table = Table(title = "List of Files")
table.add_column("File")
for filename in list_of_files: table.add_row(filename)
console = Console()
console.print(table)

print('\n')
print(" READING STATION DATABASE ".center(100, "="))
df_stantions = dd.read_csv(path_to_processed_stations_file, dtype = dtype_stations)
print(df_stantions.head())
print('\n')

print(" READ EACH FILE AND CREATE A DASK DATAFRAME ".center(100, "="))
print(f"TOTAL OF FILE = {len(list_of_files)}")
list_of_dataframes = {}
progression_bar = tqdm(list_of_files)
for filename in progression_bar:
  csv_filename = os.path.basename(filename)
  list_of_dataframes[csv_filename] = dd.read_csv( path = filename, dtype=dtype, true_values = ['TRUE'], false_values = ['FALSE', 'NA'])
print('\n')

print(" VALIDATING DATA TYPE ".center(100, "="))
columns_and_types = [
    ('is_installed', 'boolean'),
    ('is_renting', 'boolean'),
    ('is_returning', 'boolean'),
    ('is_charging_station', 'boolean'),
]

for filename in list_of_dataframes:
    for column, dtype in columns_and_types:
        result = valid_dtype_per_column(list_of_dataframes[filename], column, dtype)

print('BOOLEANS OK')

columns_and_types = [
    ('station_id', 'Int64'),
    ('num_bikes_available', 'Int64'),
    ('num_bikes_available_types.mechanical', 'Int64'),
    ('num_bikes_available_types.ebike', 'Int64'),
    ('num_docks_available', 'Int64'),
    ('last_reported', 'Int64'),
    ('last_updated', 'Int64'),

]

for filename in list_of_dataframes:
    for column, dtype in columns_and_types:
        result = valid_dtype_per_column(list_of_dataframes[filename], column, dtype)

print('INTEGERS OK')
print('\n')
print_table(list_of_dataframes=list_of_dataframes)
print('\n')
print(" PARSING TIMESTAMP TO DATETYPE ".center(100, "="))
## TODO I dont know why I have to do this
list_of_dataframes['2023_08_Agost_BicingNou_ESTACIONS.csv'] = list_of_dataframes['2023_08_Agost_BicingNou_ESTACIONS.csv'].dropna(how='all')
list_of_dataframes = convert_timestamp_to_datetype(list_of_dataframes)
print(" DROPPING TTL, TRAFFIC AND V1 ".center(100, "="))
list_of_dataframes = drop_column(list_of_dataframes, ['ttl', 'traffic', 'V1'])
print_table(list_of_dataframes=list_of_dataframes)
print('\n')
print(" MERGING WITH STATIONS ".center(100, "="))
list_of_dataframes = merge_with_station_database(list_of_dataframes, df_stantions)
print_table(list_of_dataframes=list_of_dataframes)
print(" SAVING NEW DATA ".center(100, "="))
save_file(list_of_dataframes)