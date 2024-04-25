import os
import sys
import glob
import pandas as pd

from tqdm import tqdm
from tabulate import tabulate


path_to_csv_folder = "./data/raw/historical/*.csv"

using_columns = [
    'station_id', 
    'num_bikes_available', 
    'num_docks_available', 
    'last_reported', 
    'is_charging_station', 
    'status', 
    'is_installed', 
    'is_renting', 
    'is_returning',
    'last_updated'
]

dtype = {
    'station_id'            : 'object',
    'num_bikes_available'   : 'object',
    'num_docks_available'   : 'object',
    'last_reported'         : 'object',
    'is_charging_station'   : 'object',
    'status'                : 'object',
    'is_installed'          : 'object',
    'is_renting'            : 'object',
    'is_returning'          : 'object',
    'last_updated'          : 'object',
}

list_of_files = sorted(glob.glob(path_to_csv_folder))
data = [[file] for file in list_of_files]

print()
print(tabulate(data, headers=['List of Files'], tablefmt='grid'))
print()

print(" Load List of Dataframes ".center(80, "="))
list_of_dataframes = {}
for index, file in enumerate(list_of_files):
    key = os.path.basename(file)
    print(">> " + file)
    #list_of_dataframes[key] = pd.read_csv(file, usecols=using_columns, dtype=dtype, low_memory=False)
    list_of_dataframes[key] = pd.read_csv(file, usecols=using_columns, dtype=dtype)
    list_of_dataframes[key].info()

## This files has problems with mixed types
## ./data/raw/historical/2023_08_Agost_BicingNou_ESTACIONS.csv
## ./data/raw/historical/2023_09_Setembre_BicingNou_ESTACIONS.csv