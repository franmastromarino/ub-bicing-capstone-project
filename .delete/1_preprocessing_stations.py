import glob
import os
import pandas as pd
import dask.dataframe as dd

from tabulate import tabulate

stations_dtype = {
    'station_id': 'Int64',
    'name' : 'object',
    'physical_configuration' : 'object',
    'lat' : 'object',
    'lon' : 'object',
    'name' : 'object',
}

path_to_stations_file = 'data/raw/Informacio_Estacions_Bicing.csv'

if os.path.isfile(path_to_stations_file) == False : raise Exception("File Not Found")

df_stantions = pd.read_csv(path_to_stations_file)

unique_emements_from_stations = {}

for column in df_stantions.columns:
  unique_emements_from_stations[column] = df_stantions[column].unique().tolist()

data = []
data.append(['is_charging_station', unique_emements_from_stations['is_charging_station']])
data.append(['cross_street', unique_emements_from_stations['cross_street']])
data.append(['nearby_distance', unique_emements_from_stations['nearby_distance']])
data.append(['_ride_code_support', unique_emements_from_stations['_ride_code_support']])
data.append(['rental_uris', unique_emements_from_stations['rental_uris']])
data.append(['physical_configuration', unique_emements_from_stations['physical_configuration']])

print()
print(tabulate(data, headers=['Column', 'Categories'], tablefmt='grid'))
print()

## TODO Must We delete Name or Address Column?
are_equal = df_stantions['name'].equals(df_stantions['address'])
print(f'>> Name column is equal to Address: {are_equal}')
print()

df_stantions = df_stantions.drop(columns = [
    'is_charging_station',
    'cross_street',
    'nearby_distance',
    '_ride_code_support',
    'rental_uris',
    'physical_configuration'
])

df_stantions.to_csv('./data/processed/stations.csv', index=False)
