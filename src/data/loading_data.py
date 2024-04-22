import dask.dataframe as dd

# Specifying data types explicitly
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

df = dd.read_csv('data/raw/historical/*.csv', dtype=dtype, assume_missing=True)

# print(df.head())

## Sumarizar missing values
print(df.isnull().sum().compute())