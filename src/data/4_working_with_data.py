import dask.dataframe as dd

dtype = {
    'station_id': 'Int64',  # Assuming station_id has no non-integer values
    'num_bikes_available': 'Int64',
    'num_bikes_available_types.mechanical': 'Int64',
    'num_bikes_available_types.ebike': 'Int64',
    'num_docks_available': 'Int64',
    'last_reported': 'string',  # If this column has no non-integer values, else consider 'object'
    'is_charging_station': 'boolean',  # Will convert to boolean later
    'status': 'category',
    'is_installed': 'boolean',  # Will convert to boolean later
    'is_renting': 'boolean',  # Will convert to boolean later
    'is_returning': 'boolean',  # Will convert to boolean later
    'last_updated': 'string',  # If this column has no non-integer values, else consider 'object'
}

folder = './data/processed/historical/*.csv'

##parse_dates = ['last_updated', 'last_reported']

## ddf = dd.read_csv(folder, dtype = dtype, parse_dates=parse_dates)
ddf = dd.read_csv(folder, dtype = dtype)

# ddf['last_reported'] = dd.to_datetime(ddf['last_reported'], unit='s')
# ddf['last_reported'] = dd.to_datetime(ddf['last_reported'], unit='s')

print("num_bikes_available")
print(ddf['num_bikes_available'].max().compute())
print(ddf['num_bikes_available'].min().compute())