import pandas as pd

import dask.dataframe as dd

def detect_nulls(df):
    null_count = df.isnull().sum().compute()
    return null_count

def detect_outliers(df, columns):
    # Precompute necessary quantiles to reduce computation within the loop
    quantiles = df[columns].quantile([0.25, 0.75]).compute()

    outliers_list = []
    for column in columns:
        Q1 = quantiles.loc[0.25, column]
        Q3 = quantiles.loc[0.75, column]
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Filtering outliers
        condition = (df[column] < lower_bound) | (df[column] > upper_bound)
        outliers = df[condition].compute()  # Compute to bring data into memory

        # Appending each outlier as a dictionary to the list
        for index, row in outliers.iterrows():
            outliers_list.append({
                'index': index,
                'column': column,
                'value': row[column]
            })

    return outliers_list

# Cargar la información sobre las estaciones
stations_df = pd.read_csv("../data/raw/Informacio_Estacions_Bicing.csv")
# Convertir station_id a set para búsquedas más rápidas
valid_station_ids = set(stations_df['station_id'])
stations_df.set_index('station_id', inplace=True)

def detect_anomalies(df):
    # DataFrame to record anomalies
    anomalies = []

    if isinstance(df, dd.DataFrame):
        df = df.compute()
    
    # Check for valid station_id
    invalid_stations = df[~df['station_id'].isin(valid_station_ids)].copy()
    if not invalid_stations.empty:
        invalid_stations['reason'] = 'Invalid station_id'
        anomalies.append(invalid_stations)

    # Ensure 'is_charging_station' column exists
    if 'is_charging_station' not in df.columns:
        df['is_charging_station'] = False  # Set to appropriate default value if missing

    # Join with station info to get capacity and check capacity consistencies
    df = df.join(stations_df[['capacity']], on='station_id', how='left')
    df['total_bikes_docks'] = df['num_bikes_available'] + df['num_docks_available']
    df['is_over_capacity'] = df['total_bikes_docks'] > df['capacity']
    df['is_under_capacity'] = df['total_bikes_docks'] < df['capacity']
    df['is_ebike_error'] = (df['is_charging_station'] == False) & (df['num_bikes_available_types.ebike'] > 0)
    
    # Define a dictionary for reason determination based on conditions
    reason_dict = {
        'is_over_capacity': 'Over capacity',
        'is_under_capacity': 'Under capacity',
        'is_ebike_error': 'Ebike without charging station'
    }
    
    # Apply the dictionary to set the 'reason' based on multiple conditions
    for condition, reason in reason_dict.items():
        condition_issues = df[df[condition]].copy()
        if not condition_issues.empty:
            condition_issues['reason'] = reason
            anomalies.append(condition_issues)
    
    return anomalies
