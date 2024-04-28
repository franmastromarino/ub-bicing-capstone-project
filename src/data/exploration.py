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
    
    # DataFrame para registrar anomalías
    anomalies = []

    if isinstance(df, dd.DataFrame):
        df = df.compute()
        
    # Añadir validaciones
    # Validar station_id existentes
    invalid_stations = df[~df['station_id'].isin(valid_station_ids)].copy()
    if not invalid_stations.empty:
        invalid_stations['reason'] = 'Invalid station_id'
        anomalies.append(invalid_stations)

    # Asegurar que la columna 'is_charging_station' existe
    if 'is_charging_station' not in df.columns:
        df['is_charging_station'] = False  # O el valor por defecto apropiado

    # Validaciones de capacidad y consistencia
    df = df.join(stations_df[['capacity']], on='station_id', how='left')
    df['total_bikes_docks'] = df['num_bikes_available'] + df['num_docks_available']
    df['is_over_capacity'] = df['total_bikes_docks'] > df['capacity']
    df['is_ebike_error'] = (df['is_charging_station'] == False) & (df['num_bikes_available_types.ebike'] > 0)
    
    capacity_issues = df[df['is_over_capacity'] | df['is_ebike_error']].copy()
    if not capacity_issues.empty:
        capacity_issues['reason'] = capacity_issues.apply(lambda x: 'Over capacity' if x['is_over_capacity'] else 'Ebike without charging station', axis=1)
        anomalies.append(capacity_issues)
        
    return anomalies
