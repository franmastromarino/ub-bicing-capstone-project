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

path_to_csv_folder = '../data/raw/historical/*.csv'

def load_raw():
    """
    Carga todos los archivos CSV en un directorio en un diccionario de DataFrames de Dask.
    
    Args:
    path_to_csv_folder (str): La ruta al directorio que contiene los archivos CSV.
    dtype (dict): Un diccionario que especifica los tipos de datos de las columnas de los CSV.
    
    Returns:
    dict: Un diccionario donde las claves son los nombres de los archivos y los valores son DataFrames de Dask.
    """
    # Construye el patrón de ruta completa para los archivos CSV
    list_of_files = sorted(glob.glob(path_to_csv_folder))

    files = {}
    for file_path in tqdm(list_of_files):
        files[os.path.basename(file_path)] = dd.read_csv(file_path, dtype=dtype)
            
    return files
