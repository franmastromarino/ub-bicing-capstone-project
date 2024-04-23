import dask.dataframe as dd

# Especifica el patrón de comodín para los archivos CSV
path_to_csv_folder = './data/raw/historical/*.csv'

# Cargar los archivos CSV en un DataFrame de Dask
df = dd.read_csv(path_to_csv_folder, assume_missing=True)

# Visualizar el DataFrame
print(df.head())