import dask.dataframe as dd

df = dd.read_csv('../data/raw/historial/*.csv')

df.head()