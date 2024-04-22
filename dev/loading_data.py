import dask.dataframe as dd


csv_files = "data/raw/historical/*.csv"

# load the data
df = dd.read_csv(csv_files)


df.head()
