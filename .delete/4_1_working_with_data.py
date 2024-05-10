import pandas as pd

path_to_csv_file = './data/processed/groupby/stations_v4.csv'

df = pd.read_csv(path_to_csv_file)

print(df.describe())
print(df.sort_values('num_docks_available', ascending=False).head(50))