import folium
import os.path
import pandas as pd
from folium.plugins import HeatMap

path = './data/processed/stations.csv'

if os.path.isfile(path) == False : raise Exception('File not fould')

df_stations = pd.read_csv(path)

lat_center = df_stations['lat'].mean()
lon_center = df_stations['lon'].mean()

map = folium.Map(location=[lat_center, lon_center], zoom_start=12)

for i, row in df_stations.iterrows():
    popup_text = f"{row['address']}<br>Latitud: {row['lat']}<br>Longitud: {row['lon']}"
    folium.Marker([row['lat'], row['lon']], popup=popup_text).add_to(map)

map.save('./data/processed/maps/stations_map.html')

heat_map = folium.Map(location=[lat_center, lon_center], zoom_start=12)
heat_map.add_child(HeatMap(data=df_stations[['lat', 'lon', 'altitude']], radius=15))


heat_map.save('./data/processed/maps/stations_heatmap.html')


