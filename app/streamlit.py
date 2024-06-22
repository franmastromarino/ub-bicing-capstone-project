import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from streamlit_read import streamlit_read

# Function to be called when the button is clicked
def predict(selected_station_id, selected_datetime):
    prediction = streamlit_read(selected_station_id, selected_datetime)
    st.session_state['prediction'] = prediction

# Load the dataset
path_to_csv_file_stations = './data/raw/Informacio_Estacions_Bicing.csv'
df_stations = pd.read_csv(path_to_csv_file_stations, dtype={'station_id': 'Int64'})
df_stations = df_stations.sort_values('name')

# Page configuration
st.set_page_config(page_title="Bike Stations (Jan - Mar)", page_icon=":bike:", layout="wide")

# Title
st.title(":bike: Forecasts for the Barcelona bike-share system from Jan to Mar 2024")

# Create columns for selectors
col1, col2, col3 = st.columns(3)

# Station selector
with col1:
    st.subheader("Select a station")
    station_names = ["All stations"] + list(df_stations['name'])
    selected_station_name = st.selectbox("Select station name", station_names, key='selected_station_name')

# Date selector
with col2:
    st.subheader("Select a date")
    selected_date = st.date_input("Date", value=pd.to_datetime("2024-01-01"), min_value=pd.to_datetime("2024-01-01"), max_value=pd.to_datetime("2024-03-31"), key='selected_date')

# Time selector
with col3:
    st.subheader("Select a time")
    hours = [f"{hour}:00" for hour in range(24)]
    selected_time = st.selectbox("Time", hours, key='selected_time')

selected_datetime = pd.to_datetime(f"{selected_date} {selected_time}").strftime("%Y-%m-%d %H")

# Execute prediction when station, date, or time changes
if selected_station_name != "All stations":
    selected_station_id = df_stations[df_stations['name'] == selected_station_name]['station_id'].values[0]
    predict(selected_station_id, selected_datetime)

# Filter the data based on the selected station
if selected_station_name == "All stations":
    selected_station_data = df_stations
    st.write("All stations are displayed on the map.")
    
    # Display an empty table with specified column names
    st.subheader("Station Information")
    empty_df = pd.DataFrame(columns=['name', 'address', 'post_code', 'capacity', 'prediction'])
    st.table(empty_df)
else:
    selected_station_id = df_stations[df_stations['name'] == selected_station_name]['station_id'].values[0]
    selected_station_data = df_stations[df_stations['station_id'] == selected_station_id]

    # Display selected station information in a table
    st.subheader("Station Information")
    station_info = selected_station_data.iloc[0][['name', 'address', 'post_code', 'capacity']]
    prediction = st.session_state.get('prediction', 'N/A')
    station_info['prediction'] = prediction
    station_info_df = pd.DataFrame(station_info).T
    st.table(station_info_df)

# Create the base map
m = folium.Map(location=[df_stations['lat'].mean(), df_stations['lon'].mean()], zoom_start=13, control_scale=True)

# Add marker cluster
marker_cluster = MarkerCluster().add_to(m)

# Add markers to the cluster
for _, row in selected_station_data.iterrows():
    popup_info = f"""
    <strong>Name:</strong> {row['name']}<br>
    <strong>Address:</strong> {row['address']}<br>
    <strong>Post Code:</strong> {row['post_code']}<br>
    <strong>Capacity:</strong> {row['capacity']}<br>
    <strong>Prediction:</strong> {st.session_state.get('prediction', 'N/A')}
    """
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=popup_info,
        icon=folium.Icon(icon='bicycle', prefix='fa')
    ).add_to(marker_cluster)

# Display the map in Streamlit
st_folium(m, width=None, height=500)
