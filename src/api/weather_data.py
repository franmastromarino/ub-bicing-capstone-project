import requests
import pandas as pd
import requests_cache
import openmeteo_requests

from retry_requests import retry

def get_weather( latitude : float, longitude: float, start_date : str , end_date : str) -> pd.DataFrame:

  cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
  retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
  openmeteo = openmeteo_requests.Client(session = retry_session)

  url = "https://archive-api.open-meteo.com/v1/archive"
  
  params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "hourly": "temperature_2m"
  }

  responses = openmeteo.weather_api(url, params=params)

  response = responses[0]
  # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
  # print(f"Elevation {response.Elevation()} m asl")
  # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
  # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

  hourly = response.Hourly()
  hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

  hourly_data = {"date": pd.date_range(
    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = hourly.Interval()),
    inclusive = "left"
  )}
  hourly_data["temperature_2m"] = hourly_temperature_2m

  hourly_dataframe = pd.DataFrame(data = hourly_data)

  return hourly_dataframe

if __name__ == "__main__":
    date_2024_01_01 = "2024-04-01"
    date_2024_01_30 = "2024-04-02"
    lat = 41.3800126
    lon = 2.1727183

    data = get_weather(lat, lon, date_2024_01_01, date_2024_01_30)
    print(data)