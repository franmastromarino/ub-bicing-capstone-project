import requests

"""
Retrieves the elevation at a given latitude and longitude.
"""
def get_elevation(lat : float, lon : float) -> float:

  url = "https://api.open-meteo.com/v1/elevation"

  # Latitude and Longitude parameters
  params = { "latitude": lat, "longitude": lon }

  # Making a GET request to the API
  response = requests.get(url, params=params)

  # Checking if the request was successful (status code 200)
  if response.status_code == 200:
      # Getting JSON data from the response
      data = response.json()
      # Returning the response
      return data['elevation'][0]
  else:
      # If the request fails, print the status code
      raise Exception(f"Request failed with status code: {response.status_code}")


if __name__ == "__main__":
    ## Plaza Real BCN
    get_elevation(41.3800126, 2.1727183)
    