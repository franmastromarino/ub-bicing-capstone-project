import requests

# API URL
url = "https://api.open-meteo.com/v1/elevation"

# Latitude and Longitude parameters
params = {
    "latitude": 41.3800126,
    "longitude": 2.1727183
}

# Making a GET request to the API
response = requests.get(url, params=params)

# Checking if the request was successful (status code 200)
if response.status_code == 200:
    # Getting JSON data from the response
    data = response.json()
    # Printing the response
    print("Elevation:", data['elevation'])
else:
    # If the request fails, print the status code
    print("Request failed with status code:", response.status_code)