import requests

def get_public_holidays(countie : str, year : int):

  url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/ES"

  response = requests.get(url)

  if response.status_code == 200:

    data = response.json()

    public_holidays_ES = [evento for evento in data if evento.get('counties') is None]
    public_holidays_CT = [evento for evento in data if evento.get('counties') is not None and 'ES-CT' in evento['counties']]

    return (public_holidays_ES, public_holidays_CT)

if __name__ == "__main__":
    ## Plaza Real BCN
    get_public_holidays("ES-CT", 2024)