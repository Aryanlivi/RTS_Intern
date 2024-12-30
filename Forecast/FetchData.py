import requests

def fetch_data(series_id, start, end):
    url = "http://localhost:3000/fetch-data"
    params = {"seriesId": series_id, "start": start, "end": end}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None
#GalchiSeriesId=7027
#SiurentarSeriesID=7009
#chumlingatSeriesID=6991
BudhiGandakiSeriesID=7045
data = fetch_data(BudhiGandakiSeriesID, "2024-12-14:00:00", "2024-12-16T00:00:00")
print(data)