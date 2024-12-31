import requests

def fetch_water_level_data(id):
    url = f'http://localhost:3000/get-wl/{id}'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # Return the response in JSON format
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error occurred while fetching data: {e}")
        return None


# try:
#     water_level_data = fetch_water_level_data(SocketSiurenitarId)
#     print(water_level_data)
# except Exception as e:
#     print(e)

