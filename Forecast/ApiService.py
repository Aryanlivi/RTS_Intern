import requests
import os
from dotenv import load_dotenv
load_dotenv()

token = os.getenv('token')


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



def post_data_to_api(data):
    url = 'https://your-api-endpoint.com/endpoint'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer YOUR_API_TOKEN'
}
    try:
        # Make the POST request
        response = requests.post(url, json=data, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            return response.json(), response.status_code, None
        else:
            return None, response.status_code, response.text
    except Exception as e:
        return None, None, str(e)
