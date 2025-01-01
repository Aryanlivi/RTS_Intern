import requests
import os
from dotenv import load_dotenv
load_dotenv()




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

def post_forecast(data):
    baseURL=os.getenv('baseURL')
    origin_code=os.getenv('origin_code')
    parameter_code=os.getenv('parameter_code')
    url = f'{baseURL}/import'
    for item in data:
        item['origin_code']=origin_code
        item['parameter_code']=parameter_code
        
    token = os.getenv('token')
    headers = {
    'Content-Type': 'application/json',
    'Authorization': token
    }
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            print(f"Post Successful with response:{response.json()}")
            return response.json(), response.status_code, None
        else:
            return None, response.status_code, response.text
    except Exception as e:
        return None, None, str(e)
