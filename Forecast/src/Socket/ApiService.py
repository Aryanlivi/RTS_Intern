import requests
import os
from dotenv import load_dotenv
load_dotenv()


class APIService:
    def __init__(self):
        self.base_url = os.getenv('baseURL')
        self.origin_code = os.getenv('origin_code')
        self.parameter_code = os.getenv('parameter_code')
        self.token = os.getenv('token')
        
    def post_forecast(self,data):
        url = f'{self.base_url}/import'
        for item in data:
            item['origin_code']=self.origin_code
            item['parameter_code']=self.parameter_code
            
        headers = {
        'Content-Type': 'application/json',
        'Authorization': self.token
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
