import pandas as pd  
import matplotlib.pyplot as plt 
import numpy as np
from decimal import Decimal
from Utils import  *
from Socket.ApiService import APIService
from ComputeForecast import compute_discharge_and_forecast
from dotenv import load_dotenv
import os 
# from DB_Service.Database import Database

load_dotenv()

#Distance
DISTANCE_GALCHI_SUIRENITAR=30000
DISTANCE_BUDHI_SUIRENITAR=18500




def standardize_river_data(data, id):
    try:
        if isinstance(data, list):
            if not data:  # Check if the list is empty
                return None 
            for item in data:
                if item.get('id') == int(id):
                    return item.get('waterLevel', 'Water level not available')
            return None 
        else:
            return 'Invalid data format received.'  

    except Exception as error:
        return f"Error: {error}"  

def compute_and_post(data):
    SocketGalchiId=os.getenv('SocketGalchiId')
    SocketBudhiId=os.getenv('SocketBudhiId')
    galchi_data=standardize_river_data(data=data,id=SocketGalchiId)
    budhi_data=standardize_river_data(data=data,id=SocketBudhiId)

    
    # galchi_data={'datetime': '2025-01-02T06:35:00+00:00', 'value': 366.036499023} //For Tests
    # budhi_data={'datetime': '2025-01-02T06:45:00+00:00', 'value': 333.405014648} //For Tests

    galchi_df=pd.DataFrame([galchi_data])
    budhi_df=pd.DataFrame([budhi_data])

    if not galchi_data or not budhi_data:      
        return
    final_output=compute_discharge_and_forecast(galchi_df,budhi_df)
    api_service=APIService()
    api_service.post_forecast(final_output)
    print("------Updated DF----------")
    
