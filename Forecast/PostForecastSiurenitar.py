import pandas as pd  
import matplotlib.pyplot as plt 
import numpy as np
from decimal import Decimal
from Utils import  *
from ApiService import fetch_water_level_data,post_forecast
import time
import json 

DISTANCE_GALCHI_SUIRENITAR=30000
DISTANCE_BUDHI_SUIRENITAR=18500

SocketSiurenitarId=450
SocketGalchiId=451
SocketBudhiId=452

def preprocess_data(df):   
    df['datetime']=pd.to_datetime(df['datetime'])
    df['value']=pd.to_numeric(df['value'],errors='coerce')
    df.set_index('datetime',inplace=True)
    # df=handle_outliers(df)
    # handle_null_values(df)
    return df

def create_shifted_df(df):
    shifted_data = [
    {'dateTime': (index + pd.DateOffset(hours=lag_hr,minutes=lag_min)), 'discharge': discharge}
    for index, lag_hr,lag_min, discharge in zip(df.index, df['time_lag_hr'],df['time_lag_min'], df['discharge'])
]
    return pd.DataFrame(shifted_data)




def full_task_pipeline(galchi_df,budhi_df):
    galchi_df=preprocess_data(galchi_df)
    budhi_df=preprocess_data(budhi_df)
    # suirenitar_df=preprocess_data(suirenitar_df)    
    galchi_df['discharge']=galchi_df.apply(lambda row:calculate_discharge_galchi(row['value']),axis=1)
    budhi_df['discharge']=budhi_df.apply(lambda row:calculate_discharge_budhi(row['value']),axis=1)
    # suirenitar_df['discharge']=suirenitar_df.apply(lambda row:calculate_discharge_suirenitar(row['value']),axis=1)
    galchi_df['mean_velocity']=galchi_df.apply(lambda row:calculate_mean_velocity_galchi(row['discharge']),axis=1)
    budhi_df['mean_velocity']=budhi_df.apply(lambda row:calculate_mean_velocity_buddhi(row['discharge']),axis=1)
    galchi_df[['time_lag_hr', 'time_lag_min']] = galchi_df.apply(
    lambda row: pd.Series(calcualte_time(DISTANCE_GALCHI_SUIRENITAR, row['mean_velocity'])), axis=1
    )
    budhi_df[['time_lag_hr', 'time_lag_min']] = budhi_df.apply(
        lambda row: pd.Series(calcualte_time(DISTANCE_BUDHI_SUIRENITAR, row['mean_velocity'])), axis=1
    )
    
    shifted_galchi_df=create_shifted_df(galchi_df)
    shifted_budhi_df=create_shifted_df(budhi_df)
    shifted_budhi_df['dateTime'] = pd.to_datetime(shifted_budhi_df['dateTime'])
    shifted_budhi_df.set_index('dateTime',inplace=True)

    shifted_galchi_df['dateTime'] = pd.to_datetime(shifted_galchi_df['dateTime'])
    shifted_galchi_df.set_index('dateTime',inplace=True)
    
    
    merged_df = pd.merge(
    shifted_galchi_df, shifted_budhi_df, 
    on='dateTime',      
    how='outer', 
    suffixes=('_galchi', '_budhi')
    )

    # Fill NaN with 0 and add the columns
    merged_df['discharge'] = merged_df['discharge_galchi'].fillna(0) + merged_df['discharge_budhi'].fillna(0)


    # Set 'discharge' to NaN if both columns are NaN
    for index, row in merged_df.iterrows():
        if pd.isna(row['discharge_galchi']) and pd.isna(row['discharge_budhi']):
            merged_df.at[index, 'discharge'] = np.nan
    merged_df.reset_index(inplace=True)    
    computed_suirenitar_df=merged_df[['dateTime','discharge']]
    print(merged_df)
    # Rename columns first
    computed_suirenitar_df = computed_suirenitar_df.rename(columns={'dateTime': 'time', 'discharge': 'value'})

    # Convert 'time' column to datetime
    computed_suirenitar_df['time'] = pd.to_datetime(computed_suirenitar_df['time'])

    # Convert to Nepali Time +5:45
    computed_suirenitar_df['time'] = computed_suirenitar_df['time'] + pd.Timedelta(hours=5, minutes=45)

    # Format the 'time' column to show only hour and minute
    computed_suirenitar_df['time'] = computed_suirenitar_df['time'].dt.strftime('%Y-%m-%dT%H:%M')
    
    try:
        output = computed_suirenitar_df.to_json(orient='records', date_format='iso')
        result = json.loads(output)
        return result
    except Exception as e:
        print(f"Error during JSON conversion: {e}")
        return None

    return json_result
            


def update_galchi_data(data, id):
    try:
        if isinstance(data, list):
            if not data:  # Check if the list is empty
                return None  # Return None for empty data
            for item in data:
                if item.get('id') == id:
                    return item.get('waterLevel', 'Water level not available')
            return None 
        else:
            return 'Invalid data format received.'  

    except Exception as error:
        return f"Error: {error}"  

    
def update_budhi_data(data, id):
    try:
        if isinstance(data, list):
            if not data:  # Check if the list is empty
                return None  # Return None for empty data
            for item in data:
                if item.get('id') == id:
                    return item.get('waterLevel', 'Water level not available')
            return None 
        else:
            return 'Invalid data format received.'  

    except Exception as error:
        return f"Error: {error}"   
def update_dataframe(data):
    galchi_data=update_galchi_data(data=data,id=SocketGalchiId)
    budhi_data=update_budhi_data(data=data,id=SocketBudhiId)
    
    galchi_df=pd.DataFrame([galchi_data])
    budhi_df=pd.DataFrame([budhi_data])
        
    if not galchi_data or not budhi_data:      
        return
    
    final_output=full_task_pipeline(galchi_df,budhi_df)
    post_forecast(final_output)
    
    print("------Updated DF----------")