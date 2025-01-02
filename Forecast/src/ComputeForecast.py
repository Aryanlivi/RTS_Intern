import pandas as pd  
import matplotlib.pyplot as plt 
import numpy as np
from decimal import Decimal
from Utils import  *
import time
import json 
from dotenv import load_dotenv
import os 
from DB_Service.Database import Database


load_dotenv()

#Distance
DISTANCE_GALCHI_SUIRENITAR=30000
DISTANCE_BUDHI_SUIRENITAR=18500

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

def get_shifted_df(galchi_df,budhi_df):
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
    shifted_budhi_df['dateTime']=shifted_budhi_df['dateTime'].dt.floor('10T')
    shifted_budhi_df.set_index('dateTime',inplace=True)
    
    shifted_galchi_df['dateTime'] = pd.to_datetime(shifted_galchi_df['dateTime'])
    shifted_galchi_df['dateTime']=shifted_galchi_df['dateTime'].dt.floor('10T')
    shifted_galchi_df.set_index('dateTime',inplace=True)

    return shifted_galchi_df,shifted_budhi_df


def get_merged_df(shifted_galchi_df,shifted_budhi_df):
    
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
    
    #DISCRETIZE THE DATE TIME TO 10 MIN INTERVAL BY ROINDING DOWN:
    merged_df['dateTime']=merged_df['dateTime'].dt.floor('10T')
    
    return merged_df



def compute_discharge_and_forecast(galchi_df,budhi_df):
    shifted_galchi_df,shifted_budhi_df=get_shifted_df(galchi_df,budhi_df)
    
    #CONNECT DATABASE
    db=Database()
    db.connect()
    GALCHI_TABLE=os.getenv('galchi_table')
    BUDHI_TABLE=os.getenv('budhi_table')
    SIURENITAR_TABLE=os.getenv('siurenitar_table')
    
    shifted_galchi_df.reset_index(inplace=True)
    shifted_budhi_df.reset_index(inplace=True)



    db.insert_df(shifted_galchi_df,GALCHI_TABLE)
    db.insert_df(shifted_budhi_df,BUDHI_TABLE)

    # merged_df=get_merged_df(shifted_galchi_df,shifted_budhi_df)
    # print(merged_df)
    return None 
    
    
    # computed_suirenitar_df=merged_df[['dateTime','discharge']]
    
    # # Rename columns first
    # computed_suirenitar_df = computed_suirenitar_df.rename(columns={'dateTime': 'time', 'discharge': 'value'})

    # # Convert 'time' column to datetime
    # computed_suirenitar_df['time'] = pd.to_datetime(computed_suirenitar_df['time'])

    # # Convert to Nepali Time +5:45
    # computed_suirenitar_df['time'] = computed_suirenitar_df['time'] + pd.Timedelta(hours=5, minutes=45)

    # # Format the 'time' column to show only hour and minute
    # computed_suirenitar_df['time'] = computed_suirenitar_df['time'].dt.strftime('%Y-%m-%dT%H:%M')
    
    # try:
    #     output = computed_suirenitar_df.to_json(orient='records', date_format='iso')
    #     result = json.loads(output)
    #     return result
    # except Exception as e:
    #     print(f"Error during JSON conversion: {e}")
    #     return None

    # return json_result
            
