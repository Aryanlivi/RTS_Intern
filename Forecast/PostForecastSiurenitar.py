import pandas as pd  
import matplotlib.pyplot as plt 
import numpy as np
from decimal import Decimal
from Utils import  *
from ApiService import fetch_water_level_data
import time

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
    json_result = computed_suirenitar_df.to_json(orient='records',date_format='iso')
    return json_result
            

def main():
    #for test:
    # galchi_df=pd.DataFrame([{ 'datetime': '2024-12-31T10:45:00+00:00', 'value': 366.34 }])
    # budhi_df=pd.DataFrame([{ 'datetime': '2024-12-31T10:45:00+00:00', 'value': 334.45 }])
    while True:
        galchi_df=fetch_water_level_data(SocketGalchiId)
        galchi_df=pd.DataFrame([galchi_df])
        budhi_df=fetch_water_level_data(SocketBudhiId)
        budhi_df=pd.DataFrame([budhi_df])
        # print(galchi_df)
        print(full_task_pipeline(galchi_df,budhi_df))
        #10 min before next fetch
        time.sleep(600) 
    
main()