import pandas as pd
# def handle_null_values(df):
#     #for the first null value with no preceeding value.
#     df['value']=df['value'].fillna(method='bfill')
#     #since water levels are continuous and expected to follow a natural trend we interpolate null values
#     df['value']=df['value'].interpolate(method='linear') 
    

# def handle_outliers(df):
#     min_value=50
#     max_value=10000000
#     # Remove rows where the specific column's value is below the minimum or above the maximum threshold
#     df = df[(df['value'] >= min_value) & (df['value'] <= max_value)].copy() 
#     # print(df.head())
    
#     return df

def preprocess_data(df):   
    df['datetime']=pd.to_datetime(df['datetime'])
    df['value']=pd.to_numeric(df['value'],errors='coerce')
    df.set_index('datetime',inplace=True)
    # df=handle_outliers(df)
    # handle_null_values(df)
    return df
