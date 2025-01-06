from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
load_dotenv()



def get_rows_up_to_datetime(db, table_name, datetime):
    try:
        # Fetch all rows from the table up to and including the given datetime
        query = f"""
            SELECT dateTime, discharge FROM {table_name}
            WHERE dateTime <= '{datetime}'
            ORDER BY dateTime DESC
        """
        return (db.fetch(query))[0][1]
    except Exception as e:
        print(f"Error fetching rows up to {datetime} from {table_name}: {e}")
        return []
    
    
def get_latest_dateTime(shifted_galchi_df,shifted_budhi_df):    
    latest_galchi_df_time = shifted_galchi_df['dateTime'].iloc[0]
    latest_budhi_df_time = shifted_budhi_df['dateTime'].iloc[0]
    latest_dateTime=max(latest_galchi_df_time, latest_budhi_df_time) 
    return latest_dateTime

def insert_suirenitar_table(db,latest_datetime,GALCHI_TABLE,BUDHI_TABLE,SIURENITAR_TABLE):
    try:
        
        if latest_datetime is None:
            return

        # Fetch rows from Galchi and Budhi tables for the latest datetime
        query_galchi = f"SELECT * FROM {GALCHI_TABLE} WHERE dateTime = '{latest_datetime}'"
        query_budhi = f"SELECT * FROM {BUDHI_TABLE} WHERE dateTime = '{latest_datetime}'"
        
        galchi_row = db.fetch(query_galchi)
        budhi_row = db.fetch(query_budhi)

        # Initialize variables for summing discharge values
        galchi_value = galchi_row[0][1] if galchi_row else None 
        budhi_value = budhi_row[0][1] if budhi_row else None  
        total_discharge=0
        
        # Sum values if both rows exist
        if galchi_value is not None and budhi_value is not None:
            total_discharge = galchi_value + budhi_value
        else:
            # If one of the tables has a value for latest time, add it to the total discharge
            if galchi_value is not None:
                total_discharge = get_rows_up_to_datetime(db,BUDHI_TABLE, latest_datetime)+galchi_value
            elif budhi_value is not None:
                total_discharge=get_rows_up_to_datetime(db,GALCHI_TABLE, latest_datetime)+budhi_value
        # Update or insert into the Suirenitar table
        update_query = f"""
            INSERT INTO {SIURENITAR_TABLE} (dateTime, discharge) 
            VALUES ('{latest_datetime}', {total_discharge})
            ON CONFLICT (dateTime)
            DO UPDATE SET discharge = EXCLUDED.discharge
            """
        db.execute_query(update_query)
        print(f"Updated Suirenitar table with datetime {latest_datetime} and discharge {total_discharge}")
        return True, latest_datetime
    
    except Exception as e:
        print(f"Error updating Suirenitar table: {e}")



def clean_datetime_value(value):
            if isinstance(value, datetime):
                return value.replace(tzinfo=None)  # Remove timezone if present
            return value


def recalculate_suirenitar_table(db, galchi_table, budhi_table, siurenitar_table):
    try:
        
        # Fetch all rows in siurenitar_table
        query_suirenitar = f"SELECT dateTime FROM {siurenitar_table} ORDER BY dateTime ASC"
        siurenitar_rows = db.fetch(query_suirenitar)

        
        nepali_offset = pd.Timedelta(hours=5, minutes=45)
        for suirenitar_time in siurenitar_rows:
            suirenitar_time = suirenitar_time[0]

            # Find the closest previous or equal datetime in galchi_table
            query_galchi = f"""
                SELECT discharge FROM {galchi_table}
                WHERE dateTime <= '{suirenitar_time}'
                ORDER BY dateTime DESC LIMIT 1
            """
            galchi_discharge = db.fetch(query_galchi)
            galchi_discharge = galchi_discharge[0][0] if galchi_discharge else 0

            # Find the closest previous or equal datetime in budhi_table
            query_budhi = f"""
                SELECT discharge FROM {budhi_table}
                WHERE dateTime <= '{suirenitar_time}'
                ORDER BY dateTime DESC LIMIT 1
            """
            budhi_discharge = db.fetch(query_budhi)
            budhi_discharge = budhi_discharge[0][0] if budhi_discharge else 0

            # Calculate the total discharge
            total_discharge = galchi_discharge + budhi_discharge


            suirenitar_time=clean_datetime_value(suirenitar_time)
            # Update the siurenitar_table for this dateTime  
            update_query = f"""
                UPDATE {siurenitar_table}
                SET discharge = {total_discharge}
                WHERE dateTime = '{suirenitar_time}'
            """
            db.execute_query(update_query)
            print(f"Updated {siurenitar_table}: dateTime={suirenitar_time}, discharge={total_discharge}")

        print(f"Recalculated all rows in {siurenitar_table}.")

    except Exception as e:
        print(f"Error recalculating {siurenitar_table}: {e}")
        
        
        