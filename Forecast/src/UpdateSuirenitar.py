from dotenv import load_dotenv
import os
load_dotenv()

def get_latest_datetime_from_tables(db,GALCHI_TABLE,BUDHI_TABLE):
    try:
        # Fetch the latest datetime from both tables
        query_galchi = f"SELECT MAX(dateTime) FROM {GALCHI_TABLE}"
        query_budhi = f"SELECT MAX(dateTime) FROM {BUDHI_TABLE}"
        
        latest_galchi_time = db.fetch(query_galchi)
        latest_budhi_time = db.fetch(query_budhi)
        
        # Extract datetime from the tuple
        latest_galchi_time = latest_galchi_time[0][0] if latest_galchi_time else None
        latest_budhi_time = latest_budhi_time[0][0] if latest_budhi_time else None
        
        # Compare the two datetime values
        latest_datetime = max(latest_galchi_time, latest_budhi_time) if latest_galchi_time and latest_budhi_time else None
        
        print(f"The latest datetime is: {latest_datetime}")
        return latest_datetime
    except Exception as e:
        print(f"Error fetching latest datetime: {e}")
        return None



def update_suirenitar_table(db,GALCHI_TABLE,BUDHI_TABLE,SIURENITAR_TABLE):
    try:
        # Get the latest datetime from both tables
        latest_datetime = get_latest_datetime_from_tables(db, GALCHI_TABLE, BUDHI_TABLE)
        
        if latest_datetime is None:
            return
        # Fetch rows from Galchi and Budhi tables for the latest datetime
        query_galchi = f"SELECT * FROM {GALCHI_TABLE} WHERE dateTime = '{latest_datetime}'"
        query_budhi = f"SELECT * FROM {BUDHI_TABLE} WHERE dateTime = '{latest_datetime}'"
        
        galchi_row = db.fetch(query_galchi)
        budhi_row = db.fetch(query_budhi)

        # Initialize variables for summing discharge values
        galchi_value = galchi_row[0][1] if galchi_row else None  # Adjusted index to access correct column
        budhi_value = budhi_row[0][1] if budhi_row else None  # Adjusted index to access correct column

        print(f"Galchi discharge: {galchi_value}")
        print(f"Budhi discharge: {budhi_value}")
        # Sum values if both rows exist
        if galchi_value is not None and budhi_value is not None:
            total_discharge = galchi_value + budhi_value
        else:
            # Handle missing values, keeping previous valid value from the Suirenitar table
            total_discharge = get_previous_valid_value(db,SIURENITAR_TABLE, latest_datetime)

            # If one of the tables has a value, add it to the total discharge
            if galchi_value is not None:
                total_discharge += galchi_value
            elif budhi_value is not None:
                total_discharge += budhi_value
        
        # Update the Suirenitar table with the computed discharge
        update_query = f"""
        INSERT INTO {SIURENITAR_TABLE} (dateTime, discharge) 
        VALUES ('{latest_datetime}', {total_discharge})
        """
        db.execute_query(update_query)
        print(f"Updated Suirenitar table with datetime {latest_datetime} and discharge {total_discharge}")
    
    except Exception as e:
        print(f"Error updating Suirenitar table: {e}")


def get_previous_valid_value(db,SIURENITAR_TABLE, latest_datetime):
    try:
        # Fetch the last valid discharge value before the latest datetime
        query = f"""
        SELECT discharge FROM {SIURENITAR_TABLE}
        WHERE dateTime < '{latest_datetime}' 
        ORDER BY dateTime DESC LIMIT 1
        """
        result = db.fetch(query)
        
        if result:
            return result['discharge']
        return 0  # Return 0 if no previous value is found
    
    except Exception as e:
        print(f"Error fetching previous valid value: {e}")
        return 0
