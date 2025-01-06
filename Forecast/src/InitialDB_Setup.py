from DB_Service.Database import Database
from dotenv import load_dotenv

load_dotenv()

galchi_table = 'Forecast_Galchi_To_Siurenitar'
budhi_table = 'Forecast_Budhi_At_Khari_To_Siurenitar'
siurenitar_table = 'Forecast_Siurenitar_Data'
db = Database()


db.connect()


db.execute_query(f'''
    CREATE TABLE IF NOT EXISTS public.{galchi_table} (
    dateTime TIMESTAMP WITHOUT TIME ZONE UNIQUE,
    discharge FLOAT
    );

    CREATE TABLE IF NOT EXISTS public.{budhi_table} (
        dateTime TIMESTAMP WITHOUT TIME ZONE UNIQUE,
        discharge FLOAT
    );

    CREATE TABLE IF NOT EXISTS public.{siurenitar_table} (
        dateTime TIMESTAMP WITHOUT TIME ZONE UNIQUE,
        discharge FLOAT
    );

''')

    


db.disconnect()
