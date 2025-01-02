from DB_Service.Database import Database

db = Database()


db.connect()


db.execute_query('''
        CREATE TABLE IF NOT EXISTS public.Galchi (
            ForecasteddateTime TIMESTAMP,
            discharge_value FLOAT
        );

        CREATE TABLE IF NOT EXISTS public.Budhi_at_Khari (
            ForecasteddateTime TIMESTAMP,
            discharge_value FLOAT
        );

        CREATE TABLE IF NOT EXISTS public.Siurenitar (
            ForecasteddateTime TIMESTAMP,
            discharge_value FLOAT
        );
''')

    


db.disconnect()
