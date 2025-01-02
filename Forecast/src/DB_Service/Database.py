import psycopg2
import os
from dotenv import load_dotenv
from .Loggers import *
import numpy as np

load_dotenv()

SERVER=os.getenv('server')
USERNAME=os.getenv('username')
PASSWORD=os.getenv('password')
DATABASE=os.getenv('database')



logger=Logger().get_logger()
class Database:
    def __init__(self):
        self.cursor=None
        self.connection=None   
    def connect(self):
        try:
            self.connection=psycopg2.connect(
                host=SERVER,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD
            )
            logger.info("Database connection established successfully.")
            print("Connected DB Successfully")
            return self.connection 
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return None
    def execute_query(self,query):
        if self.connection:
            try:
                self.cursor = self.connection.cursor()
                self.cursor.execute(query)
                self.connection.commit()
                logger.info(f"Query executed successfully: {query}")
                self.cursor.close()
            except Exception as e:
                logger.error(f"Error executing query '{query}': {e}")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")
        
    def fetch(self,query):
        try:
            if self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                logger.info(f"Data fetched successfully for query: {query}")
                self.cursor.close()
                logger.info(f"Fetched Data:{result}")
                return result
        except Exception as e:
            logger.error(f"Error fetching data for query '{query}': {e}")
            return None 
        
    def insert_df(self, df, table_name):
        """Insert a single row DataFrame into the specified table. 
        This Function assumes that DF has only one row."""
        try:
            # Ensure cursor is initialized
            if not self.cursor:
                self.cursor = self.connection.cursor()
            data_tuple = tuple(float(value) if isinstance(value, np.float64) else value for value in df.iloc[0])
            print(data_tuple)

            # Create an insert query based on DataFrame columns
            columns = ', '.join(df.columns)
            values_placeholder = ', '.join(['%s'] * len(df.columns))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

            # Use execute to insert a single row
            self.cursor.execute(query, data_tuple)
            self.connection.commit()
            print(f"Data inserted into {table_name} successfully.")
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            self.connection.rollback()