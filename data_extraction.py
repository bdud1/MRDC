
#%%

# Develop a method called read_rds_table in your DataExtractor
# class which will extract the database table to a pandas 
# DataFrame.
        

import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector



class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, DatabaseConnector(), table_name):
        try:
            # Initialize the database engine using the provided DatabaseConnector instance
            engine = DatabaseConnector.init_db_engine('db_creds.yaml')

            # Create a connection to the database
            connection = engine.connect()

            # Read the table into a pandas DataFrame
            df = pd.read_sql_table(table_name, con=connection)

            # Close the connection
            connection.close()

            return df
        except Exception as e:
            print(f"Error reading table '{table_name}' from database: {e}")
            return None
        

extractor = DataExtractor()
creds = extractor.read_rds_table('db_creds.yaml','name')

# %%
# %%
