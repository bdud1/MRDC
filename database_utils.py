# %%
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import yaml
import psycopg2


class DatabaseConnector():
    # Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self, filename):
        try: 
            with open(filename, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except FileNotFoundError:
            print(f'Error: {filename} not found.')
            return None
        except yaml.YAMLError as e:
            print(f'Error parsing YAML file: {e}')
            return None

    # Now create a method init_db_engine which will read the credentials from 
    # the return of read_db_creds and initialise and return an sqlalchemy database engine.
    def init_db_engine(self, filename):
        db_creds = self.read_db_creds(filename)
        if db_creds is None:
            return None
        try:
            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            HOST = db_creds['RDS_HOST']
            USER = db_creds['RDS_USER']
            PASSWORD = db_creds['RDS_PASSWORD']
            DATABASE = db_creds['RDS_DATABASE']
            PORT = db_creds['RDS_PORT']
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
            return engine
        except KeyError as e:
            print(f'Error: Missing key {e} in credentials.')
            return None
        except Exception as e:
            print(f'Error initializing database engine: {e}')
            return None 


    def list_db_tables(self, engine):
        if engine is None:
            print("Engine is not initialized.")
            return
        try:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            print("Tables in the database:")
            for table in table_names:
                print(table)
        except Exception as e:
            print(f'Error listing database tables: {e}')



connector = DatabaseConnector()
creds = connector.read_db_creds('db_creds.yaml')
engine = connector.init_db_engine('db_creds.yaml')
inspector = connector.list_db_tables(engine)




# %%
