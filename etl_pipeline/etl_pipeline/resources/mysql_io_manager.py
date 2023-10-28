import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
class MySQLIOManager:
    def __init__(self, config):
        # config for connecting to MySQL database
        self._config = config
        db_user = config['user']
        db_password = config['password']
        db_host = config['host']
        db_port = config['port']
        db = config['database']
        self.engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db}")
    def extract_data(self, sql: str) -> pd.DataFrame:
            # Connect to the MySQL database
            #my_conn = mysql.connector.connect(**self._config)

            # Read data from the database using the provided SQL query
        my_data = pd.read_sql(sql, self.engine)


        return my_data


