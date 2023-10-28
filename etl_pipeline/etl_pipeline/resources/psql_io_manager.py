import pandas as pd
from dagster import IOManager, OutputContext, InputContext
import psycopg2
from sqlalchemy import create_engine


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        db_user = config['user']
        db_password = config['password']
        db_host = config['host']
        db_port = config['port']
        db = config['database']
        conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db}'
        self.engine = create_engine(conn_string)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # insert new data from Pandas Dataframe to PostgreSQL table
        conn = self.engine.connect()
        table_name = context.asset_key.path[-1]
        schema_name = "public"
        obj.to_sql(table_name, conn, if_exists='append', schema=schema_name, index = False)

    def load_input(self, context: "InputContext"):
        pass