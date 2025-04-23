import confuse
from sqlalchemy import create_engine
import pandas as pd

config = confuse.Configuration('db_config', __name__)
config.set_file('connectors/db_config.yml')

postgres_config = config['postgres'].get()
posgres_url = (f"postgresql://{postgres_config['user']}:{postgres_config['pass']}@{postgres_config['host']}:"
               f"{postgres_config['port']}/{postgres_config['db_name']}")
posgres_engine = create_engine(posgres_url)

def df_read_postgres(query):
    df = pd.read_sql_query(query, posgres_engine)
    return df

def df_save_postgres(df, table_name, schema):
    df.to_sql(name=table_name, con= posgres_engine, schema=schema, if_exists='append', index=False)
    return 0