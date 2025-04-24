import confuse
from sqlalchemy import create_engine, text
import pandas as pd

config = confuse.Configuration('db_config', __name__)
config.set_file('connectors/db_config.yml')

postgres_config = config['postgres'].get()
posgres_url = (f"postgresql://{postgres_config['user']}:{postgres_config['pass']}@{postgres_config['host']}:"
               f"{postgres_config['port']}/{postgres_config['db_name']}")
posgres_engine = create_engine(posgres_url)

def posgres_delete_by_value(df,table_name,schema,delete_row):
    delete_values = df[delete_row].dropna().unique().tolist()
    batch_size = 1000
    with posgres_engine.begin() as conn:
        for i in range(0, len(delete_values), batch_size):
            batch = delete_values[i:i + batch_size]
            delete_values_list = ', '.join([f":p{j}" for j in range(len(batch))])
            delete = f"DELETE FROM {schema}.{table_name} WHERE {delete_row} IN ({delete_values_list})"
            params = {f"p{j}": fecha for j, fecha in enumerate(batch)}
            conn.execute(text(delete), params)

def posgres_truncate(table_name,schema):
        with posgres_engine.begin() as conn:
                delete = f"TRUNCATE TABLE {schema}.{table_name}"
                conn.execute(text(delete))

def df_read_postgres(query):
    df = pd.read_sql_query(query, posgres_engine)
    return df

def df_save_postgres(df, table_name, schema):
    df.to_sql(name=table_name, con= posgres_engine, schema=schema, if_exists='append', index=False)