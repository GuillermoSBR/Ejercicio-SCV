import os
from sqlalchemy import text
from connectors import connectors as cn


def create_schemes_posgres(app):
    schemes_route = './schemas/'

    with cn.posgres_engine.connect() as conn:

        with conn.begin():
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS produccion;'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS preproduccion;'))

            for archivo_sql in os.listdir(schemes_route):
                if archivo_sql.endswith('.sql'):
                    ruta_archivo = os.path.join(schemes_route, archivo_sql)
                    with open(ruta_archivo, 'r') as archivo:
                        sql_script = archivo.read()

                    with app.app_context():
                        conn.execute(text(sql_script))
