import pandas as pd
import numpy as np

def save_caldad_aire_posgres_transform(df):
    df.columns = df.columns.str.lower()

    df = df[(df['hora'] < 24) & (df['hora'] > 0)].copy()
    df['hora'] = df['hora'].replace(24, 0)

    df['fecha'] = pd.to_datetime(df['fecha'], format='%d%b%Y:%H:%M:%S')
    df['datetime'] = pd.to_datetime(df['fecha'].dt.strftime('%Y-%m-%d') + ' ' + df['hora'].astype(str) + ':00:00')
    df = df.drop(columns=['fecha', 'hora'])
    df = df.rename(columns={'datetime': 'fecha'})

    df = df.replace(to_replace=r'.*<.*', value=np.nan, regex=True)
    df = df.replace(to_replace=r'.*#.*', value=np.nan, regex=True)
    df = df.replace(to_replace=r'(?i)^s/d$', value=np.nan, regex=True)

    df = df.astype({'co_centenario': 'float', 'no2_centenario': 'float', 'pm10_centenario': 'float', 'co_cordoba': 'float',
    'no2_cordoba': 'float', 'pm10_cordoba': 'float', 'co_la_boca': 'float', 'no2_la_boca': 'float',
    'pm10_la_boca': 'float', 'co_palermo': 'float', 'no2_palermo': 'float', 'pm10_palermo': 'float'})

    df = df.reindex(
        columns=['fecha','co_centenario','no2_centenario','pm10_centenario','co_cordoba','no2_cordoba','pm10_cordoba',
                 'co_la_boca','no2_la_boca','pm10_la_boca','co_palermo','no2_palermo','pm10_palermo'])
    return df