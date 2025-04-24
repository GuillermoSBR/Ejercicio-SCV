import pandas as pd

def save_caldad_aire_posgres_transform(df):
    df.columns = df.columns.str.lower()

    df = df[(df['hora'] < 24) & (df['hora'] > 0)].copy()
    df['hora'] = df['hora'].replace(24, 0)

    df['fecha'] = pd.to_datetime(df['fecha'], format='%d%b%Y:%H:%M:%S')
    df['datetime'] = pd.to_datetime(df['fecha'].dt.strftime('%Y-%m-%d') + ' ' + df['hora'].astype(str) + ':00:00')
    df = df.drop(columns=['fecha', 'hora'])
    df = df.rename(columns={'datetime': 'fecha'})

    df = df.astype(
        {'co_centenario': 'string', 'no2_centenario': 'string', 'pm10_centenario': 'string', 'co_cordoba': 'string',
         'no2_cordoba': 'string', 'pm10_cordoba': 'string', 'co_la_boca': 'string', 'no2_la_boca': 'string',
         'pm10_la_boca': 'string', 'co_palermo': 'string', 'no2_palermo': 'string', 'pm10_palermo': 'string'})
    df = df.reindex(
        columns=['fecha','co_centenario','no2_centenario','pm10_centenario','co_cordoba','no2_cordoba','pm10_cordoba',
                 'co_la_boca','no2_la_boca','pm10_la_boca','co_palermo','no2_palermo','pm10_palermo'])
    df = df.fillna('n/d')
    return df