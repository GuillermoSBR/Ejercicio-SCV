import pandas as pd
import numpy as np
import aqi

def caldad_aire_transform(df):
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

def promedio_metricas_por_area_tmp_transform(df):
    df = df.astype( {'co': 'float', 'no2': 'float', 'pm10': 'float'})
    return df

def calc_aqi_row(row):
    pm10_aqi = aqi.to_iaqi(aqi.POLLUTANT_PM10, str(row['avg_pm10']), algo=aqi.ALGO_EPA)
    no2_aqi = aqi.to_iaqi(aqi.POLLUTANT_NO2_1H, str(row['avg_no2']), algo=aqi.ALGO_EPA)
    co_aqi = aqi.to_iaqi(aqi.POLLUTANT_CO_8H, str(row['avg_co']), algo=aqi.ALGO_EPA)
    return pd.Series({'pm10_aqi': pm10_aqi, 'no2_aqi': no2_aqi, 'co_aqi': co_aqi})

def promedio_metricas_por_dia_transform(df):
    df = df.fillna(0)
    df_aqi = df.join(df.apply(calc_aqi_row, axis=1))
    df_aqi = df_aqi.drop(['avg_co', 'avg_no2', 'avg_pm10'], axis=1)
    df_aqi = df_aqi.assign(aqi=df_aqi[["pm10_aqi", "no2_aqi", "co_aqi"]].max(axis=1))[["fecha", "aqi"]]
    return df_aqi

def get_aqi_category(aqi_value,df_aqi_categories):
    row = df_aqi_categories[(df_aqi_categories['aqi_inf'] <= aqi_value) & (df_aqi_categories['aqi_sup'] >= aqi_value)]
    return row[['categoria', 'nivel']].iloc[0]

def df_day_indicator_transform(df_aqi,df_aqi_categories):
    df_aqi[['categoria', 'nivel']] = df_aqi['aqi'].apply(lambda x: get_aqi_category(x, df_aqi_categories))
    return df_aqi

def top_10_best_days_transform(df):
    df_top_10 = df.sort_values(by='aqi', ascending=True).head(10)
    return df_top_10

def top3_per_month_transform(df):
    df['año'] = df['fecha'].dt.year
    df['mes'] = df['fecha'].dt.month
    df_top3_per_month= df.sort_values(by='aqi', ascending=True).groupby(['año', 'mes']).head(3)
    df_top3_per_month = df_top3_per_month.sort_values(by=['año', 'mes','aqi'], ascending=True)
    df_top3_per_month = df_top3_per_month.drop(columns=['año', 'mes'])
    return df_top3_per_month