from connectors.connectors import *
from transforms import *
from queries.promedio_metricas_por_area_tmp_query import *
from queries.promedio_metricas_por_dia_query import *
from queries.get_aqi_categories_query import *
import json
import aqi

config = confuse.Configuration('main_config', __name__)
config.set_file('config.yml')

def save_calidad_aire_posgres(): #Ej 1
    try:
        df = pd.read_csv("datasets/calidad-aire.csv")
        df = save_caldad_aire_posgres_transform(df)

        posgres_delete_by_value(df,'calidad_aire','public','fecha')
        df_save_postgres(df,'calidad_aire','public')

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

def save_promedio_metricas_por_area_tmp(area):
    query = promedio_metricas_por_area_tmp_query(area)
    df = df_read_postgres(query)
    #TODO transform
    df_save_postgres(df, 'promedio_metricas_por_area_tmp', 'public')

def calc_aqi_row(row):
    pm10_aqi = aqi.to_iaqi(aqi.POLLUTANT_PM10, str(row['avg_pm10']), algo=aqi.ALGO_EPA)
    no2_aqi = aqi.to_iaqi(aqi.POLLUTANT_NO2_1H, str(row['avg_no2']), algo=aqi.ALGO_EPA)
    co_aqi = aqi.to_iaqi(aqi.POLLUTANT_CO_8H, str(row['avg_co']), algo=aqi.ALGO_EPA)
    return pd.Series({'pm10_aqi': pm10_aqi, 'no2_aqi': no2_aqi, 'co_aqi': co_aqi})

def get_aqi_indicators():
    areas = config['areas'].get()
    posgres_truncate('promedio_metricas_por_area_tmp', 'public')
    for area in areas:
        save_promedio_metricas_por_area_tmp(area)

    promedio_metricas_por_dia_sql = promedio_metricas_por_dia_query()
    df = df_read_postgres(promedio_metricas_por_dia_sql)
    df = df.fillna(0)
    df_aqi = df.join(df.apply(calc_aqi_row, axis=1))
    df_aqi = df_aqi.drop(['avg_co', 'avg_no2', 'avg_pm10'], axis=1)
    df_max = df_aqi.assign(aqi=df_aqi[["pm10_aqi", "no2_aqi", "co_aqi"]].max(axis=1))[["fecha", "aqi"]]
    get_aqi_categories_sql = get_aqi_categories_query()
    df_aqi_categories = df_read_postgres(get_aqi_categories_sql)

    df_result = df_max.merge(df_aqi_categories, how='cross')
    df_result = df_result[
        (df_result['aqi'] >= df_result['aqi_inf']) &
        (df_result['aqi'] <= df_result['aqi_sup'])
        ]
    #Resultado Ejercicio 2
    df_result = df_result[['fecha', 'aqi', 'categoria']]
    print(df_result)

    #Resultado Ejercicio 3
    top_10_best_days = df_result.sort_values(by='aqi').head(10)
    print(top_10_best_days)

    df_result['año'] = df_result['fecha'].dt.year
    df_result['mes'] = df_result['fecha'].dt.month
    df_sorted = df_result.sort_values(by='aqi')

    top3_per_month_year = df_sorted.groupby(['año', 'mes']).head(3).reset_index(drop=True)
    top3_per_month_year = top3_per_month_year.drop(columns=['año', 'mes'])
    # Resultado Ejercicio 4
    top3_per_month_year = top3_per_month_year.sort_values(by='fecha', ascending=False)

    print(top3_per_month_year)