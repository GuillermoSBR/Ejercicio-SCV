from connectors.connectors import *
from transforms import *
from queries.promedio_metricas_por_area_tmp_query import *
from queries.promedio_metricas_por_dia_query import *
from queries.dias_impacto_contaminacion_query import *
from queries.aqi_categories_query import *
from create_schemes import *
import json
from flask import Flask

app = Flask(__name__)

create_schemes_posgres(app)

config = confuse.Configuration('main_config', __name__)
config.set_file('config.yml')

@app.route('/calidad_aire', methods=["POST"])
def calidad_aire(): #Ej 1
    try:
        df = pd.read_csv("datasets/calidad-aire.csv")
        df = caldad_aire_transform(df)

        posgres_delete_by_value(df = df,
                                table_name=config['DL']['calidad_aire']['table_name'].get(),
                                schema=config['DL']['calidad_aire']['schema'].get(),
                                delete_row=config['DL']['calidad_aire']['delete_row'].get())

        df_save_postgres(df = df,
                         table_name=config['DL']['calidad_aire']['table_name'].get(),
                         schema=config['DL']['calidad_aire']['schema'].get())

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

@app.route('/promedio_metricas_por_area_tmp', methods=["POST"])
def promedio_metricas_por_area_tmp():
    try:
        posgres_truncate(table_name=config['DL']['promedio_metricas_por_area_tmp']['table_name'].get(),
                         schema=config['DL']['promedio_metricas_por_area_tmp']['schema'].get())
        areas = config['areas'].get()
        for area in areas:
            query = promedio_metricas_por_area_tmp_query(area)
            df = df_read_postgres(query)
            df = promedio_metricas_por_area_tmp_transform(df)
            df_save_postgres(df=df,
                             table_name=config['DL']['promedio_metricas_por_area_tmp']['table_name'].get(),
                             schema=config['DL']['promedio_metricas_por_area_tmp']['schema'].get())
    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

@app.route('/aqi_indicators', methods=["POST"])
def aqi_indicators():
    try:

        df_aqi_categorias = pd.read_csv("datasets/aqi_categorias.csv")
        posgres_truncate(table_name=config['DL']['aqi_categorias']['table_name'].get(),
                         schema=config['DL']['aqi_categorias']['schema'].get())
        df_save_postgres(df=df_aqi_categorias,
                         table_name=config['DL']['aqi_categorias']['table_name'].get(),
                         schema=config['DL']['aqi_categorias']['schema'].get())

        promedio_metricas_por_dia_sql = promedio_metricas_por_dia_query()
        df = df_read_postgres(promedio_metricas_por_dia_sql)
        df_aqi = promedio_metricas_por_dia_transform(df)

        aqi_categories_sql = aqi_categories_query()
        df_aqi_categories = df_read_postgres(aqi_categories_sql)

        #Resultado Ejercicio 2
        df_day_indicator = df_day_indicator_transform(df_aqi, df_aqi_categories)
        posgres_delete_by_value(df=df_day_indicator,
                                table_name=config['DW']['indicador_calidad_aire']['table_name'].get(),
                                schema=config['DW']['indicador_calidad_aire']['schema'].get(),
                                delete_row=config['DW']['indicador_calidad_aire']['delete_row'].get())
        df_save_postgres(df=df_day_indicator,
                         table_name=config['DW']['indicador_calidad_aire']['table_name'].get(),
                         schema=config['DW']['indicador_calidad_aire']['schema'].get())

        #Resultado Ejercicio 3
        top_10_best_days = top_10_best_days_transform(df_day_indicator)
        posgres_delete_by_value(df=top_10_best_days,
                                table_name=config['DW']['top_10_dias']['table_name'].get(),
                                schema=config['DW']['top_10_dias']['schema'].get(),
                                delete_row=config['DW']['top_10_dias']['delete_row'].get())
        df_save_postgres(df=top_10_best_days,
                         table_name=config['DW']['top_10_dias']['table_name'].get(),
                         schema=config['DW']['top_10_dias']['schema'].get())

        # Resultado Ejercicio 4
        top3_per_month_year = top3_per_month_transform(df_day_indicator)
        posgres_delete_by_value(df=top3_per_month_year,
                                table_name=config['DW']['top_3_por_mes']['table_name'].get(),
                                schema=config['DW']['top_3_por_mes']['schema'].get(),
                                delete_row=config['DW']['top_3_por_mes']['delete_row'].get())
        df_save_postgres(df=top3_per_month_year,
                         table_name=config['DW']['top_3_por_mes']['table_name'].get(),
                         schema=config['DW']['top_3_por_mes']['schema'].get())

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

@app.route('/viajes_transporte_publico', methods=["POST"])
def viajes_transporte_publico():
    try:
        df = pd.read_csv("datasets/dataset_viajes_sube.csv")
        df = viajes_transporte_transform(df)

        posgres_delete_by_value(df = df,
                               table_name=config['DL']['viajes_transporte_publico']['table_name'].get(),
                               schema=config['DL']['viajes_transporte_publico']['schema'].get(),
                               delete_row=config['DL']['viajes_transporte_publico']['delete_row'].get())

        df_save_postgres(df = df,
                         table_name=config['DL']['viajes_transporte_publico']['table_name'].get(),
                         schema=config['DL']['viajes_transporte_publico']['schema'].get())
    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

@app.route('/dias_menor_impacto_contaminacion/<criteria>', methods=["POST"])
def dias_menor_impacto_contaminacion(criteria):
    try:
    # Ejercicio 5
        if criteria.lower() == 'menor':
            table = 'dias_menor_impacto_contaminacion'
            query_criteria = 'ASC'
        else:
            table = 'dias_mayor_impacto_contaminacion'
            query_criteria = 'DESC'

        dias_impacto_contaminacion_sql = dias_impacto_contaminacion_query(query_criteria)
        df = df_read_postgres(dias_impacto_contaminacion_sql)
        df = dias_impacto_contaminacion_transform(df)
        posgres_delete_by_value(df=df,
                                table_name=config['DW'][table]['table_name'].get(),
                                schema=config['DW'][table]['schema'].get(),
                                delete_row=config['DW'][table]['delete_row'].get())

        df_save_postgres(df=df,
                         table_name=config['DW'][table]['table_name'].get(),
                         schema=config['DW'][table]['schema'].get())

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

@app.route('/run_all', methods=["POST"])
def run_all():
    try:
        calidad_aire()
        promedio_metricas_por_area_tmp()
        aqi_indicators()
        viajes_transporte_publico()
        dias_menor_impacto_contaminacion('menor')
        dias_menor_impacto_contaminacion('mayor')

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=8080, debug=True)