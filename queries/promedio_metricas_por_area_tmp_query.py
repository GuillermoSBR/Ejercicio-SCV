
def promedio_metricas_por_area_tmp_query(area):
    return """
    SELECT 
      AVG(co_{}) AS co, 
      AVG(no2_{}) AS no2, 
      AVG(pm10_{}) AS pm10, 
      fecha::date 
    FROM 
      preproduccion.calidad_aire 
    GROUP BY 
      fecha::date
    """.format(area,area,area)