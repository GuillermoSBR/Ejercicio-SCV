
def promedio_metricas_por_dia_query():
    return """
        SELECT AVG(co) AS avg_co,
               AVG(no2) AS avg_no2,
               AVG(pm10) AS avg_pm10,
               fecha
        FROM preproduccion.promedio_metricas_por_area_tmp
        group by fecha
    """