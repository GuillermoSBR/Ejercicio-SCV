def dias_impacto_contaminacion_query(criteria):
    return """
    SELECT vtp.fecha, ia.aqi, vtp.cantidad FROM produccion.indicador_calidad_aire ia
    JOIN preproduccion.viajes_transporte_publico vtp ON vtp.fecha = ia.fecha
    ORDER BY ia.aqi {}, vtp.cantidad {} 
    LIMIT 10 
    """.format(criteria,criteria)