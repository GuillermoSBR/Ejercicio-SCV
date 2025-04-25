def dias_impacto_contaminacion_query(criteria):
    return """
    SELECT vtp.fecha, ica.aqi, vtp.cantidad 
FROM produccion.indicador_calidad_aire ica 
  JOIN (
  SELECT fecha, SUM(cantidad) as cantidad 
    FROM preproduccion.viajes_transporte_publico 
    GROUP BY fecha
    ) vtp ON vtp.fecha = ica.fecha 
ORDER BY ica.aqi {}, vtp.cantidad {} 
LIMIT 10  
    """.format(criteria,criteria)