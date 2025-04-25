CREATE TABLE IF NOT EXISTS produccion.indicador_calidad_aire (
	fecha timestamp,
    aqi int,
    categoria varchar(40),
    nivel varchar(1)
);