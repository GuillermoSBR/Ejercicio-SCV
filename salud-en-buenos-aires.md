Salud En Buenos Aires
=====================

Introducción
------------

En este ejercicio se intenta evaluar al candidato para el puesto de Data Engineering de SCVSoft.

Vamos a trabajar con dos datasets [calidad de aire desde 2009](https://cdn.buenosaires.gob.ar/datosabiertos/datasets/agencia-de-proteccion-ambiental/calidad-aire/calidad-aire.csv) y [cantidad de viajes en transporte público](https://cdn.buenosaires.gob.ar/datosabiertos/datasets/transporte-y-obras-publicas/sube/dataset_viajes_sube.csv) del Gobierno de la Ciudad de Buenos Aires [(fuentes)](#referencias).

Primer ejercicio
================

Hay que cargar la data del dataset de aire para poder procesarla. Recomendamos cargarla en una base de datos SQL de tal manera de poder procesarla de esa manera. La base de datos puede ser entre ellas:
1. MySQL
2. PostgreSQL
3. Redshift
4. Snowflake
5. BigQuery

Se puede utilizar python, ruby, jupyter-notebooks o cualquier tipo de tecnología ETL.

Segundo ejercicio
=================

En el dataset de calidad de aire vemos que hay 3 tipos de mediciones:

1. Monóxido de Carbono (CO)
2. Dióxido de Nitrogeno (NO2)
3. Partículas de menos de 10 milímetros (PM10)

A su vez hay 4 lugares que el gobierno mide:
1. Palermo
2. Calle Córboba
3. Parque Centenario
4. La Boca

Juntando y agrupando todas los mediciones (cuando las hay), hacer un estimado/promedio de calidad de aire en toda la ciudad, lo que sería equivalente a tener un indicador si el día fue bueno o malo.

Tercer ejercicio
================

En el dataset de calidad de aire usando el indicador del previo ejercicio, calcular los mejores 10 días de la ciudad en calidad de aire de todo el dataset.

Cuarto ejercicio
================

Nuevamente en el dataset de calidad de aire calcular para cada mes los 3 mejores días.

Quinto ejercicio
================

Cargar el segundo dataset (el de transportes de la ciudad de buenos aires). Asumir que si hay menos gente en el transporte público, el impacto en la población es menor y obtener los días que menos contaminación hubo, con menor impacto a la población. Obtener los días que mas gente se intoxicó equivalente a decir los días de mayor impacto en la población.

Sexto ejercicio
===============

Infraestructura. Cargar y correr cualquiera de los ejercicios anteriores en un entorno amigable con la tecnologías modernas. Puede ser entre los siguientes entornos y pueden ser algún otro tipo de tecnología de "contener" y/o hacer management:

* Docker (docker-compose también)
* Airflow
* Kubernetes
* kaggle

Referencias
===========
1. [Página de fuente de la calidad del aire del Gobierno de la Ciudad de Buenos Aires](https://data.buenosaires.gob.ar/dataset/calidad-aire)
2. [Página de fuente de los transportes del Gobierno de la Ciudad de Buenos Aires](https://data.buenosaires.gob.ar/dataset/sube).

