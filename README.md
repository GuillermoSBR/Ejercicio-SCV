
## Cómo correr el programa

Parado en la raiz del proyecto ejecutar este comando:

   ```bash
   docker compose up --build
   ```

Luego enviar la siguiente solicitud como un POST (Creé este endpoint por conveniencia asi hace todo con una sola llamada y no tener que ejecutar 6):

   ```
   http://localhost:8080/run_all
   ```

Luego conectarse a la base de datos con una coneccion que tenga las siguientes caracteristicas:

- **Host**: `localhost`
- **Puerto**: `5433`
- **Usuario**: `postgres`
- **Contraseña**: `tostadojq`

Para ver las tablas que piden los ejercicios ejecutar las siguientes queries:

### Ejercicio 1

```sql
SELECT * FROM preproduccion.calidad_aire;
```

### Ejercicio 2

```sql
SELECT * FROM produccion.indicador_calidad_aire;
```

### Ejercicio 3

```sql
SELECT * FROM produccion.top_10_dias;
```

### Ejercicio 4

```sql
SELECT * FROM produccion.top_3_por_mes;
```

### Ejercicio 5

```sql
SELECT * FROM preproduccion.viajes_transporte_publico;
SELECT * FROM produccion.dias_mayor_impacto_contaminacion;
SELECT * FROM produccion.dias_menor_impacto_contaminacion;
```