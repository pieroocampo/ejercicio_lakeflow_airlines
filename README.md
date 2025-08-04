# Laboratorio Lakehouse de Aerolíneas con Lakeflow Declarative Pipelines


## Dataset

Se usará el dataset en: `/databricks-datasets/airlines/` pero se hará un sampling para reducir su tamaño

- Formato: CSV.
- Se declara el esquema explícitamente en el pipeline.
- Columnas típicas:\
  `Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, Cancelled, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay`.

---

## Parte 1: Capa Bronze — Ingesta cruda con esquema explícito

### Propósito

Capturar los CSV originales sin limpieza ni filtrado.

### Pipeline declarativo ejemplo

```sql
CREATE OR REFRESH STREAMING TABLE bronze_airlines_raw AS
SELECT * FROM STREAM(
  read_files(
    '<directorio con muestra>',
    format => 'csv',
    header => true,
    schema => 'STRUCT<
      Year INT,
      Month INT,
      DayofMonth INT,
      DayOfWeek INT,
      DepTime INT,
      CRSDepTime INT,
      ArrTime INT,
      CRSArrTime INT,
      UniqueCarrier STRING,
      FlightNum STRING,
      TailNum STRING,
      ActualElapsedTime INT,
      CRSElapsedTime INT,
      AirTime INT,
      ArrDelay DOUBLE,
      DepDelay DOUBLE,
      Origin STRING,
      Dest STRING,
      Distance DOUBLE,
      Cancelled INT,
      Diverted INT,
      CarrierDelay DOUBLE,
      WeatherDelay DOUBLE,
      NASDelay DOUBLE,
      SecurityDelay DOUBLE,
      LateAircraftDelay DOUBLE
    >'
  )
);
```

---

## Parte 2: Capa Silver — Calidad y enriquecimiento (sin reemplazar NULLs)

### Propósito

Aplicar validaciones, mantener NULLs en retrasos y derivar columnas útiles. La capa Silver no imputa NULLs en `ArrDelay`/`DepDelay`; se mantiene su ausencia y los promedios posteriores los ignorarán.

### Validaciones (expectativas) que se aplican

- `UniqueCarrier`, `Origin`, `Dest` no nulos.
- `Distance > 0`.
- `CRSDepTime` y `CRSArrTime` entre 0 y 2359.

### Columnas derivadas

- `flight_date`: fecha completa.
- `scheduled_hour`: hora programada (`floor(CRSDepTime / 100)`).
- `route`: `Origin-Dest`. es decir, la concatenación con -
- `ArrDelay_cleaned` y `DepDelay_cleaned`: si `Cancelled = 1` entonces NULL, si el original es NULL permanece NULL.
- `on_time_flag`: 1 si no está cancelado y `ArrDelay_cleaned` está entre -15 y 15.

### Pipeline declarativo ejemplo (silver\_airlines)


---

## Parte 3: Capa Gold — Vistas materializadas con promedios que ignoran NULLs

### Propósito

Generar métricas curadas:

### Vista materializada de retraso promedio por aerolínea y hora programada
* Promedio de retraso de llegada (arr_delay)
* conteo de vuelos con retraso
* conteo de vuelos totales
* Al nivel Aerolínea (UniqueCarrier) y Hora agendada (scheduled_hour)


### Vista de rutas con más cancelaciones
* A nivel ruta
* Vuelos totales
* Total de vuelos cancelados
* Porcentaje de vuelos cancelados


---

## Optimización de layout y lecturas

Opcional: Si te da tiempo, modifica la tabla silver para particionar por mes y año

Opcional 2: Si te da tiempo, modifica la primera vista materializada gold para poner un cluster líquido por carrier y hora


