# Benchmark local de grandes volúmenes

Los cinco escenarios de esta guía son **objetivos de prueba**, no resultados
verificados. No se ejecutan automáticamente. Empezar con la prueba pequeña y
avanzar de a un escenario, comprobando espacio libre y `metrics.json`.

## Qué se encontró en el proyecto

- `scripts/run_demo.py` genera tres máquinas, `days=1/24`, intervalo de un minuto:
  60 lecturas por máquina, 180 en total. El generador habitual configura lecturas
  con `int(days * 1440 // interval)`, además de máquinas, semilla y anomalías.
- Leo escribe cada lectura progresivamente, pero `_plan_anomalies` construye y
  mezcla una lista de posiciones de tamaño proporcional a todas las lecturas.
  También conserva el detalle de todas las anomalías. Ese estado puede agotar RAM.
- La ingesta habitual usa `read_text().splitlines()`, listas de líneas originales
  y normalizadas, diccionarios de registros y listas de timestamps. Además relee
  y consolida particiones históricas existentes en memoria. Esa ruta es para demos.
- La lectura directa de calidad utiliza `wholetext=True`, con todas las líneas
  de un archivo dentro de una sola fila. La demo usa normalización previa, pero
  exporta DataFrames completos con `toArrow()`. El cargador usa pandas y no es
  apropiado para millones de lecturas. Las copias raw y normalizadas multiplican
  el uso de disco.
- Los joins y ordenamientos de duplicados, las estadísticas y las ventanas de
  Spark pueden derramar datos a disco. Un heap mayor no elimina esa necesidad.

## Ruta separada y decisiones

`python -m scripts.run_benchmark` siempre genera, ingiere y procesa **sin bases**.
No acepta `--load`, no importa cargadores, no inicia Docker, no usa cloud ni cambia
el catálogo compartido. `scripts/run_demo.py` conserva su configuración y sus
exportaciones para la demo.

1. El generador de benchmark reutiliza los perfiles y `_simulate_reading` de Leo.
   Escribe una lectura por vez con un búfer de archivo de 1 MiB. Mantiene solo el
   catálogo de máquinas y contadores; reserva anomalías por muestreo secuencial,
   con episodios de degradación de hasta cinco pasos. No arma un plan de N posiciones
   ni conserva el detalle completo de anomalías en RAM.
2. La ingesta aplica `validate_and_normalize_record` a cada línea. Agrupa hasta
   `--batch-size` filas (10.000 por defecto; máximo 100.000), escribe row groups
   PyArrow y rota archivos a `--rows-per-file` filas (250.000 por defecto).
   Conserva línea original y número en el Parquet temporal y auditoría de rechazos
   en JSONL. No crea copias raw ni JSONL normalizados; no mezcla históricos existentes.
3. Spark lee Parquet con esquema y reutiliza exactamente las reglas de contenido,
   duplicados idénticos y claves en conflicto mediante `clean_telemetry_frame`.
   Materializa la clasificación una vez, escribe el Parquet curado y deriva las
   mismas estadísticas, ventanas de cinco minutos, semáforo y alertas. Los cálculos
   no usan `is_anomaly` como regla de riesgo. Los umbrales siguen siendo los actuales.
4. En este Windows faltan las utilidades nativas de Hadoop que requieren el listado
   de carpetas y la escritura nativa de Spark. Se pasan archivos Parquet explícitos
   al lector y se escriben lotes Arrow desde los workers locales con `mapInArrow`.
   Cada intento de tarea tiene archivos propios; el driver publica solo los
   archivos devueltos por tareas confirmadas. Al driver llegan **nombres y conteos**,
   nunca las lecturas completas. El tamaño Arrow se limita con
   `spark.sql.execution.arrow.maxRecordsPerBatch = batch_size`. Se usan las
   dependencias ya instaladas, sin descargar `winutils.exe` ni DLL externas.
   PySpark 4.0.1 también tiene el fallo de cierre de socket
   [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759) con Python 3.12+
   en Windows. Para esa combinación se selecciona `src.benchmark.spark_worker`,
   que conserva la conexión y cierra el búfer explícitamente. La corrección se
   limita al arranque del worker de benchmark y no modifica paquetes instalados.
5. Los directorios Parquet contienen múltiples `part-*.parquet`, con esquemas
   compatibles con los resultados habituales. `results/alerts/part-*.jsonl`
   conserva los campos del contrato de alertas. No se fuerza un archivo único.
   Estas salidas se destinan al benchmark, no a `run_demo --load-only`.

La generación del benchmark usa otro orden (máquina, luego paso) y otro algoritmo
de selección de anomalías. Una misma configuración y versión de Python produce
el mismo JSONL; **no** reproduce byte por byte la muestra del generador habitual.
Las mediciones y su clasificación por umbrales son reproducibles; `calculated_at`
mantiene el significado del contrato y registra el instante real de procesamiento.
El orden y los bytes de los archivos Parquet pueden variar entre ejecuciones.

## PowerShell: preparación y prueba pequeña

Ejecutar desde la raíz, con `.venv` y `requirements.txt` instalados. JDK 17 o 21.
En este equipo está disponible el siguiente JDK; ajustar si cambia su ubicación:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot'
$env:SPARK_HOME = Join-Path (Get-Location) '.venv\Lib\site-packages\pyspark'
.\.venv\Scripts\python.exe -m scripts.run_benchmark --machines 3 --readings-per-machine 60 --seed 42 --output-dir artifacts/benchmarks
if ($LASTEXITCODE -ne 0) { throw 'El benchmark no terminó correctamente; revisar metrics.json y error.log.' }
```

`SPARK_HOME` explícito evita que los scripts de arranque de Spark busquen otro
Python en Windows. Cada comando abre un proceso Python/JVM nuevo. No reutilizar
una sesión de notebook para cambiar la memoria del driver.

## Cinco escenarios manuales

Los comandos usan la misma configuración de Spark para facilitar comparaciones.
`4g` es el heap de la JVM, **no** la RAM total de la prueba. Para un equipo con
poca memoria empezar con `2g` y escenarios pequeños. Los cambios de configuración
quedan registrados y deben considerarse al comparar tiempos.

| Máquinas | Lecturas por máquina | Registros solicitados |
| ---: | ---: | ---: |
| 10 | 1.000 | 10.000 |
| 100 | 1.000 | 100.000 |
| 100 | 10.000 | 1.000.000 |
| 500 | 10.000 | 5.000.000 |
| 1.500 | 10.000 | 15.000.000 |

Ejecutar **una línea de escenario por vez**, después de revisar el resultado anterior:

```powershell
$opciones = @('--seed', '42', '--output-dir', 'artifacts/benchmarks', '--spark-memory', '4g', '--spark-threads', '2', '--shuffle-partitions', '32')

# 10.000
.\.venv\Scripts\python.exe -m scripts.run_benchmark @opciones --machines 10 --readings-per-machine 1000

# 100.000
.\.venv\Scripts\python.exe -m scripts.run_benchmark @opciones --machines 100 --readings-per-machine 1000

# 1.000.000: ejecutar manualmente
.\.venv\Scripts\python.exe -m scripts.run_benchmark @opciones --machines 100 --readings-per-machine 10000

# 5.000.000: ejecutar manualmente
.\.venv\Scripts\python.exe -m scripts.run_benchmark @opciones --machines 500 --readings-per-machine 10000

# 15.000.000: ejecutar manualmente
.\.venv\Scripts\python.exe -m scripts.run_benchmark @opciones --machines 1500 --readings-per-machine 10000
```

También están disponibles `--batch-size`, `--rows-per-file`, `--interval`
(minutos), `--start-time`, `--anomaly-rate` y `--max-partition-mb` (64 por defecto).
El producto de máquinas y lecturas no puede superar 15.000.000 por corrida.
Usar `--help` para ver todas las opciones.

## Artefactos, conteos y errores

Cada ejecución crea `artifacts/benchmarks/<fecha-UTC>-<UUID>/`. No reutiliza ni
sobrescribe carpetas previas, incluso con la misma semilla. Guarda:

- `telemetry.jsonl`: única copia del JSONL generado.
- `ingestion_rejected.jsonl`: auditoría de ingesta; vacía si no hubo rechazos.
- `results/`: Parquet curado, rechazados, estadísticas, métricas, riesgo y alertas.
- `metrics.json` y `metrics.csv`: configuración, contadores, tiempos y tamaños.
- `error.log`: traceback si hubo una excepción. `work/` queda disponible en fallos;
  en una corrida exitosa se retiran solamente los temporales de esa misma corrida.

Solo considerar exitosa una corrida con `status = success`, código de salida 0
y todos los elementos de `checks` en `true`. Se verifica:

```text
requested = generated = received = valid
received = ingestion_rejected + spark_rejected + valid
ingestion_valid = spark_received = filas del Parquet de ingesta
valid = filas del Parquet curado = suma(n_readings) en estadísticas y ventanas
machines = filas de estadísticas = filas de riesgo
min(n_readings por máquina) = max(n_readings por máquina) = readings_per_machine
```

Los duplicados están incluidos en `spark_rejected`, no se suman dos veces.
Una anomalía sintética puede ser válida: anomalía y rechazo de calidad son
conceptos distintos. Un rechazo inesperado de datos sintéticos hace fallar la
ejecución aun cuando el balance contable cierre.

Además de la agregación distribuida, se verifican los conteos almacenados en los
footers Parquet, sin cargar las tablas a Python. Los pocos `first()` del benchmark
devuelven una fila agregada; el iterador del escritor devuelve solo metadatos de
archivos. No se usa `collect()`, `toPandas()` ni `toArrow()` sobre la telemetría.

Para revisar la última corrida desde PowerShell:

```powershell
$corrida = Get-ChildItem artifacts/benchmarks -Directory | Sort-Object Name -Descending | Select-Object -First 1
$m = Get-Content (Join-Path $corrida.FullName 'metrics.json') -Raw | ConvertFrom-Json
$m.status
$m.counts
$m.checks
$m.seconds
$m.sizes
if ($m.status -ne 'success' -or @($m.checks.PSObject.Properties | Where-Object { $_.Value -ne $true }).Count -gt 0) {
    throw 'Corrida incompleta o sin todos los conteos verificados.'
}
```

`seconds` separa generación, ingesta, arranque de Spark y procesamiento
(calidad, exportación y verificaciones); el total incluye también el entorno y
el cierre/limpieza. Los tamaños se expresan en bytes: JSONL fuente, JSONL total,
Parquet final y Parquet intermedio antes de retirarlo. No representan el pico de
spill ni la asignación física de bloques del sistema de archivos.
El JSON registra SO, CPU lógicas, RAM total/disponible cuando se puede medir,
espacio libre inicial, versiones de Python, Java, Spark/PyArrow y configuración Spark.

Para reunir CSV para gráficos, elegir un archivo nuevo que todavía no exista:

```powershell
$comparacion = 'artifacts/comparacion-benchmarks.csv'
if (Test-Path -LiteralPath $comparacion) { throw 'Elegir otro nombre de comparación.' }
Get-ChildItem artifacts/benchmarks -Recurse -Filter metrics.csv |
    ForEach-Object { Import-Csv -LiteralPath $_.FullName } |
    Export-Csv -LiteralPath $comparacion -NoTypeInformation -Encoding UTF8
```

Filtrar `status = success` para gráficos de rendimiento completos. Mostrar por
separado errores/interrupciones; no extrapolar una ejecución parcial como éxito.

## Detener y dimensionar

`Ctrl+C` interrumpe la prueba, intenta detener Spark y registra `interrupted`
(salida 130). Un fallo usa `failed` (salida 1). No toca datos ni volúmenes Docker.
Una terminación forzada del proceso o del equipo puede dejar `running`: eso
también indica una corrida incompleta. Los contadores en curso son avances
parciales; valores `null` indican etapas que no llegaron a medirse.
Reintentar crea una nueva corrida; no existe reanudación automática de parciales.

Estimaciones iniciales, **no requisitos medidos ni garantía**:

| Escala | RAM total orientativa | Espacio libre orientativo |
| --- | --- | --- |
| 10.000–100.000 | 8 GiB; JVM de 2 GiB | 2–5 GiB |
| 1.000.000 | 16 GiB; JVM de 4 GiB | 10–20 GiB |
| 5.000.000 | 16–32 GiB; JVM de 4–8 GiB | 30–50 GiB |
| 15.000.000 | 32 GiB aconsejables; JVM de 4–8 GiB | 60–100 GiB |

Dependen del equipo, disco, compresión, particiones, SO, procesos abiertos y spill
de joins/ordenamientos. Python, workers Arrow y memoria nativa se suman al heap de
la JVM. Un SSD ayuda. El catálogo usa memoria proporcional a máquinas; el búfer
de ingesta y de cada worker depende del lote, no de los registros totales.
El JSONL ronda varios GB en el escenario mayor y se conserva entre corridas:
reservar espacio acumulado. Usar `source_jsonl_bytes / generated` de una prueba
pequeña para estimar el JSONL grande; dejar margen amplio para temporales.
No se ha medido el pico de RAM/disco ni validado el máximo de 15 millones.

## Pruebas de desarrollo

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_benchmark.py -q
.\.venv\Scripts\python.exe -m pytest -q
```

Las pruebas nuevas usan volúmenes pequeños, reproducibilidad, límites de lote,
auditoría, errores/interrupciones, corridas independientes y Spark real. Incluyen
la regresión del preset habitual de tres máquinas y 180 lecturas. Las pruebas
de integración con servicios permanecen optativas (`BD3_INTEGRATION=1`); no se
necesitan para validar este benchmark y no deben activarse para esta verificación.

Referencias de las API utilizadas: [mapInArrow en Spark 4.0.1](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInArrow.html)
y [toLocalIterator](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html).

## Evidencia de desarrollo del 12/11/2026

Se ejecutó una corrida pequeña por la CLI, con el comando de 10 máquinas y 1.000
lecturas por máquina, semilla 42, heap de 2 GiB, dos hilos y 16 particiones de
shuffle. Entorno: Windows 11, Python 3.12.10, Java 17.0.21, PySpark 4.0.1,
PyArrow 26.0.1, ocho CPU lógicas y aproximadamente 16 GiB de RAM total.

Resultado conservado en
`artifacts/benchmarks/20261113T013135-8da5e380508b473497e850d366ad1b63/metrics.json`
y su `metrics.csv`. El identificador usa UTC (13/11); la ejecución ocurrió
el 12/11 en Argentina. Código de salida 0, `success`, los 14 controles en `true`:

| Medición | Resultado observado |
| --- | ---: |
| Solicitados / generados / recibidos / válidos | 10.000 cada uno |
| Rechazados en ingesta / Spark | 0 / 0 |
| Máquinas / lecturas por máquina | 10 / 1.000 |
| Filas curadas / ventanas / filas de riesgo | 10.000 / 2.000 / 10 |
| Suma de lecturas en ventanas | 10.000 |
| Generación | 0,134 s |
| Ingesta | 0,451 s |
| Arranque de Spark | 11,580 s |
| Procesamiento, exportación y controles | 93,369 s |
| Total | 106,328 s |
| JSONL fuente | 3.243.060 bytes |
| JSONL total, incluidas alertas | 3.295.249 bytes |
| Parquet final | 174.129 bytes |
| Parquet intermedio antes de retirarlo | 2.350.223 bytes |

Estos tiempos describen una única validación pequeña y su costo de arranque y
escritura en Windows; no prueban el rendimiento de los escenarios mayores.
100.000, 1.000.000, 5.000.000 y 15.000.000 permanecen sin ejecutar/verificar.

La suite general se ejecutó con integración de servicios desactivada y
`--continue-on-collection-errors`: **119 aprobadas, 16 fallidas, 2 omitidas,
1 error de colección y 41 subpruebas aprobadas**. Las inconsistencias encontradas
en archivos existentes son:

- `tests/test_dirty_pipeline.py` importa `reconcile_counts`, ausente en
  `scripts/run_demo.py`.
- Quince casos de `tests/test_contamination.py` esperan `rejected_audit_path`,
  ausente en `IngestionResult` de la ingesta habitual.
- Otro caso de contaminación espera dos líneas físicas y la ingesta habitual
  cuenta cuatro al dividir separadores Unicode mediante `splitlines()`.

No se presenta la suite general como aprobada. `scripts/run_demo.py` y
`src/ingestion/ingest.py` conservan su contenido original. La regresión del
preset habitual de 180 lecturas sí se comprobó con cero rechazos, junto con las
pruebas específicas del benchmark y las cuatro pruebas de `tests/test_pipeline.py`.

La pasada final `python -m pytest tests/test_benchmark.py tests/test_pipeline.py -q`
terminó con **23 aprobadas en 78,77 s** (19 del benchmark y cuatro del pipeline).
La compilación de los módulos pasó; la inspección estática del paquete de benchmark
no encontró llamadas a `collect`, `toPandas`, `toArrow` ni `read_text`.
