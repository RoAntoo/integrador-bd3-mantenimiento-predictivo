# Integrador BD3: mantenimiento predictivo

Proyecto grupal de monitoreo industrial y mantenimiento predictivo. El sistema
previsto combina generación de sensores, procesamiento con PySpark, persistencia
y visualización.

## Alcance implementado

Spark se ejecuta localmente y lee un archivo JSONL con esquema explícito. El
lector estricto del primer PR detiene el lote ante errores y muestra los datos.
El flujo de calidad separa lecturas válidas y rechazadas, conserva la línea
original y el motivo de cada rechazo, resuelve duplicados por máquina e instante
y calcula métricas del lote. Incluye una muestra de cinco lecturas y pruebas con
Spark real.

El PR #3 agrega estadísticas por máquina y ventanas UTC de cinco minutos, y un
semáforo configurable sobre máximos de la última ventana disponible. Es una
clasificación académica por reglas, no una predicción de fallas. Conserva los
DataFrames compatibles con el cargador de Gastón sin conectar bases.

Leo ya aporta generación, catálogo e ingesta JSONL/Parquet; Gastón aporta
infraestructura, cargadores y dashboard. El pipeline del PR #4 enlaza estos
módulos y puede exportar sin servicios o cargar los resultados en Compose.

## Ejecutar el pipeline (PR #4)

Para pruebas de volumen, usar el **modo separado de benchmark** descrito en
[docs/benchmark.md](docs/benchmark.md). Permite configurar máquinas, lecturas por
máquina, semilla y salida, procesa Parquet con PySpark sin cargar bases, y registra
conteos, tiempos y tamaños en JSON/CSV. Los escenarios llegan hasta 15.000.000;
la demo de esta sección conserva 180 lecturas.

Para visualizar una corrida de benchmark ya terminada, consultar
[publicación de resúmenes y capturas en Grafana](docs/benchmark_grafana.md).
Incluye la evidencia de la corrida de 15 millones ejecutada por la usuaria y
permite llevar solo un resumen pequeño a otra computadora, sin repetir Spark.

Desde la raíz, creá el entorno virtual con Python 3.12, configurá el JDK 17 o
21 e instalá `requirements.txt` según las instrucciones de instalación más abajo.
En Windows, con el Python de ese entorno:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_demo
```

El preset genera tres máquinas durante una hora, con semilla 42 e inicio UTC
fijo. Para procesar otro lote de Leo usá `--input ruta/al/lote.jsonl`. También
podés pasar `--seed`, `--start-time`, `--thresholds` o `--work-dir`. La
consola muestra la ruta `artifacts/pipeline/<batch_id>` y los conteos de ingesta
y de calidad. `balance` concilia recibidos, rechazos de ingesta, rechazos de
Spark y válidos. Los motivos de ingesta están en `results/ingestion_rejected.jsonl`
y los de Spark en `results/rejected_telemetry.parquet`, con línea original y número.
El pipeline acepta nombres cortos o con unidades y rechaza variantes contradictorias.
El directorio contiene el JSONL original, `lake/` con el raw y el
Parquet histórico de Leo, y `results/` con Parquet curado, rechazos, estadísticas,
métricas, riesgo y alertas JSONL.

La versión del contrato forma parte del identificador desde el PR #5. Reprocesar
una entrada de una versión anterior genera otro lote. `--load-only` permite
reintentar sus exportaciones existentes.

Si el preset ya está cargado, cambiar la semilla (`--seed`) o los umbrales
(`--thresholds`) genera otro identificador de lote. PostgreSQL rechaza la carga
si alguna ventana ya pertenece al lote original. Para probar la variante, usá
`--start-time` con un horario que no se solape o bases separadas de PostgreSQL
y MongoDB.

Para cargar y visualizar, iniciá Docker Desktop, copiá `.env.example` a `.env`
y completá `POSTGRES_PASSWORD`, `MONGO_INITDB_ROOT_PASSWORD` y `GRAFANA_PASSWORD`
con contraseñas locales. Ejecutá:

```powershell
docker compose --env-file .env -f infra/docker-compose.yml up -d --wait
.\.venv\Scripts\python.exe -m scripts.run_demo --load
```

En Linux o macOS reemplazá `.\.venv\Scripts\python.exe` por `.venv/bin/python`.
Usá los puertos del `.env` y el usuario de Grafana allí configurado. Abrí el
dashboard `bd3-machines-dashboard` en `http://localhost:3000`. Para el preset,
elegí un rango absoluto UTC del 25/10/2026 de 00:00 a 01:00 y filtrá una máquina.
Las tendencias respetan ese rango. El ranking muestra el último estado disponible,
con `calculated_at` y `measured_window_start`, independientemente del rango elegido.
La segunda fecha indica el inicio de la ventana de medición de cinco minutos.

Si PostgreSQL confirmó la carga y MongoDB falló, corregí la conexión y retomá
desde el directorio impreso. En Windows:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_demo --load-only "<directorio-del-lote>"
```

En Linux o macOS:

```sh
.venv/bin/python -m scripts.run_demo --load-only "<directorio-del-lote>"
```

El comando repite los upserts con los mismos artefactos. Las métricas y las
alertas conservan sus claves, y una ventana antigua no reemplaza el riesgo de
una ventana posterior. Para repetir la demostración, podés volver a ejecutar
`--load` con los mismos parámetros y comparar los conteos en ambas bases.

En un volumen PostgreSQL existente, ejecutá la migración antes de usar el
cargador actualizado. El comando siguiente usa el usuario y la base de ejemplo:

```powershell
docker compose --env-file .env -f infra/docker-compose.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U admin_bd3 -d mantenimiento_predictivo -f /docker-entrypoint-initdb.d/002_pipeline_integration.sql
```

Para la demo usá una base sin los CSV de ejemplo. Para cargarlos en una base de
fixtures, ejecutá `.\.venv\Scripts\python.exe -m src.storage.load_results` en
Windows o `.venv/bin/python -m src.storage.load_results` en Linux o macOS. Una
métrica de fixture carece de propietario y su riesgo carece de ventana de origen.
La carga integrada rechaza estas colisiones y conserva los datos existentes.

### Verificación y defensa

Podés probar la exportación local sin Compose. Para la prueba de integración,
levantá Compose y configurá el entorno. En Windows:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_pipeline.py
$env:BD3_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest tests/test_pipeline_integration.py
```

En Linux o macOS:

```sh
.venv/bin/python -m pytest tests/test_pipeline.py
BD3_INTEGRATION=1 .venv/bin/python -m pytest tests/test_pipeline_integration.py
```

La prueba de integración consulta ambos motores y ejecuta las consultas de los
paneles mediante la API de Grafana.

En la defensa, Leo puede mostrar un evento del JSONL y su partición histórica.
Anto sigue la máquina en los conteos y Parquet de Spark. Gastón compara el
semáforo y las series del dashboard con PostgreSQL y recupera las alertas en
MongoDB. Para demostrar un ROJO histórico con último estado VERDE, ejecutá el
comando con `--input tests/fixtures/pipeline_sample.jsonl --load` y filtrá `PR4-A`
el 25/10/2026 de 12:00 a 12:15 UTC. Consultas de control:

```sql
SELECT machine_id, window_start, risk_level, calculated_at
FROM machine_risk_latest WHERE machine_id = 'PR4-A';
SELECT window_start, n_readings, max_vibration
FROM sensor_metrics_5m WHERE machine_id = 'PR4-A' ORDER BY window_start;
```

```javascript
db.getSiblingDB('mantenimiento_predictivo').alerts.find({machine_id: 'PR4-A'}).sort({timestamp: 1})
```

## Ejecutar el análisis completo (PR #3)

Con el entorno virtual instalado, desde la raíz en PowerShell:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
$calculatedAt = [DateTime]::UtcNow.ToString("o")
.\.venv\Scripts\python.exe -m src.processing.analysis --input data/sample/telemetry_sample.jsonl --calculated-at $calculatedAt --thresholds config/risk_thresholds.example.json
```

Muestra métricas de calidad y hasta 20 filas de cada DataFrame: estadísticas del
lote, estadísticas por ventana y último riesgo por máquina. No escribe en bases.
Los esquemas, API Python, configuración, pruebas y limitaciones (incluida presión
baja deshabilitada) están en [reglas de riesgo](docs/risk_rules.md).

## Requisitos

- Python 3.12, versión de referencia para reproducir esta entrega.
- JDK 17 o 21. Seleccionar su carpeta mediante `JAVA_HOME`.
- PySpark 4.0.1, fijado en `requirements.txt`.

La documentación de [Spark 4.0.1](https://spark.apache.org/docs/4.0.1/index.html)
indica soporte para Java 17/21 y Python 3.9+. Para este proyecto se toma Python
3.12 como referencia. Instalar PySpark con `pip` proporciona Spark y sus
dependencias necesarias; no hace falta instalar un clúster Hadoop.

## Instalación en Windows (PowerShell)

Ejecutar desde la raíz del repositorio. Ajustar la ruta del JDK a la instalación
local; el ejemplo selecciona un JDK 21 ya instalado. Las variables siguientes
solo se aplican a la terminal actual.

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
python --version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Se usa directamente el Python del entorno virtual, por lo que no es necesario
activar scripts ni modificar la política de ejecución de PowerShell. Si el comando
`python` apunta a otra versión, usar `py -3.12 -m venv .venv`.
Si se abre otra terminal, volver a seleccionar `JAVA_HOME` y `Path`.

En Linux/macOS, seleccionar un JDK 17/21 y usar:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m src.processing.etl --input tests/fixtures/processing_sample.jsonl
.venv/bin/python -m src.processing.quality --input tests/fixtures/processing_sample.jsonl
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

## Ejecutar el lector estricto

En PowerShell, desde la raíz:

```powershell
.\.venv\Scripts\python.exe -m src.processing.etl --input tests/fixtures/processing_sample.jsonl
```

La salida muestra los campos `machine_id`, `timestamp`, `temperature`, `vibration`
y `pressure`, sus tipos y los cinco registros de la muestra. Finaliza con:

```text
Cantidad de registros: 5
```

Los registros se muestran sin un orden garantizado. Las fechas se muestran en UTC.
Spark se detiene al terminar, incluso si falla la lectura. Un archivo inexistente
o incompatible produce un mensaje de error y código de salida 1.
Spark también puede imprimir advertencias de arranque de la JVM o de Hadoop;
comprobar el resultado y el código de salida para distinguirlas de un fallo.

Para leer otra muestra local, cambiar el argumento `--input` por su ruta. Si contiene
espacios, encerrarla entre comillas. El archivo debe cumplir el
[contrato provisional](docs/data_contract.md).

## Ejecutar el control de calidad

```powershell
.\.venv\Scripts\python.exe -m src.processing.quality --input tests/fixtures/processing_sample.jsonl
```

La muestra incluida devuelve `Recibidos: 5`, `Válidos: 5`, `Rechazados: 0` y
`Duplicados: 0`. La función `clean_telemetry(spark, input_path)` devuelve dos
DataFrames (`valid` y `rejected`) y `metrics`. `rejected` incluye `raw_line` y
`rejection_reasons`; el comando muestra los contadores. La igualdad
`received = valid + rejected` se cumple para cada lote. Las reglas, motivos y
decisiones sobre duplicados están en [limpieza y calidad](docs/quality_rules.md).

## Ejecutar las pruebas

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Las pruebas usan `unittest` de Python y Spark local. Verifican ambos lectores,
incluidos campos ausentes o nulos, JSON y fechas inválidos, duplicados idénticos,
claves con mediciones en conflicto, valores no finitos, métricas, entrada vacía y
archivo inexistente. Los errores del lector estricto pueden imprimir trazas de
Spark esperadas por las pruebas; el resultado final debe indicar `OK`.

Las pruebas nuevas `test_statistics.py`, `test_risk_rules.py` y `test_analysis.py`
cubren estadísticas, fronteras inclusivas, prioridades, ventanas, última ventana,
configuración y entradas vacías/rechazadas. Para ejecutar solo una suite, sustituir
`test_*.py` por su nombre. El test de almacenamiento de Gastón es una función de
pytest y **no se ejecuta con unittest**. Con sus dependencias y las bases activas,
ejecutá `.\.venv\Scripts\python.exe -m pytest tests/test_storage.py -v` en Windows
o `.venv/bin/python -m pytest tests/test_storage.py -v` en Linux o macOS.

## Organización

```text
requirements.txt                         Versión de PySpark
docs/data_contract.md                    Entrada provisional y acuerdos pendientes
docs/quality_rules.md                    Reglas y métricas de calidad
src/processing/spark.py                  Sesión local de Spark, dos hilos y UTC
src/processing/schema.py                 Campos y tipos explícitos
src/processing/validation.py             Reglas de lectura compartidas
src/processing/etl.py                    Lector estricto y ejecución por consola
src/processing/quality.py                Limpieza, rechazos y métricas
tests/fixtures/processing_sample.jsonl   Muestra independiente del generador
tests/test_processing.py                 Pruebas del lector estricto
tests/test_quality.py                    Pruebas del flujo de calidad
```

La muestra está en `tests/fixtures/` para que Leo pueda desarrollar el generador y
su dataset compartido sin editar el mismo archivo. Su formato definitivo se
acordará con el equipo; no se asume que estos nombres sean la interfaz final.
