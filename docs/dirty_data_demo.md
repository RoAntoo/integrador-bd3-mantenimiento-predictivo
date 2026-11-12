# Demostración reproducible de calidad de datos

El contaminador `src.generator.contaminate` es independiente del simulador de Leo.
Se ejecuta después de generar o copiar el JSONL y antes de la ingesta. No se
modificó el generador: `anomaly_rate` sigue representando anomalías industriales.
Una temperatura extrema pero finita continúa siendo válida para calidad y puede
activar las reglas de riesgo. No se agregan dependencias.

## PowerShell: preparación

Desde la raíz del proyecto, con Python 3.12, las dependencias de `requirements.txt`
y JDK 17 o 21 instalados (ajustar `JAVA_HOME` a la instalación local):

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
New-Item -ItemType Directory -Force artifacts/dirty-demo/tmp | Out-Null
$env:TEMP = (Resolve-Path artifacts/dirty-demo/tmp).Path
$env:TMP = $env:TEMP
$env:TMPDIR = $env:TEMP
```

Las variables temporales evitan rutas virtualizadas incompatibles con los scripts
de arranque de Spark en algunos entornos Windows; se aplican a esta terminal.

## Demo normal y contaminada

```powershell
# Normal: la contaminación está desactivada por defecto.
.\.venv\Scripts\python.exe -m scripts.run_demo --work-dir artifacts/dirty-demo/normal

# Misma telemetría industrial, contaminación independiente del 10 %.
.\.venv\Scripts\python.exe -m scripts.run_demo --dirty-rate 0.10 --dirty-seed 42 --work-dir artifacts/dirty-demo/dirty

# Fixture de cobertura completa con resultado conocido.
.\.venv\Scripts\python.exe -m scripts.run_demo --input tests/fixtures/dirty_pipeline_sample.jsonl --work-dir artifacts/dirty-demo/fixture
```

El preset normal tiene 180 líneas: `180 = 0 + 0 + 180`. Con `dirty-rate=0.10`
y `dirty-seed=42`, se seleccionan 18 líneas, se modifican 14 y se rechazan 16:
`180 = 10 rechazos de ingesta + 6 de Spark + 164 válidas finales`.
El fixture de 10 líneas ya está contaminado; se procesa con tasa cero:
`10 = 5 rechazos de ingesta + 3 de Spark + 2 válidas finales`.
Una de sus válidas es una anomalía industrial y genera riesgo ROJO y una alerta.

La consola imprime la carpeta del lote y `batch.json`, que incluye
`contamination`, `ingestion`, `quality` y `reconciliation`. Repetir el mismo
comando reutiliza el lote completo sin duplicar datos. La identidad incluye
hashes de las entradas, configuración y versiones del pipeline/contaminador,
para evitar reutilizar exportaciones anteriores a estas reglas.

Para usar el contaminador por separado:

```powershell
.\.venv\Scripts\python.exe -m src.generator.generate --machines 3 --days 0.041666666666666664 --interval 1 --seed 42 --output artifacts/dirty-demo/original.jsonl
.\.venv\Scripts\python.exe -m src.generator.contaminate --input artifacts/dirty-demo/original.jsonl --output artifacts/dirty-demo/contaminated.jsonl --dirty-rate 0.10 --dirty-seed 42
.\.venv\Scripts\python.exe -m scripts.run_demo --input artifacts/dirty-demo/contaminated.jsonl --work-dir artifacts/dirty-demo/external
```

El comando independiente imprime el informe de contaminación; el pipeline solo
puede informar su propia selección, por lo que para conservar ese informe dentro
del lote se recomienda usar directamente `scripts.run_demo --dirty-rate ...`.

## Porcentaje, escenarios y reproducción

`dirty-rate` acepta valores finitos entre 0 y 1. Para N líneas selecciona
`floor(N * dirty_rate)` posiciones sin reemplazo. No cambia el número de líneas.
`dirty-seed` es un entero y usa un RNG propio; la semilla industrial `--seed`
no se consume ni modifica. Con la misma entrada, versión, tasa y semilla se
obtienen los mismos bytes, posiciones y métricas (los relojes de ejecución son
metadatos de auditoría, no parte del resultado reproducible).

Se asignan por turnos estos siete escenarios: obligatorio ausente, nulo, tipo
incompatible, fecha inválida, JSON malformado, duplicado idéntico y conflicto.
Los errores de medición eliminan o modifican ambos aliases cuando corresponde.
Los pares reservan dos posiciones seleccionadas; su primera línea queda como
ancla y la segunda se reemplaza por una copia idéntica o contradictoria. Los
escenarios usan posiciones disjuntas. Un último presupuesto de una línea recibe
un error individual. Con al menos nueve seleccionadas se cubren los siete tipos.
Si el presupuesto es menor se informa `uncovered_types`, sin prometer cobertura.

Los contadores tienen alcances diferentes:

- `selected_lines`: participantes de los escenarios, incluidas las anclas.
- `modified_lines`: líneas cuyo contenido cambió respecto de la entrada.
- `reconciliation.rejected_total`: filas rechazadas realmente por ambas etapas.
- `breakdown`: cantidad de escenarios de cada tipo; un escenario duplicado ocupa
  dos líneas. `scenarios` identifica posiciones seleccionadas y modificadas.

Con tasa cero se copia byte por byte, incluso una entrada ya sucia o vacía. Con
tasa positiva se exige telemetría válida y claves normalizadas únicas para evitar
interacciones accidentales con suciedad preexistente. El original nunca se
sobrescribe. Un archivo de entrada inválido en ese modo es un error de
configuración; las líneas intencionalmente contaminadas se tratan por registro.

## Evidencia y responsabilidades

Cada lote conserva:

- `telemetry_original.jsonl`: entrada anterior a la contaminación.
- `telemetry.jsonl`: líneas efectivamente recibidas por el pipeline.
- `contamination.json`: configuración, hashes y escenarios.
- `lake/raw/date=...`: líneas aceptadas por ingesta, todavía con duplicados.
- `lake/audit/*_accepted.jsonl`: referencia original y representación normalizada
  entregada a Spark, con `batch_id`, `line_number`, `raw_line`, `normalized_line`.
- `results/ingestion_rejected.jsonl` y su copia en `lake/audit/*_rejected.jsonl`:
  etapa `ingestion`, código, detalle y línea original.
- `results/rejected_telemetry.parquet`: etapa `spark`, motivos, campos parseados
  e identidad original. Las copias idénticas mantienen números de línea distintos.
- `results/clean_telemetry.parquet`: cinco campos curados y `data_quality_flag=VALID`.
- `lake/historical/telemetry/date=...`: histórico integrado de esas filas curadas.
- Estadísticas, métricas, riesgos y alertas calculados exclusivamente del curado.

`raw_line` conserva el contenido sin terminador de línea. Se reconocen CR, LF y
CRLF como terminadores; los separadores Unicode dentro del JSON no crean filas.
La ingesta informa el primer error de cada línea con códigos como
`MISSING_TEMPERATURE`, `NULL_VIBRATION`, `INVALID_TYPE_TEMPERATURE`,
`INVALID_TIMESTAMP` o `MALFORMED_JSON`. Spark aplica sus códigos de calidad
existentes, incluidos `DUPLICATE_KEY` y `CONFLICTING_DUPLICATE_KEY`.

La ingesta valida y normaliza estructura, tipos, fechas y aliases. Spark recibe
esa normalización junto con la referencia original y resuelve los duplicados
del lote. Sus controles defensivos permanecen disponibles para entradas directas.
En los idénticos se conserva una copia; en conflictos se rechaza toda la clave.
Se comprueban todas las fronteras antes de publicar el lote:

```text
recibidos = rechazados_ingesta + aceptados_ingesta
aceptados_ingesta = recibidos_Spark
recibidos_Spark = rechazados_Spark + válidos_finales
recibidos = rechazados_ingesta + rechazados_Spark + válidos_finales
```

`quality.duplicates` ya es parte de `quality.rejected`. No se suma nuevamente.
Una reconciliación fallida impide publicar el lote.

## Lotes vacíos y limitaciones

Un lote vacío o totalmente rechazado conserva su auditoría y exporta Parquet
vacío con esquema estable, más `alerts.jsonl` vacío. No genera filas ficticias ni
histórico curado. `load_batch` omite PostgreSQL/MongoDB cuando no hay válidos;
el cargador PostgreSQL también evita abrir conexión si ambas entradas están
vacías. Se conservan los riesgos y las métricas de lotes anteriores.

El flujo es batch local: el contaminador, la ingesta y la exportación materializan
datos en memoria. La numeración del lector directo de calidad usa el texto
completo de un único archivo local. No hay deduplicación de calidad entre lotes,
validación del catálogo ni detección estadística de valores físicamente dudosos.
Los campos adicionales conservan las reglas previas de Leo, incluidos algunos
valores predeterminados; no se amplió el contrato obligatorio a esos campos.
Los instantes del intercambio integrado se normalizan a UTC con milisegundos.
El histórico del comando independiente de ingesta conserva su comportamiento
previo de consolidación; para análisis y alertas se usa el curado del pipeline.

## Pruebas

```powershell
# Cobertura local de contaminación y pipeline (sin bases).
.\.venv\Scripts\python.exe -m pytest tests/test_contamination.py tests/test_dirty_pipeline.py -q --basetemp artifacts/dirty-demo/tests-local

# Suite completa con PostgreSQL, MongoDB y Grafana, usando el .env local.
docker compose --env-file .env -f infra/docker-compose.yml up -d --wait
$env:BD3_INTEGRATION = "1"
.\.venv\Scripts\python.exe -m pytest -q --basetemp artifacts/dirty-demo/tests-full
```

Las pruebas de servicios usan las credenciales de `.env`; la prueba de
almacenamiento existente carga sus fixtures de ejemplo. Las nuevas pruebas de
integración usan máquinas únicas y eliminan sus filas al terminar. La regresión
de persistencia comprueba que un lote totalmente rechazado conserve un riesgo
ROJO previamente cargado. Sin servicios se puede ejecutar toda la cobertura
local omitiendo los dos archivos que requieren bases:

```powershell
.\.venv\Scripts\python.exe -m pytest -q --ignore=tests/test_storage.py --ignore=tests/test_pipeline_integration.py --basetemp artifacts/dirty-demo/tests-without-services
```

## Validación realizada el 11/11/2026

Se ejecutaron las tres demostraciones de la CLI y se contrastaron los conteos
del manifiesto con los Parquet curados, de rechazos y de métricas exportados:

| Demostración | Recibidas | Rechazos ingesta | Rechazos Spark | Válidas finales |
| --- | ---: | ---: | ---: | ---: |
| Normal | 180 | 0 | 0 | 180 |
| Contaminación 10 %, semilla 42 | 180 | 10 | 6 | 164 |
| Fixture integrado | 10 | 5 | 3 | 2 |

La corrida contaminada seleccionó 18 líneas, modificó 14 y rechazó 16.
Los conflictos rechazan también las anclas originales; los duplicados idénticos
retienen una copia. El fixture produjo una alerta ROJA para `DIRTY-ANOMALY` y
conservó `DIRTY-COPY` como VERDE.

La suite completa, con `BD3_INTEGRATION=1` y PostgreSQL, MongoDB y Grafana
levantados, terminó con **80 pruebas y 41 subpruebas aprobadas, sin fallos**, en
282,89 segundos. Incluye las regresiones de Leo, Anto y Gastón y la preservación
del riesgo anterior ante un lote sin válidos. Comando final ejecutado:

```powershell
$env:BD3_INTEGRATION = "1"
.\.venv\Scripts\python.exe -u -m pytest -q --tb=short --basetemp artifacts/dirty-tests/full-3
```

Evidencia local: `artifacts/dirty-tests/full-3.log`, los tres lotes en
`artifacts/dirty-demo/{normal,dirty,fixture}/` y el resumen estructurado
`artifacts/dirty-demo/validation.json`. Estos artefactos son locales; los
comandos anteriores permiten reconstruirlos.
