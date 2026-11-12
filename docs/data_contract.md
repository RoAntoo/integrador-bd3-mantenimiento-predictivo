# Contrato del módulo de procesamiento

## Intercambio del pipeline (PR #4)

El pipeline acepta `temperature`/`temperature_c`, `vibration`/`vibration_mm_s`
y `pressure`/`pressure_bar`. La ingesta centraliza la validación y normalización.
Cada variante presente debe ser un número finito, excluyendo booleanos, cadenas
y nulos. Si aparecen ambas, deben ser numéricamente iguales (`60` equivale a
`60.0`), sin tolerancia ni preferencia por una variante. Un conflicto rechaza la
línea con `CONFLICTING_MEASUREMENT`.

La ingesta entrega un JSONL normalizado con nombres cortos, timestamp UTC con
milisegundos, `source_raw_line` y `source_line_number` (base 1). Conserva todas
las lecturas aceptadas, incluidos duplicados. El texto original excluye solamente
el separador de línea. Spark consume este archivo mediante `normalized_input=True`.
El lector estricto y el análisis directo mantienen su contrato histórico de cinco
campos cortos. Para admitir ambas variantes se utiliza el pipeline integrado.

El lote conserva `telemetry.jsonl` original y dos auditorías separadas.
`results/ingestion_rejected.jsonl` contiene `batch_id`, `line_number`, `reason` y `raw_line`.
`results/rejected_telemetry.parquet` contiene `batch_id`, `line_number`, `raw_line`, los cinco
campos parseados y `rejection_reasons`. El rechazo de ingesta no vuelve a entrar
a Spark. Los duplicados ya están incluidos en los rechazos de Spark.
El pipeline propaga la identidad del manifiesto a ambas auditorías. En ingesta,
los archivos se separan bajo `audit/<SHA256(batch_id)>/`; repetir el mismo lote
los reemplaza atómicamente, sin cambiar los nombres raw y Parquet.

`batch.json.balance` verifica la igualdad entre `received` y la suma de
`ingestion_rejected`, `spark_rejected` y `valid`. También exige que los aceptados
de ingesta coincidan con los recibidos por Spark. Un lote vacío o totalmente
rechazado en modo permisivo publica auditoría y resultados vacíos. El modo
estricto de la ingesta escribe su auditoría y luego falla ante rechazos.

El Parquet curado contiene los cinco campos de calidad y `data_quality_flag`.
El histórico de ingesta conserva campos adicionales y resuelve claves duplicadas
reteniendo la última lectura procesada. Spark conserva una copia de duplicados
idénticos y rechaza todas las lecturas de una clave con mediciones contradictorias.
Por eso el histórico y el curado pueden tener distintos conteos y valores. El
curado es la fuente de métricas y riesgo.

La identidad del lote incluye entrada, umbrales y `pipeline_version=6`. Esto evita
reutilizar resultados anteriores a la normalización y a la propagación del
identificador del manifiesto a los rechazos. `--load-only` sigue aceptando
los manifiestos anteriores. Regenerar una entrada antigua crea otro identificador.
Si sus ventanas ya están publicadas, PostgreSQL rechaza la nueva carga y conserva
los datos existentes. Para comparar versiones, usar una base de pruebas separada.

| Archivo | Contrato |
| --- | --- |
| `sensor_metrics_5m.parquet` | `machine_id`, `window_start`, `avg_temp`, `max_vibration`, `avg_pressure`, `n_readings`. SQL almacena las mediciones con dos decimales y el conteo como `INTEGER`. |
| `machine_risk_latest.parquet` | `machine_id`, `window_start`, `calculated_at`, `risk_level`, `risk_reason`. El estado más reciente se decide por `window_start`, con `calculated_at` como fecha de auditoría. |
| `alerts.jsonl` | UTF-8, un objeto por ventana AMARILLA o ROJA con `machine_id`, `timestamp`, `level`, `description`. `timestamp` es el inicio UTC en `YYYY-MM-DDTHH:MM:SSZ`. MongoDB conserva esa clave como texto para mantener compatibilidad con sus fixtures. |
| `machine_statistics.parquet` | Estadísticas completas del lote por máquina. Archivo local de auditoría. |

Los timestamps Parquet tienen zona UTC. El riesgo usa los máximos originales
de Spark y los mismos umbrales en el semáforo y en el histórico. El exportador
local materializa un DataFrame por vez en memoria para escribir Parquet con
PyArrow. Está pensado para los lotes de la demo.

Cada clave `(machine_id, window_start)` publicable pertenece a un solo lote,
identificado por el SHA-256 de entrada, configuración de umbrales y versión del pipeline. Se
aceptan ventanas con pocas lecturas como cerradas. Si otro lote aporta datos a
la misma clave, PostgreSQL revierte toda su carga. La combinación de ventanas
parciales y las correcciones de ventanas publicadas quedan fuera del MVP.
Conservá `batch.json` y sus resultados juntos para el reintento. PostgreSQL y
MongoDB confirman por separado. Si falla MongoDB, `--load-only` vuelve a cargar
el mismo conjunto de archivos.

## Integración confirmada en el PR #3

El generador de Leo ya entrega JSONL con los cinco campos de procesamiento
descritos abajo. `temperature` equivale a `temperature_c` (°C), `vibration` a
`vibration_mm_s` (mm/s) y `pressure` a `pressure_bar` (bar). Emite ambas variantes,
fechas UTC y campos adicionales: `rpm`, `power_kw`, `operating_hours`,
`machine_state`, `is_anomaly`, `anomaly_type`. El PR #2 mantiene únicamente los
cinco campos originales en `QualityResult.valid`; no cambia nombres ni incorpora
etiquetas sintéticas. Los acuerdos provisionales más abajo documentan el origen
del PR #1 y quedan actualizados por esta sección para el productor actual.

El análisis consume ese DataFrame y produce:

| Dataset | Columnas |
| --- | --- |
| `machine_statistics` | `machine_id`, `first_timestamp`, `last_timestamp`, `n_readings`, `avg_temp`, `min_temp`, `max_temp`, `avg_vibration`, `min_vibration`, `max_vibration`, `avg_pressure`, `min_pressure`, `max_pressure` |
| `sensor_metrics_5m` | `machine_id`, `window_start`, `window_end`, `n_readings` y los mismos nueve estadísticos |
| `machine_risk_latest` | `machine_id`, `window_start`, `window_end`, `calculated_at`, `risk_level`, `risk_reason` |

Identificadores, niveles y motivos son string; tiempos son timestamp; estadísticos
double; `n_readings` long. El esquema se conserva sin datos y el conteo solo
incluye registros válidos deduplicados. Las columnas mínimas del cargador de
Gastón están presentes; no se conecta almacenamiento en el PR #3. Sus tablas
actuales no conservan todos los campos adicionales. Ver [reglas de riesgo](risk_rules.md).

## Contexto del contrato inicial

Este documento describe la muestra independiente de Anto introducida en el primer PR.
Es una base de conversación con Leo, responsable de generar los sensores.
Los nombres, unidades y reglas aquí indicados son provisionales: no representan
una interfaz definitiva ni el contrato A completo del plan de trabajo.

## Formato de la muestra

- Archivo local JSONL, codificado en UTF-8.
- Un objeto JSON por línea; sin un arreglo que envuelva los eventos.
- Cada objeto representa una lectura de una máquina en un instante.
- Las mediciones se escriben como números JSON, no como textos con unidades.
- Las fechas incluyen zona horaria. La muestra usa UTC con el sufijo `Z`.
- Se admiten segundos y, opcionalmente, tres dígitos de milisegundos.

| Campo de esta muestra | Descripción | Tipo en Spark | Convención provisional |
| --- | --- | --- | --- |
| `machine_id` | Identificador de la máquina que produce la lectura. | `string` | Ejemplo: `MAQ-001`. |
| `timestamp` | Fecha y hora de la medición, no de la carga del archivo. | `timestamp` | ISO 8601; ejemplo: `2026-10-25T12:00:00Z`. |
| `temperature` | Temperatura registrada por el sensor. | `double` | Grados Celsius. |
| `vibration` | Nivel de vibración registrado por el sensor. | `double` | Milímetros por segundo. |
| `pressure` | Presión registrada por el sensor. | `double` | Bar. |

Ejemplo de un evento:

```json
{"machine_id":"MAQ-001","timestamp":"2026-10-25T12:00:00Z","temperature":65.5,"vibration":2.1,"pressure":5.0}
```

`tests/fixtures/processing_sample.jsonl` contiene cinco lecturas ficticias de dos
máquinas. Sus valores sirven para probar lectura y tipos; no definen rangos de
operación industrial ni niveles de riesgo.

## Qué valida el lector estricto del primer PR

El lector utiliza un esquema explícito y modo `FAILFAST`. Rechaza JSON mal
formado, mediciones incompatibles con el tipo numérico y fechas que no puedan
interpretarse con el formato indicado. También verifica que ninguno de los cinco
campos obligatorios resulte nulo; esto detecta campos ausentes y nulos explícitos.

Spark declara las columnas leídas como anulables, por eso no se depende de
`nullable=False` para validar los datos. El lector ejecuta una acción de Spark
antes de devolver el DataFrame para que los errores no queden pendientes por
su evaluación diferida.

Es una validación de lectura, no un validador estricto de todos los tipos JSON:
Spark puede convertir algunos valores al tipo declarado, por ejemplo un
identificador numérico a texto. El productor debería enviar `machine_id` como
texto. Este lector no comprueba el catálogo, identificadores vacíos, rangos físicos
ni valores numéricos especiales como NaN o infinito; el flujo de calidad
posterior sí rechaza identificadores vacíos y mediciones no finitas.

Los campos adicionales no se incorporan al DataFrame. Los duplicados se conservan.
Un archivo vacío es válido y produce cero registros. Los desfases horarios
explícitos se interpretan y se muestran en UTC; la convención propuesta para
el generador es emitir directamente UTC.

Ante errores, este lector interrumpe el lote; no elimina ni corrige filas en
silencio. Para separar filas válidas y rechazadas, conservar las líneas originales
y obtener métricas se usa el [flujo de calidad](quality_rules.md), que mantiene
el mismo esquema y las mismas opciones de parseo.

## Acuerdos pendientes con el generador de Leo

| Tema | Acuerdo por definir |
| --- | --- |
| Nombres y unidades | Confirmar nombres definitivos y sus unidades. El plan propone `temperature_c`, `vibration_mm_s` y `pressure_bar`; esta muestra usa los nombres cortos del alcance inicial. No se realizan renombrados automáticos. |
| Campos disponibles | Identificar cuáles estarán siempre presentes y cuáles serán opcionales. El esquema inicial solo cubre estos cinco campos. |
| Fechas | Confirmar UTC, precisión temporal y formato de serialización. |
| Identificadores | Acordar cómo se identifica cada máquina y cómo se comparte su catálogo. |
| Entrega | Confirmar formato, ubicación y organización de archivos por lote. En este PR se recibe un único archivo local por `--input`. |
| Calidad | Las reglas técnicas del segundo PR están documentadas en `quality_rules.md`. Sigue pendiente acordar con el generador cómo representará errores y anomalías intencionales; una lectura anómala puede ser válida. |
| Muestra compartida | Entregar una muestra pequeña y reproducible con cantidad conocida de eventos para probar la integración. |

Para probar otro archivo que cumpla este esquema basta con cambiar `--input`.
Si cambian nombres o tipos, habrá que ajustar el esquema, la muestra y las pruebas
en un PR acordado. El generador y su futura muestra compartida conservan su
responsabilidad propia; este módulo no los implementa.
