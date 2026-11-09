# Estadísticas y clasificación de riesgo (PR #3)

El análisis es batch y consume exclusivamente `QualityResult.valid` del PR #2.
No modifica lecturas, rechazos, deduplicación ni métricas de calidad. Una anomalía
finita puede ser una lectura válida. Las funciones de agregación y clasificación
suponen esta entrada validada; no son validadores alternativos para DataFrames
arbitrarios con nulos, NaN o claves duplicadas.

## Estadísticas y período evaluado

`machine_statistics` contiene una fila por máquina presente en el lote, el primer
y último timestamp, el número de lecturas y promedio, mínimo y máximo de cada
sensor. `sensor_metrics_5m` contiene los mismos estadísticos por máquina y ventana
fija de cinco minutos. Las ventanas están alineadas a UTC y son `[inicio, fin)`:
una lectura a las 12:05 pertenece a 12:05–12:10, no a 12:00–12:05.

El semáforo se calcula con los **máximos de la última ventana con datos de cada
máquina**. No se usa el peor valor del histórico ni el promedio para clasificar.
Los máximos de distintos sensores pueden corresponder a instantes diferentes:
el resultado describe la ventana y no implica que los picos fueran simultáneos.
Una máquina puede volver a VERDE en una ventana posterior. No hay histéresis,
evaluación de tendencias, detección de persistencia ni relleno de ventanas vacías.

`calculated_at` es un instante de ejecución que el llamador proporciona
explícitamente, con zona horaria; se normaliza a UTC. Nunca se sustituye por la
hora de la última lectura. `window_start` y `window_end` se conservan por separado.
El CLI utiliza la sesión UTC existente. Spark almacena instantes; al convertir
filas a objetos Python, su representación puede depender de la zona del host.

## Reglas académicas configurables

| Máximo de la ventana | AMARILLO desde | ROJO desde | Unidad |
| --- | ---: | ---: | --- |
| Temperatura | 89,5 | 94 | °C |
| Vibración | 6 | 6,8 | mm/s |
| Presión alta | 8 | 8,6 | bar |

Todas las comparaciones son inclusivas (`>=`). Cada sensor activa su nivel más
alto y la máquina toma el máximo: ROJO > AMARILLO > VERDE. Los motivos enumeran
todos los sensores activados, su máximo observado, nivel y umbral; no se ocultan
los motivos amarillos cuando otro sensor activa rojo.

Estos valores son decisiones académicas provisionales aprobadas por el equipo,
basadas en `src/generator/generate.py`: la degradación comienza en 89,5 °C,
6 mm/s y 8 bar; los picos comienzan en 94 °C, 6,8 mm/s y 8,6 bar. Leo no define
estos colores: la correspondencia entre valores y severidad es nuestra regla.
No son límites industriales certificados, probabilidades ni predicciones de
fallas. No se usa Machine Learning ni `is_anomaly`/`anomaly_type` como entrada.
Las etiquetas sintéticas tampoco garantizan coincidencia perfecta con el semáforo.

`config/risk_thresholds.example.json` contiene la configuración completa. Puede
copiarse y pasarse con `--thresholds`. Sin esa opción se usan los mismos valores
aprobados. Se exigen exactamente los tres sensores, cada uno con `yellow` y `red`,
números finitos y `yellow < red`; errores, booleanos y claves desconocidas fallan
explícitamente. No se agrega una dependencia para interpretar YAML.

## Presión baja y ausencia de datos

La presión baja **no se evalúa**: `valid` conserva solo cinco campos y no incluye
`machine_state`. Leo simula presión normal cercana a 1 bar cuando la máquina está
detenida, además de anomalías de presión entre 1,1 y 2,2 bar. No se infiere el
estado operativo a partir de un umbral improvisado. VERDE significa únicamente
que no se activaron las reglas altas evaluadas, no que la máquina esté sana.

Un lote vacío o completamente rechazado produce DataFrames vacíos con los mismos
esquemas que uno poblado. No se inventan máquinas, ventanas ni estados VERDE.
El catálogo no se usa para convertir ausencia de lecturas en normalidad. Una
última ventana antigua sigue siendo la última disponible: no se calcula frescura.

## Compatibilidad y límites de integración

Se conservan las columnas de Gastón: `avg_temp`, `max_vibration`, `avg_pressure`,
`n_readings`, `machine_id`, `window_start`; y para riesgo `machine_id`,
`calculated_at`, `risk_level`, `risk_reason`. Los demás estadísticos y límites de
ventana permanecen en los DataFrames aunque su cargador SQL actual no los use.
Las mediciones agregadas son double, el conteo long y los tiempos timestamp.

El CLI muestra los resultados y no conecta bases ni exporta archivos. La API
devuelve los DataFrames para su futura escritura a Parquet. El cargador de Gastón
recibe rutas CSV/Parquet, no DataFrames Spark directamente.

Los lotes son independientes. No se combinan ventanas parciales entre archivos,
no se deduplica entre lotes y no se conserva estado global. En el PR #4 habrá que
resolver la recomposición de ventanas y evitar que una carga antigua sobrescriba
un riesgo más reciente. También habrá que acordar persistir los estadísticos
adicionales y los límites de ventana. El histórico de Leo usa otra resolución de
duplicados; este análisis recibe JSONL original a través del PR #2.

## Ejecución local

Desde la raíz del repositorio, con el entorno virtual instalado:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
$calculatedAt = [DateTime]::UtcNow.ToString("o")
.\.venv\Scripts\python.exe -m src.processing.analysis --input data/sample/telemetry_sample.jsonl --calculated-at $calculatedAt --thresholds config/risk_thresholds.example.json
```

Para usar los resultados desde Python:

```python
from datetime import datetime, timezone
from src.processing.analysis import analyze_telemetry
from src.processing.risk_rules import load_thresholds
from src.processing.spark import create_spark_session

spark = create_spark_session()
try:
    result = analyze_telemetry(
        spark, "data/sample/telemetry_sample.jsonl",
        calculated_at=datetime.now(timezone.utc),
        thresholds=load_thresholds("config/risk_thresholds.example.json"),
    )
    result.machine_statistics.show()
    result.sensor_metrics_5m.show()
    result.machine_risk_latest.show(truncate=False)
    print(result.quality.metrics)
finally:
    spark.stop()
```

Pruebas del PR #3 (cada comando debe terminar en `OK`):

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_statistics.py" -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_risk_rules.py" -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_analysis.py" -v
```

Las suites `test_processing.py`, `test_quality.py` y `test_generator.py` se ejecutan
igual para regresión. Las pruebas de Spark necesitan Java funcional; un fallo de
arranque no cuenta como éxito. `test_storage.py` requiere pytest, dependencias
del cargador y PostgreSQL/MongoDB activos; unittest no ejecuta su función de test.
