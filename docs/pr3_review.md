# Revisión del PR #3: estadísticas y riesgo por reglas

Rama: `codex/pr3-statistics-risk`, creada desde `main` actualizado en `c436f76`.

Las lecturas válidas del PR #2 ahora pueden resumirse por máquina y por ventanas
UTC de cinco minutos. El riesgo de cada máquina corresponde a los máximos de su
última ventana disponible: un pico antiguo no mantiene el riesgo actual en rojo.
Se conservan el resultado de calidad y tres DataFrames separados. No se cambian
los módulos anteriores, el generador, la ingesta, el cargador ni las tablas SQL.

## Archivos

| Archivo | Cambio |
| --- | --- |
| `src/processing/statistics.py` | Nuevo: conteo y nueve estadísticos por máquina y ventana |
| `src/processing/risk_rules.py` | Nuevo: configuración validada, prioridad y motivos por sensor |
| `src/processing/analysis.py` | Nuevo: API `AnalysisResult` y CLI del análisis completo |
| `config/risk_thresholds.example.json` | Nuevo: valores académicos aprobados |
| `tests/test_statistics.py` | Nuevo: 2 pruebas con Spark real |
| `tests/test_risk_rules.py` | Nuevo: 3 pruebas Spark y 3 de configuración/fecha |
| `tests/test_analysis.py` | Nuevo: 2 pruebas de integración con calidad |
| `docs/risk_rules.md` | Nuevo: reglas, contrato, ejecución, limitaciones |
| `docs/pr3_review.md` | Nuevo: resumen de revisión y evidencia |
| `docs/data_contract.md` | Actualiza productor real y salidas del análisis |
| `README.md` | Actualiza alcance, comando y distinción unittest/pytest |

## Validación realizada el 9 de noviembre de 2026

Entorno: Windows, Python 3.12.10, PySpark 4.0.1 y JDK 21.0.9 seleccionado
explícitamente mediante `JAVA_HOME`. Las ejecuciones Spark necesitaron permisos
fuera del sandbox para lanzar sus procesos locales.

| Suite | Resultado final |
| --- | --- |
| `test_processing.py` | 8 aprobadas |
| `test_quality.py` | 13 aprobadas |
| `test_statistics.py` | 2 aprobadas |
| `test_risk_rules.py` | 6 aprobadas |
| `test_analysis.py` | 2 aprobadas |
| `test_generator.py` | 10 aprobadas en la repetición |
| `test_storage.py` | 1 prueba descubierta con pytest; no ejecutada porque escribe en PostgreSQL/MongoDB |

Son **41 pruebas aprobadas entre las ejecuciones finales de cada suite**. No se
presenta la primera ejecución conjunta como exitosa: terminó con 39 aprobadas y
2 errores de escritura Parquet en pruebas de Leo. Faltaban dependencias del
repositorio en `.venv`; se instaló `requirements.txt` sin modificarlo, y después
las 10 pruebas de Leo finalizaron en `OK`.

Antes de esa ejecución, las pruebas nuevas que creaban DataFrames desde listas
Python fallaron por cierre inesperado del worker Python de Spark. Las fixtures
se cambiaron a JSONL temporales leídos por Spark, coherentes con la entrada real
del proyecto. No se sustituyó Spark por mocks ni se omitieron esos casos.

También aprobaron `compileall`, `git diff --check`, ayuda del CLI y el análisis
completo sobre la muestra de Leo. Los logs locales están en `artifacts/` (ignorado
por Git): `pr3-tests.log`, `pr3-generator-tests.log`, `pr3-example-output.log` y
`pr3-dependencies.log`. Las trazas de rechazo intencional del lector estricto
forman parte de pruebas aprobadas; no deben confundirse con un fallo de la suite.

## Ejemplo real de salida

Muestra `data/sample/telemetry_sample.jsonl`:

```text
QualityMetrics(received=120, valid=120, rejected=0, duplicates=0)
```

Estadísticas de MAQ-001 sobre el lote (promedios redondeados aquí para lectura):

| Sensor | Promedio | Mínimo | Máximo |
| --- | ---: | ---: | ---: |
| Temperatura °C | 70,5967 | 50,86 | 101,46 |
| Vibración mm/s | 3,3075 | 0,99 | 9,55 |
| Presión bar | 5,7383 | 3,5 | 9,63 |

Son 24 lecturas entre 00:00 y 23:00 UTC del 25/10/2026. El DataFrame conserva
la precisión double; este redondeo es solo de presentación.

Las cinco máquinas terminan VERDE para su última ventana
`[2026-10-25 23:00:00Z, 2026-10-25 23:05:00Z)`. El `calculated_at` de esta
ejecución fue `2026-11-09T23:01:08.700918Z`. Esto es esperable aunque existan
anomalías anteriores: se evalúa la última ventana y no los máximos del lote.

El motivo verde fue: `Sin umbrales altos activados; presión baja no evaluada.`
Las pruebas verifican también amarillo, rojo y motivos combinados: por ejemplo,
temperatura 89,5 y vibración 6,8 activan AMARILLO y ROJO respectivamente; el nivel
final es ROJO y el motivo conserva ambos sensores.

## Para revisar antes de fusionar

- Comparaciones inclusivas y prioridad ROJO > AMARILLO > VERDE.
- Temperatura 89,5/94 °C; vibración 6/6,8 mm/s; presión alta 8/8,6 bar.
- Ventanas `[inicio, fin)`; última ventana por máquina, no última ventana global.
- `calculated_at` explícito, independiente de `window_start` y `window_end`.
- Vacíos mantienen esquema y producen cero filas; rechazados/duplicados no cuentan.
- Se documenta presión baja deshabilitada, ausencia de control de frescura y de ML.
- DataFrames compatibles con columnas consumidas por Gastón; sin carga a bases.
- Reconciliación entre lotes, exportación, persistencia de columnas adicionales y
  protección contra sobrescritura por lotes antiguos quedan para el PR #4.

El comando completo y la API Python están en [reglas de riesgo](risk_rules.md).
