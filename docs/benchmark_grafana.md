# Mostrar una corrida terminada en Grafana

El dashboard **BD3 | Benchmark de telemetría industrial** se abre en:

[Abrir el benchmark en Grafana local](http://localhost:3000/d/bd3-benchmark-dashboard?orgId=1&kiosk)

Para la captura, abrir el enlace en el navegador habitual, maximizar la ventana
y usar F11 si se desea ocultar la interfaz del navegador. El enlace incluye modo
kiosco y el dashboard guarda el rango absoluto de las mediciones sintéticas;
no seleccionar «últimas seis horas», porque las mediciones son de octubre.

## Qué representa

La corrida `20261113T014052-85e354d4a17648508bfcceca5642f855` fue ejecutada por
la usuaria y terminó correctamente: **15.000.000 lecturas válidas, 1.500 máquinas,
10.000 lecturas por máquina, cero rechazos y 857,664 segundos de tiempo total**.
Los conteos y los controles provienen de su `metrics.json` y se contrastaron con
los Parquet guardados, sin volver a ejecutar Spark ni leer el JSONL crudo.

- Las seis tarjetas muestran conteos, duración y tamaño Parquet de la corrida completa.
- Las barras muestran los tiempos reales por etapa.
- El semáforo resume el último riesgo de las **1.500 máquinas**: 1.310 VERDE,
  185 ROJO y 5 AMARILLO. Es clasificación por umbrales, no predicción de fallas.
- Las curvas muestran ventanas de cinco minutos de **tres ejemplos**: MAQ-001,
  MAQ-002 y MAQ-003. No representan las 1.500 curvas simultáneamente.

Texto sugerido para la diapositiva:

> Procesamiento batch de 15 millones de lecturas sintéticas de 1.500 máquinas
> con PySpark. Corrida completada en 14 min 18 s, con cero rechazos y conteos
> verificados. Grafana muestra resúmenes persistidos y tres series de ejemplo;
> no se reproduce la generación durante la presentación.

## Publicación pequeña y separada

`src/benchmark/presentation.py` lee estadísticas y riesgo por bloques y recorre
las **3.000.000 ventanas ya agregadas** en lotes de 10.000. Verifica que sumen
las 15.000.000 lecturas; arma 2.000 puntos agregados de toda la flota, las 1.500
estadísticas/riesgos y 6.000 ventanas de las tres máquinas de ejemplo.

Se guarda un `presentation.json` portátil y se inserta **una fila JSONB** en la
tabla nueva `benchmark_presentations`. Los paneles consultan esa tabla.
No se cargan lecturas individuales ni las tres millones de ventanas en PostgreSQL.
No se escriben `machines`, `sensor_metrics_5m`, `machine_risk_latest` ni MongoDB.
El dashboard habitual permanece separado.

Repetir la publicación del mismo resumen es idempotente. Si el identificador
de corrida ya contiene otro resumen, se rechaza el reemplazo. Solo se aceptan
corridas exitosas con controles y conteos completos. El dashboard nuevo tiene
UID `bd3-benchmark-dashboard` y su definición está en `dashboards/benchmark.json`.

## Reabrir en esta computadora

Con Docker Desktop iniciado, desde la raíz en PowerShell:

```powershell
docker compose --env-file .env -f infra/docker-compose.yml up -d --wait postgres grafana
Start-Process 'http://localhost:3000/d/bd3-benchmark-dashboard?orgId=1&kiosk'
```

Los datos publicados permanecen en el volumen PostgreSQL existente. No hace
falta iniciar Spark, MongoDB ni repetir el benchmark.

El resumen portátil preparado ocupa 2.153.742 bytes (aproximadamente 2 MB).

## Preparar otro resumen desde una corrida completa

```powershell
.\.venv\Scripts\python.exe -m scripts.publish_benchmark --run-dir 'artifacts/benchmarks/<corrida>' --prepare-only
```

El comando informa una carpeta única dentro de `artifacts/benchmark_presentations`.
Para preparar y publicar en un paso, omitir `--prepare-only` con PostgreSQL y
Grafana iniciados. La conexión usa las credenciales locales del `.env`; no se
incluyen en el JSON portátil ni en el dashboard.

## Llevarlo a otra computadora sin los 15 millones

Para una presentación con capturas basta llevar las imágenes o el PowerPoint.
Si también se quiere abrir Grafana, llevar el proyecto y este resumen:

```text
artifacts/benchmark_presentations/20261113T014052-85e354d4a17648508bfcceca5642f855-bd6497ab/presentation.json
```

Con las dependencias instaladas, el `.env` local configurado y PostgreSQL/Grafana
levantados, publicar ese archivo:

```powershell
.\.venv\Scripts\python.exe -m scripts.publish_benchmark --summary '.\presentation.json'
```

No requiere Java, Spark en ejecución, JSONL ni Parquet del benchmark. El código
del proyecto sigue teniendo PySpark como dependencia, pero este comando no inicia
una JVM. `artifacts/` no se incluye en Git: copiar el JSON portátil explícitamente.

## Verificación

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_benchmark_presentation.py -q
.\.venv\Scripts\python.exe -m scripts.verify_benchmark_dashboard --summary 'artifacts/benchmark_presentations/20261113T014052-85e354d4a17648508bfcceca5642f855-bd6497ab/presentation.json'
```

La verificación consulta los diez paneles de datos por la API real de Grafana,
incluidos los filtros de fechas de las curvas. Exige datos, ausencia de errores
y que el indicador principal coincida con las lecturas válidas del resumen.
Las pruebas pequeñas cubren agregación, transporte JSON y rechazo de corridas
parciales o resúmenes con conteos alterados.

Verificación realizada: diez paneles respondieron con datos y sin errores, y
las pruebas pequeñas de publicación más las pruebas unitarias del benchmark
terminaron con **20 aprobadas y dos pruebas de Spark excluidas**. También se
comprobó visualmente que las curvas de temperatura y vibración se dibujan.
