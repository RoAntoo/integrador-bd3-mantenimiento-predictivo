# Persistencia y visualización

## Ejecución local

Desde la raíz del repositorio, instalar Python 3.12, JDK 17 o 21 y las dependencias
según el README. Iniciar Docker Desktop, copiar `.env.example` a `.env` y completar
las tres contraseñas. Las siguientes instrucciones usan PowerShell.

```powershell
docker compose --env-file .env -f infra/docker-compose.yml up -d --wait
.\.venv\Scripts\python.exe -m scripts.run_demo --load
```

En volúmenes PostgreSQL existentes, aplicar primero la migración con el usuario
y base configurados. Este ejemplo utiliza los valores predeterminados.

```powershell
docker compose --env-file .env -f infra/docker-compose.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U admin_bd3 -d mantenimiento_predictivo -f /docker-entrypoint-initdb.d/002_pipeline_integration.sql
```

La consola imprime el directorio publicado y el balance global. Abrir Grafana en
el puerto configurado y seleccionar el tablero `bd3-machines-dashboard`.
El preset corresponde al 25/10/2026, entre 00:00 y 01:00 UTC. Las tendencias
filtran por máquina y rango temporal. El ranking conserva el último estado y
muestra la fecha de cálculo y el inicio de su ventana de medición. Los registros
antiguos cargados desde CSV pueden carecer de ventana y mostrar un valor vacío.

## Verificación de datos

En PostgreSQL ejecutar las consultas siguientes.

```sql
SELECT machine_id, window_start, n_readings FROM sensor_metrics_5m
ORDER BY machine_id, window_start;
SELECT machine_id, risk_level, window_start, calculated_at FROM machine_risk_latest;
```

En una sesión autenticada de MongoDB, seleccionar la base configurada y consultar
la colección de alertas.

```javascript
db.alerts.find({}, {machine_id: 1, timestamp: 1, level: 1, description: 1})
```

Repetir la carga y verificar que las claves y conteos se mantienen. Si PostgreSQL
confirmó y MongoDB falló, corregir la conexión y ejecutar el reintento.

```powershell
.\.venv\Scripts\python.exe -m scripts.run_demo --load-only "<directorio-del-lote>"
```

## Auditoría y pruebas

El contrato de datos describe el histórico de ingesta, el curado y los dos archivos
de rechazos. `batch.json.balance` concilia cada línea recibida una sola vez.
Los lotes totalmente rechazados publican resultados vacíos y su auditoría.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_input_contract.py tests/test_pipeline.py
$env:BD3_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest tests/test_pipeline_integration.py
```

La integración utiliza identificadores propios y elimina sus registros al terminar.
Verifica reintentos, rollback y consultas reales de Grafana dentro y fuera del rango.
`tests/test_storage.py` carga los CSV de muestra y debe ejecutarse en una base de
pruebas separada. Esas filas carecen de propietario y ventana verificable.

## Limitaciones vigentes

Las ventanas publicadas pertenecen a un lote. Otro lote sobre las mismas claves
produce rollback. Las ventanas parciales no se recomponen. PostgreSQL y MongoDB
confirman por separado. MongoDB almacena alertas con timestamp de texto.
El catálogo SQL utiliza nombres y ubicaciones genéricos. El histórico de ingesta
resuelve conflictos por última lectura, mientras el curado rechaza las claves
conflictivas. La exportación materializa cada DataFrame en memoria y está orientada
a los volúmenes de la demo. La ingesta también acumula el lote en memoria.

Los umbrales académicos evalúan máximos altos. La presión baja, la frescura automática,
la predicción por ML y las tendencias de degradación quedan pendientes. El instante
de cálculo no sustituye a la fecha de medición que muestra el tablero.
