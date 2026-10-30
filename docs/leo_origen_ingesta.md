# Módulo de Origen de Datos, Ingesta y Data Lake (LEO-01 a LEO-05)

Este documento describe la arquitectura, decisiones técnicas y comandos de ejecución del módulo de **Origen de Datos e Ingesta** (responsabilidad de Leo), abarcando desde la simulación determinista de sensores hasta el almacenamiento en el Data Lake particionado en Parquet.

---

## 1. Catálogo compartido de máquinas (`LEO-02`)

El catálogo se define en [`src/generator/catalog.py`](../src/generator/catalog.py) y se publica en [`data/sample/machines_catalog.json`](../data/sample/machines_catalog.json).
Cada equipo posee un identificador único (`MAQ-001` a `MAQ-NNN`) y parámetros nominales según su familia industrial:

| Familia | Tipo (`machine_type`) | Sector | Temp. Base (°C) | Vibración Base (mm/s) | Presión Base (bar) | RPM Base | Potencia Base (kW) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Compresor Tornillo | `compressor` | Sector A - Compresión | 66.0 | 2.1 | 5.2 | 1480 | 22.5 |
| Bomba Centrífuga | `pump` | Sector B - Hidráulica | 70.0 | 2.6 | 5.5 | 1750 | 18.0 |
| Turbina de Extracción | `turbine` | Sector C - Energía | 74.5 | 2.9 | 6.0 | 2850 | 42.0 |
| Motor Trifásico Principal | `motor` | Sector D - Tracción | 64.0 | 1.9 | 4.8 | 1450 | 15.5 |
| Prensa Neumática | `press` | Sector E - Estampado | 68.5 | 2.4 | 5.8 | 1600 | 27.0 |

---

## 2. Contrato de datos y compatibilidad con PySpark (`LEO-01` / `LEO-02`)

Cada línea de [`data/sample/telemetry_sample.jsonl`](../data/sample/telemetry_sample.jsonl) es un objeto JSON válido codificado en UTF-8 que incluye:
- **Campos del Contrato A (Plan de trabajo)**: `machine_id`, `timestamp` (ISO 8601 UTC con sufijo `Z`), `temperature_c`, `vibration_mm_s`, `pressure_bar`, `rpm`, `power_kw`, `operating_hours`, `machine_state` (`operating`, `idle`, `stopped`).
- **Campos cortos compatibles con `src/processing/schema.py`**: `temperature`, `vibration`, `pressure`. Permiten que tanto el lector estricto (`src.processing.etl`) como el flujo de calidad (`src.processing.quality`) de Anto lean la muestra directamente sin errores.
- **Trazabilidad de fallas simuladas**: `is_anomaly` (`true`/`false`) y `anomaly_type` (`null` o código de anomalía).

### Tipos de anomalías simuladas (Puntuales y Sostenidas)
1. **`overheating_spike` (Puntual)**: pico térmico entre `94.0 °C` y `114.0 °C` con aumento de consumo eléctrico.
2. **`vibration_spike` (Puntual)**: vibración mecánica elevada entre `6.8 mm/s` y `11.5 mm/s`.
3. **`pressure_anomaly` (Puntual)**: despresurización por fuga (`1.1 - 2.2 bar`) o sobrepresión (`8.6 - 10.8 bar`).
4. **`sustained_degradation` (Sostenida)**: ráfaga consecutiva de 3 a 5 lecturas en una misma máquina donde la temperatura (`89.0+ °C`), la vibración (`5.8+ mm/s`) y la presión se incrementan progresivamente.

---

## 3. Ingesta Batch, Zona Raw y Data Lake Parquet (`LEO-03` / `LEO-04`)

El script [`src/ingestion/ingest.py`](../src/ingestion/ingest.py) implementa el proceso reproducible de recepción y persistencia histórica:

1. **Validación de entrada y registro de volumen/fechas**:
   - Verifica la estructura JSON, tipos numéricos finitos y marcas temporales UTC.
   - Calcula el hash `SHA-256` del lote, tamaño en bytes, conteo de líneas recibidas/válidas/inválidas, máquinas involucradas y rango temporal (`min_timestamp` y `max_timestamp`).
2. **Zona `raw`**:
   - En modo permisivo, omite las líneas rechazadas (que permanecen únicamente en el archivo de entrada original) y copia las lecturas válidas organizadas por fecha en `<lake_root>/raw/date=YYYY-MM-DD/<batch_tag>.jsonl`.
   - Actualiza de forma idempotente el manifiesto `<lake_root>/raw/_ingestion_manifest.jsonl`.
3. **Zona `historical` en formato Parquet particionado (`date=YYYY-MM-DD`)**:
   - Escribe los registros validados y deduplicados por `(machine_id, timestamp)` en `<lake_root>/historical/telemetry/date=YYYY-MM-DD/part-<batch_tag>.parquet`.
   - **Idempotencia**: volver a ejecutar la ingesta sobre el mismo lote detecta el `SHA-256` previo, consolida la partición del lote y no duplica filas ni entradas en el manifiesto.

### Comparativa práctica: JSONL vs. Parquet en el Data Lake
- **JSONL (Zona `raw`)**: formato semiestructurado orientado a filas y legible por humanos; conserva las líneas válidas recibidas en texto plano por fecha junto con el manifiesto de conteos del lote.
- **Parquet (Zona `historical`)**: almacenamiento columnar comprimido con tipos explícitos. Reduce drásticamente el tamaño en disco, acelera agregaciones analíticas (promedios/máximos sobre columnas puntuales como `temperature_c` o `vibration_mm_s`) y aprovecha el particionado `date=YYYY-MM-DD` (*partition pruning*) para evitar escanear todo el histórico.

---

## 4. Configuración opcional de Azure Blob Storage (`LEO-04`)

La subida al Data Lake en la nube es opcional y nunca almacena credenciales en el código:
1. Exportar las variables en la terminal (el script lee únicamente `os.environ` y no carga archivos `.env`; `.env.example` sirve como referencia de nombres):
   ```powershell
   $env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
   $env:AZURE_STORAGE_CONTAINER = "datalake"
   ```
2. Instalar el cliente si se desea probar la subida cloud: `pip install azure-storage-blob`.
3. Ejecutar la ingesta agregando `--upload-azure`:
   ```powershell
   python -m src.ingestion.ingest --input data/sample/telemetry_sample.jsonl --upload-azure
   ```

---

## 5. Comandos de ejecución y verificación (`LEO-05`)

### 1. Generar la muestra liviana versionable (120 eventos)
```powershell
python -m src.generator.generate --machines 5 --days 1 --interval 60 --seed 42 --anomaly_rate 0.10 --output data/sample/telemetry_sample.jsonl
```

### 2. Presets de volumen (14.400 y 504.000 eventos)
```powershell
# Preset 1: 10 máquinas / 1 día / 1 min = 14.400 eventos
python -m src.generator.generate --machines 10 --days 1 --interval 1 --seed 42 --anomaly_rate 0.05 --output artifacts/telemetry_14k.jsonl

# Preset 2: 50 máquinas / 7 días / 1 min = 504.000 eventos
python -m src.generator.generate --machines 50 --days 7 --interval 1 --seed 42 --anomaly_rate 0.05 --output artifacts/telemetry_504k.jsonl
```

### 3. Ejecutar ingesta a Zona Raw y Parquet particionado
```powershell
python -m src.ingestion.ingest --input data/sample/telemetry_sample.jsonl
```

### 4. Ejecutar pruebas automáticas del módulo de Leo
```powershell
python -m unittest discover -s tests -p "test_generator.py" -v
```

---

## 6. Evidencia de ejecución y conteos de entrada/salida

### Salida del generador (`LEO-01` / `LEO-02`)
```text
=== Resumen de generación de telemetría (LEO-01) ===
Archivo de salida: data/sample/telemetry_sample.jsonl
Máquinas simuladas: 5 (MAQ-001 .. MAQ-005)
Lecturas por máquina: 24
Rango temporal UTC: 2026-10-25T00:00:00Z -> 2026-10-25T23:00:00Z
Semilla (seed): 42 | Tasa de anomalías objetivo: 0.1
Total de eventos generados: 120
Eventos normales: 108
Eventos anómalos: 12
Desglose de anomalías inyectadas por tipo:
  - overheating_spike: 2
  - pressure_anomaly: 3
  - sustained_degradation: 4
  - vibration_spike: 3
Muestra de anomalías inyectadas (primeras 5):
  * [2026-10-25T00:00:00Z] MAQ-001 -> sustained_degradation (temp=90.34°C, vib=6.04mm/s, pres=8.11bar)
  * [2026-10-25T01:00:00Z] MAQ-001 -> sustained_degradation (temp=93.95°C, vib=6.96mm/s, pres=8.73bar)
  * [2026-10-25T02:00:00Z] MAQ-001 -> sustained_degradation (temp=97.56°C, vib=8.03mm/s, pres=8.92bar)
  * [2026-10-25T03:00:00Z] MAQ-001 -> sustained_degradation (temp=101.46°C, vib=9.0mm/s, pres=9.63bar)
  * [2026-10-25T04:00:00Z] MAQ-001 -> vibration_spike (temp=67.28°C, vib=9.55mm/s, pres=5.3bar)
```

### Salida de la ingesta a Zona Raw y Parquet (`LEO-03` / `LEO-04`)
```text
=== Resumen de Ingesta y Data Lake (LEO-03 / LEO-04) ===
Archivo de entrada: data/sample/telemetry_sample.jsonl (38919 bytes)
SHA-256 del lote: b17fc8dae1e389f2234a5120a86e092bcbd9d0d4bdfe77d70209e707ce121c87
Re-ejecución idempotente: No
Conteos -> Recibidos: 120 | Válidos: 120 | Inválidos: 0
Rango de fechas UTC: 2026-10-25T00:00:00Z -> 2026-10-25T23:00:00Z
Máquinas detectadas (5): MAQ-001, MAQ-002, MAQ-003, MAQ-004, MAQ-005
Particiones generadas (1): date=2026-10-25
Archivos en Zona Raw:
  - artifacts/datalake/raw/date=2026-10-25/telemetry_sample_b17fc8dae1.jsonl
Archivos en Histórico Parquet:
  - artifacts/datalake/historical/telemetry/date=2026-10-25/part-telemetry_sample_b17fc8dae1.parquet
Manifiesto de ingesta: artifacts/datalake/raw/_ingestion_manifest.jsonl
```

