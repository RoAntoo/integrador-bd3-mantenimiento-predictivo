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

La clasificación de riesgos, estadísticas por máquina, Machine Learning,
exportaciones y conexiones a bases corresponden a entregas posteriores. Este
módulo no requiere Kafka, cloud ni otros servicios.

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
