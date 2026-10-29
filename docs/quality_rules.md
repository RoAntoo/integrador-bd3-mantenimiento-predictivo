# Limpieza y control de calidad de telemetría

Este flujo procesa un archivo JSONL local por lote con `clean_telemetry(spark,
input_path)`. Devuelve `QualityResult.valid`, `QualityResult.rejected` y
`QualityResult.metrics`. El lector estricto `read_telemetry` del primer PR sigue
disponible y conserva sus errores y su comportamiento ante duplicados.

## Lectura y reglas

Se lee el archivo como texto, una fila por línea física, para conservar el
contenido original en `rejected.raw_line`. El separador de línea no forma parte
de `raw_line`. Una línea vacía cuenta como recibida y se rechaza. Un archivo
vacío produce todos los contadores en cero. Un archivo inexistente genera
`FileNotFoundError`, como en el lector estricto.

El parseo usa el mismo esquema y las mismas opciones de JSON y fechas que el
lector estricto, pero en modo `PERMISSIVE` y con una columna interna para
capturar registros corruptos. Un error de sintaxis JSON, tipo numérico o fecha
produce `PARSE_ERROR`. No se agregan motivos por campos nulos cuando hubo un
error de parseo, porque esos nulos pueden ser consecuencia del error.

Para una fila parseada, se aplican estas reglas:

| Motivo | Condición |
| --- | --- |
| `NULL_<CAMPO>` | Falta el campo obligatorio o su valor es `null`. Se usa para los cinco campos del esquema. |
| `EMPTY_MACHINE_ID` | El identificador solo contiene espacios o está vacío. No se modifica el identificador recibido. |
| `NON_FINITE_<CAMPO>` | `temperature`, `vibration` o `pressure` es `NaN`, `+Infinity` o `-Infinity`. |
| `DUPLICATE_KEY` | Copia adicional de una lectura válida con la misma máquina, instante y mediciones. |
| `CONFLICTING_DUPLICATE_KEY` | Hay mediciones diferentes entre lecturas válidas con la misma máquina e instante. Se rechazan todas las filas de esa clave. |

`rejection_reasons` es una lista de códigos; una fila puede tener varios motivos
de contenido. Las filas rechazadas incluyen también los cinco campos parseados
cuando estén disponibles. `valid` contiene únicamente los cinco campos del
esquema original.

La deduplicación se aplica después de los controles de contenido y solo entre
las candidatas válidas. La clave es `(machine_id, timestamp)`; Spark interpreta
los instantes con zona horaria y la sesión los normaliza a UTC. Una fila inválida
no compite con una válida que tenga la misma clave. Para copias idénticas se
conserva una; si las líneas difieren por formato o campos adicionales, se
conserva la menor línea en orden lexicográfico, sin cambiar la lectura parseada.
Los campos adicionales no forman parte de la comparación.

No se aplican rangos físicos, estadísticas por máquina ni reglas de riesgo: una
medición anómala pero finita sigue siendo válida. Como en el lector inicial,
Spark puede convertir ciertos tipos JSON al tipo declarado (por ejemplo, un
`machine_id` numérico a texto). Este flujo no pretende validar el tipo JSON
original de cada campo ni la existencia de la máquina en un catálogo.

## Métricas

| Contador | Significado |
| --- | --- |
| `received` | Líneas leídas, incluidas las vacías y mal formadas. |
| `valid` | Lecturas que pasan todas las reglas y quedan en `valid`. |
| `rejected` | Filas en `rejected`, cada una con su línea original y motivos. |
| `duplicates` | Filas rechazadas por `DUPLICATE_KEY` o `CONFLICTING_DUPLICATE_KEY`; es un subconjunto de `rejected`. |

Se cumple `received = valid + rejected`. En un conflicto de dos lecturas, las
dos cuentan como rechazadas y duplicadas. La deduplicación es **dentro del
archivo procesado**; no hay estado ni comparación entre lotes.
