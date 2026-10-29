# Entrada provisional del módulo de procesamiento

Este documento describe la muestra independiente de Anto para el primer PR.
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

## Qué valida esta primera implementación

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
texto. No se comprueba todavía el catálogo, identificadores vacíos, rangos físicos
ni valores numéricos especiales como NaN o infinito.

Los campos adicionales no se incorporan al DataFrame. Los duplicados se conservan.
Un archivo vacío es válido y produce cero registros. Los desfases horarios
explícitos se interpretan y se muestran en UTC; la convención propuesta para
el generador es emitir directamente UTC.

Ante errores se interrumpe el lote; no se eliminan ni corrigen filas en silencio.
En una próxima entrega se acordará cómo contabilizar y tratar registros inválidos
para implementar la limpieza solicitada en el plan.

## Acuerdos pendientes con el generador de Leo

| Tema | Acuerdo por definir |
| --- | --- |
| Nombres y unidades | Confirmar nombres definitivos y sus unidades. El plan propone `temperature_c`, `vibration_mm_s` y `pressure_bar`; esta muestra usa los nombres cortos del alcance inicial. No se realizan renombrados automáticos. |
| Campos disponibles | Identificar cuáles estarán siempre presentes y cuáles serán opcionales. El esquema inicial solo cubre estos cinco campos. |
| Fechas | Confirmar UTC, precisión temporal y formato de serialización. |
| Identificadores | Acordar cómo se identifica cada máquina y cómo se comparte su catálogo. |
| Entrega | Confirmar formato, ubicación y organización de archivos por lote. En este PR se recibe un único archivo local por `--input`. |
| Calidad | Definir representación de nulos, duplicados, errores y anomalías intencionales. Una lectura anómala puede ser un dato válido. |
| Muestra compartida | Entregar una muestra pequeña y reproducible con cantidad conocida de eventos para probar la integración. |

Para probar otro archivo que cumpla este esquema basta con cambiar `--input`.
Si cambian nombres o tipos, habrá que ajustar el esquema, la muestra y las pruebas
en un PR acordado. El generador y su futura muestra compartida conservan su
responsabilidad propia; este módulo no los implementa.
