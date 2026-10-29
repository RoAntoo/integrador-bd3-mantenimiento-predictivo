"""Esquema provisional de la muestra de procesamiento."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TELEMETRY_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("temperature", DoubleType(), nullable=True),
        StructField("vibration", DoubleType(), nullable=True),
        StructField("pressure", DoubleType(), nullable=True),
    ]
)

# Al leer JSON, Spark puede producir nulos para campos ausentes. La validación
# del lector verifica los obligatorios; nullable=False no reemplaza ese control.
