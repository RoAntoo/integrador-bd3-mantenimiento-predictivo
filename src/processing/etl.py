"""Leer una muestra JSONL, validar sus campos y mostrarla con Spark."""

import argparse
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session
from src.processing.validation import (
    JSON_OPTIONS,
    REQUIRED_FIELDS,
    missing_required_value,
    resolve_input_path,
)


def read_telemetry(spark: SparkSession, input_path: str | Path) -> DataFrame:
    """Leer un archivo local y rechazar errores de lectura o campos nulos."""
    dataset_path = resolve_input_path(input_path)

    try:
        telemetry = (
            spark.read.schema(TELEMETRY_SCHEMA)
            .options(**JSON_OPTIONS)
            .option("mode", "FAILFAST")
            .json(dataset_path.as_uri())
        )
        # Spark evalúa de forma diferida: esta acción obliga a leer los datos
        # antes de devolverlos, incluyendo los campos que podrían ser inválidos.
        has_missing_values = telemetry.filter(missing_required_value()).limit(1).count()
    except (PySparkException, Py4JJavaError) as error:
        raise ValueError(
            "No se pudo leer el JSONL con el esquema esperado. "
            "Revisá el formato JSON, los números y las fechas con zona horaria."
        ) from error

    if has_missing_values:
        required_fields = ", ".join(REQUIRED_FIELDS)
        raise ValueError(
            "Hay campos obligatorios ausentes o nulos. "
            f"Cada registro debe incluir: {required_fields}."
        )
    return telemetry


def main() -> None:
    """Ejecutar la demostración desde la línea de comandos."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Ruta a un archivo JSONL local.")
    arguments = parser.parse_args()

    spark = create_spark_session()
    try:
        telemetry = read_telemetry(spark, arguments.input)
        telemetry.printSchema()
        print("Registros de muestra (hasta 20):")
        telemetry.show(20, truncate=False)
        print(f"Cantidad de registros: {telemetry.count()}")
    except (FileNotFoundError, ValueError) as error:
        parser.exit(status=1, message=f"Error: {error}\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
