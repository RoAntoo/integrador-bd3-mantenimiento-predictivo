"""Clasificar lecturas JSONL y medir su calidad sin perder filas rechazadas."""

import argparse
from dataclasses import dataclass
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session
from src.processing.validation import (
    JSON_OPTIONS,
    REQUIRED_FIELDS,
    required_field_null_checks,
    resolve_input_path,
)


MEASUREMENT_FIELDS = ("temperature", "vibration", "pressure")
CORRUPT_RECORD_FIELD = "_corrupt_record"
QUALITY_SCHEMA = StructType(
    [*TELEMETRY_SCHEMA.fields, StructField(CORRUPT_RECORD_FIELD, StringType(), True)]
)


@dataclass(frozen=True)
class QualityMetrics:
    """Conteos de filas del lote; duplicados es parte de rechazados."""

    received: int
    valid: int
    rejected: int
    duplicates: int


@dataclass(frozen=True)
class QualityResult:
    """Dos DataFrames separados y métricas calculadas del mismo lote."""

    valid: DataFrame
    rejected: DataFrame
    metrics: QualityMetrics


def _parse_lines(spark: SparkSession, input_path: Path) -> DataFrame:
    """Leer una fila por línea y conservar el texto aun si falla el parseo."""
    options = {
        **JSON_OPTIONS,
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": CORRUPT_RECORD_FIELD,
    }
    return (
        spark.read.text(input_path.as_uri())
        .select(F.col("value").alias("raw_line"))
        .withColumn("_parsed", F.from_json("raw_line", QUALITY_SCHEMA, options))
        .withColumn(
            "_parse_error",
            F.col("_parsed").isNull()
            | F.col(f"_parsed.{CORRUPT_RECORD_FIELD}").isNotNull(),
        )
        .select("raw_line", "_parse_error", "_parsed.*")
    )


def _classify_content(parsed: DataFrame) -> DataFrame:
    """Asignar motivos a fallas de parseo, nulos y valores técnicos inválidos."""
    parse_ok = ~F.col("_parse_error")
    reasons = [F.when(F.col("_parse_error"), F.lit("PARSE_ERROR"))]
    reasons.extend(
        F.when(parse_ok & condition, F.lit(f"NULL_{field.upper()}"))
        for field, condition in required_field_null_checks().items()
    )
    reasons.append(
        F.when(
            parse_ok
            & F.col("machine_id").isNotNull()
            & (F.trim("machine_id") == ""),
            F.lit("EMPTY_MACHINE_ID"),
        )
    )
    for field in MEASUREMENT_FIELDS:
        value = F.col(field)
        reasons.append(
            F.when(
                parse_ok
                & value.isNotNull()
                & (F.isnan(value) | (F.abs(value) == float("inf"))),
                F.lit(f"NON_FINITE_{field.upper()}"),
            )
        )
    return parsed.withColumn(
        "rejection_reasons", F.array_compact(F.array(*reasons))
    ).drop("_parse_error", CORRUPT_RECORD_FIELD)


def _classify_duplicates(candidates: DataFrame) -> DataFrame:
    """Retener una copia idéntica y rechazar todas las claves con conflicto."""
    aggregates = []
    for field in MEASUREMENT_FIELDS:
        aggregates.extend(
            [F.min(field).alias(f"_min_{field}"), F.max(field).alias(f"_max_{field}")]
        )
    by_key = candidates.groupBy("machine_id", "timestamp").agg(*aggregates)
    different_measurement = reduce(
        lambda previous, current: previous | current,
        [
            F.col(f"_min_{field}") != F.col(f"_max_{field}")
            for field in MEASUREMENT_FIELDS
        ],
    )
    window = Window.partitionBy("machine_id", "timestamp").orderBy("raw_line")
    duplicate_reason = (
        F.when(different_measurement, F.lit("CONFLICTING_DUPLICATE_KEY"))
        .when(F.row_number().over(window) > 1, F.lit("DUPLICATE_KEY"))
        .otherwise(F.lit(None))
    )
    return (
        candidates.join(by_key, ["machine_id", "timestamp"])
        .withColumn(
            "rejection_reasons", F.array_compact(F.array(duplicate_reason))
        )
        .select(*candidates.columns)
    )


def clean_telemetry(spark: SparkSession, input_path: str | Path) -> QualityResult:
    """Separar lecturas válidas y rechazadas, y contar un lote JSONL local."""
    parsed = _parse_lines(spark, resolve_input_path(input_path))
    classified_content = _classify_content(parsed)
    invalid = classified_content.filter(F.size("rejection_reasons") > 0)
    candidates = classified_content.filter(F.size("rejection_reasons") == 0)
    classified = invalid.unionByName(_classify_duplicates(candidates))

    is_rejected = F.size("rejection_reasons") > 0
    is_duplicate = F.array_contains("rejection_reasons", "DUPLICATE_KEY") | F.array_contains(
        "rejection_reasons", "CONFLICTING_DUPLICATE_KEY"
    )

    def count_if(condition):
        return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

    counts = classified.agg(
        F.count("*").alias("received"),
        count_if(~is_rejected).alias("valid"),
        count_if(is_rejected).alias("rejected"),
        count_if(is_duplicate).alias("duplicates"),
    ).first()
    metrics = QualityMetrics(**counts.asDict())

    return QualityResult(
        valid=classified.filter(~is_rejected).select(*REQUIRED_FIELDS),
        rejected=classified.filter(is_rejected).select(
            "raw_line", *REQUIRED_FIELDS, "rejection_reasons"
        ),
        metrics=metrics,
    )


def main() -> None:
    """Mostrar las métricas de calidad de un archivo JSONL local."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Ruta a un archivo JSONL local.")
    arguments = parser.parse_args()
    spark = create_spark_session()
    try:
        result = clean_telemetry(spark, arguments.input)
        print(f"Recibidos: {result.metrics.received}")
        print(f"Válidos: {result.metrics.valid}")
        print(f"Rechazados: {result.metrics.rejected}")
        print(f"Duplicados: {result.metrics.duplicates}")
    except FileNotFoundError as error:
        parser.exit(status=1, message=f"Error: {error}\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
