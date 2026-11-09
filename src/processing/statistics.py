"""Estadísticas de lecturas válidas del PR #2, sin alterar su contrato."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _aggregates():
    expressions = [F.count("*").alias("n_readings")]
    for source, suffix in (("temperature", "temp"), ("vibration", "vibration"), ("pressure", "pressure")):
        for name, function in (("avg", F.avg), ("min", F.min), ("max", F.max)):
            expressions.append(function(source).alias(f"{name}_{suffix}"))
    return expressions


def machine_statistics(valid: DataFrame) -> DataFrame:
    """Una fila por máquina presente en el lote; entrada: QualityResult.valid."""
    return valid.groupBy("machine_id").agg(
        F.min("timestamp").alias("first_timestamp"),
        F.max("timestamp").alias("last_timestamp"),
        *_aggregates(),
    )


def sensor_metrics_5m(valid: DataFrame) -> DataFrame:
    """Ventanas fijas UTC [inicio, fin); no se generan ventanas sin lecturas."""
    return (
        valid.groupBy("machine_id", F.window("timestamp", "5 minutes"))
        .agg(*_aggregates())
        .select("machine_id", F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "n_readings", "avg_temp", "min_temp", "max_temp",
                "avg_vibration", "min_vibration", "max_vibration",
                "avg_pressure", "min_pressure", "max_pressure")
    )
