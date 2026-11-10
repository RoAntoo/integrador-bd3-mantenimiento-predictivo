"""Exportar el análisis de Spark a los contratos de consumo y auditoría."""

from pathlib import Path

import pyarrow.parquet as parquet
from pyspark.sql import functions as F

from src.processing.analysis import AnalysisResult
from src.processing.risk_rules import RiskThresholds, classify_windows


METRICS_FILE = "sensor_metrics_5m.parquet"
RISK_FILE = "machine_risk_latest.parquet"
ALERTS_FILE = "alerts.jsonl"


def export_results(result: AnalysisResult, thresholds: RiskThresholds, output: Path) -> None:
    """Escribir los datasets a un directorio nuevo de la corrida."""
    output.mkdir()
    frames = {
        "clean_telemetry.parquet": result.quality.valid.withColumn("data_quality_flag", F.lit("VALID")),
        "rejected_telemetry.parquet": result.quality.rejected,
        "machine_statistics.parquet": result.machine_statistics,
        METRICS_FILE: result.sensor_metrics_5m.select(
            "machine_id", "window_start", "avg_temp", "max_vibration", "avg_pressure", "n_readings",
        ),
        RISK_FILE: result.machine_risk_latest.select(
            "machine_id", "window_start", "calculated_at", "risk_level", "risk_reason",
        ),
    }
    # Arrow conserva el esquema y la zona UTC de Spark en la escritura local.
    for name, frame in frames.items():
        parquet.write_table(frame.toArrow(), output / name)

    alerts = classify_windows(result.sensor_metrics_5m, thresholds).where(
        F.col("risk_level").isin("AMARILLO", "ROJO")
    ).select(
        "machine_id",
        F.date_format("window_start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
        F.col("risk_level").alias("level"),
        F.col("risk_reason").alias("description"),
    )
    json_lines = alerts.select(F.to_json(F.struct(*alerts.columns)).alias("line"))
    with (output / ALERTS_FILE).open("w", encoding="utf-8", newline="\n") as target:
        for row in json_lines.toLocalIterator():
            target.write(row.line + "\n")
