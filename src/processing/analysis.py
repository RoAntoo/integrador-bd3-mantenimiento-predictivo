"""Ejecutar calidad, estadísticas y riesgo con Spark local, sin bases de datos."""

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.processing.quality import QualityResult, clean_telemetry
from src.processing.risk_rules import RiskThresholds, classify_risk, execution_time, load_thresholds
from src.processing.spark import create_spark_session
from src.processing.statistics import machine_statistics, sensor_metrics_5m


@dataclass(frozen=True)
class AnalysisResult:
    quality: QualityResult
    machine_statistics: DataFrame
    sensor_metrics_5m: DataFrame
    machine_risk_latest: DataFrame


def analyze_telemetry(spark: SparkSession, input_path: str | Path,
                      calculated_at: datetime,
                      thresholds: RiskThresholds = RiskThresholds(), *,
                      normalized_input: bool = False, batch_id: str | None = None) -> AnalysisResult:
    """Conservar el resultado de calidad y derivar tres DataFrames independientes."""
    instant = execution_time(calculated_at)
    quality = clean_telemetry(spark, input_path, normalized_input=normalized_input, batch_id=batch_id)
    metrics = sensor_metrics_5m(quality.valid)
    return AnalysisResult(quality, machine_statistics(quality.valid), metrics,
                          classify_risk(metrics, instant, thresholds))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Archivo JSONL local.")
    parser.add_argument("--calculated-at", required=True, help="Instante de ejecución ISO 8601 con zona.")
    parser.add_argument("--thresholds", type=Path, help="JSON de umbrales; por defecto reglas académicas aprobadas.")
    args = parser.parse_args()
    try:
        instant = execution_time(datetime.fromisoformat(args.calculated_at))
        rules = load_thresholds(args.thresholds) if args.thresholds else RiskThresholds()
    except (ValueError, OSError) as error:
        parser.error(str(error))
    spark = create_spark_session()
    try:
        result = analyze_telemetry(spark, args.input, instant, rules)
        print(f"Calidad: {result.quality.metrics}")
        for name in ("machine_statistics", "sensor_metrics_5m", "machine_risk_latest"):
            print(f"\n{name} (hasta 20 filas):")
            frame = getattr(result, name)
            frame.printSchema()
            order = ["machine_id", "window_start"] if "window_start" in frame.columns else ["machine_id"]
            frame.orderBy(*order).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
