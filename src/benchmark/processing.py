"""Procesamiento distribuido con las reglas y los contratos del pipeline normal."""

import os
from pathlib import Path
import sys

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F

from src.benchmark.writer import write_local_dataset
from src.processing.export_results import METRICS_FILE, RISK_FILE
from src.processing.quality import clean_telemetry_frame
from src.processing.risk_rules import RiskThresholds, classify_risk, classify_windows
from src.processing.statistics import machine_statistics, sensor_metrics_5m


def create_benchmark_spark(config, scratch: Path) -> SparkSession:
    """Crear una sesión dedicada; la CLI inicia un proceso Python/JVM nuevo."""
    if SparkContext._active_spark_context is not None:
        raise RuntimeError("El benchmark requiere un proceso sin otra sesión Spark activa.")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    scratch.mkdir()
    builder = (SparkSession.builder.appName("BD3 - Benchmark local")
             .master(f"local[{config.spark_threads}]")
             .config("spark.driver.memory", config.spark_memory)
             .config("spark.driver.maxResultSize", "128m")
             .config("spark.local.dir", str(scratch.resolve()))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
             .config("spark.sql.files.maxPartitionBytes", str(config.max_partition_mb * 1024**2))
             .config("spark.sql.files.maxRecordsPerFile", str(config.rows_per_file))
             .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(config.batch_size))
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.hadoop.parquet.block.size", str(64 * 1024**2)))
    if os.name == "nt" and sys.version_info >= (3, 12) and pyspark.__version__ == "4.0.1":
        worker_path = str(Path(__file__).resolve().parents[2])
        if os.environ.get("PYTHONPATH"):
            worker_path += os.pathsep + os.environ["PYTHONPATH"]
        builder = (builder.config("spark.python.worker.module", "src.benchmark.spark_worker")
                   .config("spark.executorEnv.PYTHONPATH", worker_path))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def parquet_rows(path: Path) -> int:
    """Contar las filas almacenadas a partir de los footers, sin traer datos a Python."""
    import pyarrow.parquet as pq

    files = path.rglob("*.parquet") if path.is_dir() else iter([path])
    total, file_count = 0, 0
    for part in files:
        total += pq.ParquetFile(part).metadata.num_rows
        file_count += 1
    if not file_count:
        raise ValueError(f"No hay archivos Parquet verificables en {path}")
    return total


def read_parquet(spark, path: Path):
    """Pasar archivos explícitos evita el listado Hadoop que requiere DLL en Windows."""
    files = sorted(path.glob("*.parquet"))
    if not files:
        raise ValueError(f"No hay partes Parquet en {path}")
    return spark.read.parquet(*(part.resolve().as_uri() for part in files))


def process_parquet(spark, source: Path, output: Path, work: Path, *,
                    batch_id: str, calculated_at, config, progress) -> dict:
    """Materializar calidad una vez; derivar estadísticas desde el Parquet curado."""
    output.mkdir()
    parsed = (read_parquet(spark, source)
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("_parse_error", F.lit(False)))
    def write_path(frame, path):
        write_local_dataset(frame, path, rows_per_file=config.rows_per_file)

    def materialize(frame):
        path = work / "classified.parquet"
        write_path(frame, path)
        return read_parquet(spark, path)

    quality = clean_telemetry_frame(parsed, materialize=materialize)
    counts = {"spark_received": quality.metrics.received,
              "spark_rejected": quality.metrics.rejected,
              "valid": quality.metrics.valid, "duplicates": quality.metrics.duplicates}
    progress(counts)

    def write(frame, name):
        write_path(frame, output / name)

    write(quality.valid.withColumn("data_quality_flag", F.lit("VALID")), "clean_telemetry.parquet")
    write(quality.rejected, "rejected_telemetry.parquet")
    clean = read_parquet(spark, output / "clean_telemetry.parquet")
    write(machine_statistics(clean), "machine_statistics.parquet")
    windows = sensor_metrics_5m(clean)
    # Conservar máximos y window_end hasta calcular el riesgo, sin perder contrato B.
    windows_path = work / "windows.parquet"
    write_path(windows, windows_path)
    windows = read_parquet(spark, windows_path)
    write(windows.select("machine_id", "window_start", "avg_temp", "max_vibration",
                         "avg_pressure", "n_readings"), METRICS_FILE)
    rules = RiskThresholds()
    write(classify_risk(windows, calculated_at, rules).select(
        "machine_id", "window_start", "calculated_at", "risk_level", "risk_reason"), RISK_FILE)
    alerts = classify_windows(windows, rules).where(F.col("risk_level").isin("AMARILLO", "ROJO")).select(
        "machine_id", F.date_format("window_start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
        F.col("risk_level").alias("level"), F.col("risk_reason").alias("description"))
    # Cada part-*.jsonl conserva el contrato; no se fuerza un archivo único.
    write_local_dataset(alerts, output / "alerts", rows_per_file=config.rows_per_file, jsonl=True)

    stored = {name: parquet_rows(output / name) for name in (
        "clean_telemetry.parquet", "rejected_telemetry.parquet", "machine_statistics.parquet",
        METRICS_FILE, RISK_FILE)}
    stats = read_parquet(spark, output / "machine_statistics.parquet")
    # first() devuelve únicamente una fila agregada de tamaño constante.
    machine_check = stats.agg(F.count("*").alias("machines"),
                              F.coalesce(F.sum("n_readings"), F.lit(0)).alias("readings"),
                              F.min("n_readings").alias("min_readings"),
                              F.max("n_readings").alias("max_readings")).first().asDict()
    metrics = read_parquet(spark, output / METRICS_FILE)
    window_sum = metrics.agg(F.coalesce(F.sum("n_readings"), F.lit(0)).alias("n")).first().n
    checks = {
        "stored_clean_matches_valid": stored["clean_telemetry.parquet"] == counts["valid"],
        "stored_rejections_match_quality": stored["rejected_telemetry.parquet"] == counts["spark_rejected"],
        "statistics_cover_valid": machine_check["readings"] == counts["valid"],
        "windows_cover_valid": window_sum == counts["valid"],
        "expected_machines": machine_check["machines"] == config.machines,
        "expected_readings_per_machine": machine_check["min_readings"] == config.readings_per_machine
            and machine_check["max_readings"] == config.readings_per_machine,
        "risk_for_every_machine": stored[RISK_FILE] == config.machines,
    }
    return {"counts": counts, "stored_rows": stored, "machine_check": machine_check,
            "window_readings_sum": window_sum, "checks": checks}
