"""Corridas únicas, métricas verificadas y tratamiento explícito de interrupciones."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import csv
import ctypes
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
from time import perf_counter
import traceback
from uuid import uuid4

from src.benchmark.generation import generate_jsonl
from src.benchmark.ingestion import ingest_jsonl
from src.generator.generate import DEFAULT_START_TIME, _parse_utc_timestamp


@dataclass(frozen=True)
class BenchmarkConfig:
    machines: int
    readings_per_machine: int
    seed: int = 42
    output_dir: Path = Path("artifacts/benchmarks")
    batch_size: int = 10000
    rows_per_file: int = 250000
    anomaly_rate: float = 0.05
    start_time: str = DEFAULT_START_TIME
    interval: int = 1
    spark_threads: int = 2
    spark_memory: str = "2g"
    shuffle_partitions: int = 16
    max_partition_mb: int = 64

    def __post_init__(self):
        for name in ("machines", "readings_per_machine", "interval", "spark_threads",
                     "shuffle_partitions", "max_partition_mb", "rows_per_file", "batch_size"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} debe ser mayor a cero.")
        if self.machines * self.readings_per_machine > 15000000:
            raise ValueError("El límite por corrida es 15.000.000 de registros.")
        if self.batch_size > 100000 or self.rows_per_file < self.batch_size:
            raise ValueError("batch_size <= 100000 y rows_per_file >= batch_size.")
        if not 0 <= self.anomaly_rate <= 1:
            raise ValueError("anomaly_rate debe estar entre 0 y 1.")
        if not re.fullmatch(r"[1-9][0-9]*[gGmM]", self.spark_memory):
            raise ValueError("spark_memory debe tener formato 2g o 4096m.")
        _parse_utc_timestamp(self.start_time)


def environment_info(output: Path) -> dict:
    info = {"os": platform.platform(), "python": sys.version, "python_executable": sys.executable,
            "logical_cpus": os.cpu_count(), "disk_free_bytes_before": shutil.disk_usage(output).free,
            "ram_total_bytes": None, "ram_available_bytes_before": None}
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [
                (name, ctypes.c_ulonglong) for name in
                ("total", "available", "page_total", "page_available", "virtual", "virtual_available", "extended")]
        memory = MemoryStatus()
        memory.length = ctypes.sizeof(memory)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory)):
            info.update(ram_total_bytes=memory.total, ram_available_bytes_before=memory.available)
    elif hasattr(os, "sysconf"):
        try:
            info["ram_total_bytes"] = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            pass
    info["packages"] = {name: importlib.metadata.version(name) for name in ("pyspark", "pyarrow")}
    java = Path(os.environ["JAVA_HOME"]) / "bin" / "java" if os.environ.get("JAVA_HOME") else "java"
    try:
        result = subprocess.run([str(java), "-version"], capture_output=True, text=True, timeout=10)
        info["java"] = (result.stderr or result.stdout).strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        info["java"] = str(error)
    return info


def size_bytes(path: Path, suffixes=None) -> int:
    if not path.exists():
        return 0
    paths = path.rglob("*") if path.is_dir() else iter([path])
    return sum(p.stat().st_size for p in paths if p.is_file()
               and (suffixes is None or p.suffix in suffixes))


def save_metrics(run: Path, metrics: dict, *, final: bool = False):
    staging = run / "metrics.json.tmp"
    staging.write_text(json.dumps(metrics, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    staging.replace(run / "metrics.json")
    if final:
        row = {"run_id": metrics["run_id"], "status": metrics["status"],
               "machines": metrics["config"]["machines"],
               "readings_per_machine": metrics["config"]["readings_per_machine"],
               "seed": metrics["config"]["seed"],
               **{key: metrics["config"][key] for key in (
                   "batch_size", "rows_per_file", "spark_threads", "spark_memory",
                   "shuffle_partitions", "max_partition_mb", "anomaly_rate", "interval")},
               "stage": metrics["stage"], "error_count": len(metrics["errors"]),
               **metrics["counts"],
               **{f"seconds_{k}": v for k, v in metrics["seconds"].items()}, **metrics["sizes"]}
        # Campos estructurados completos permanecen en JSON; CSV tiene una fila por corrida.
        row.pop("anomaly_breakdown", None)
        with (run / "metrics.csv").open("x", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)


def run_benchmark(config: BenchmarkConfig) -> tuple[Path, dict]:
    started = perf_counter()
    root = Path(config.output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
    run = root / run_id
    run.mkdir()  # Nunca reutilizar ni sobrescribir una corrida previa.
    work = run / "work"
    work.mkdir()
    source = run / "telemetry.jsonl"
    metrics = {"format_version": 1, "run_id": run_id, "status": "running", "stage": "environment",
               "started_at": datetime.now(timezone.utc).isoformat(),
               "config": {**asdict(config), "output_dir": str(root)}, "counts": {
                   "requested": config.machines * config.readings_per_machine, "generated": 0,
                   "received": None, "ingestion_rejected": None, "spark_received": None,
                   "spark_rejected": None, "rejected": None, "valid": None,
                   "anomalous": 0, "anomaly_breakdown": {}, "ingestion_valid": None,
                   "max_buffered_records": None, "ingestion_parquet_files": None,
                   "duplicates": None},
               "seconds": {name: None for name in ("generation", "ingestion", "spark_startup", "processing", "total")},
               "sizes": {}, "checks": {}, "errors": []}
    spark = None
    stage_started = perf_counter()
    checkpoint_time = 0.0

    def checkpoint(counts):
        nonlocal checkpoint_time
        metrics["counts"].update(counts)
        now = perf_counter()
        if now - checkpoint_time >= 5:
            save_metrics(run, metrics)
            checkpoint_time = now

    def stage(name):
        nonlocal stage_started
        if metrics["stage"] in metrics["seconds"]:
            metrics["seconds"][metrics["stage"]] = perf_counter() - stage_started
        metrics["stage"] = name
        stage_started = perf_counter()
        save_metrics(run, metrics)
        print(f"[{run_id}] {name}", flush=True)

    try:
        save_metrics(run, metrics)
        metrics["environment"] = environment_info(run)
        stage("generation")
        checkpoint(generate_jsonl(source, machines=config.machines,
                                  readings_per_machine=config.readings_per_machine, seed=config.seed,
                                  anomaly_rate=config.anomaly_rate, start_time=config.start_time,
                                  interval=config.interval, progress=checkpoint))
        if metrics["counts"]["generated"] != metrics["counts"]["requested"]:
            raise ValueError("La generación no produjo todos los registros solicitados.")
        stage("ingestion")
        checkpoint(ingest_jsonl(source, work / "ingested.parquet", run / "ingestion_rejected.jsonl",
                                batch_size=config.batch_size, rows_per_file=config.rows_per_file,
                                batch_id=run_id, progress=checkpoint))
        if metrics["counts"]["received"] != metrics["counts"]["generated"]:
            raise ValueError("La ingesta no recibió todos los registros generados.")
        stage("spark_startup")
        from src.benchmark.processing import create_benchmark_spark, parquet_rows, process_parquet
        spark = create_benchmark_spark(config, work / "spark-local")
        keys = ("spark.master", "spark.app.name", "spark.driver.memory", "spark.driver.maxResultSize",
                "spark.local.dir", "spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                "spark.sql.files.maxPartitionBytes", "spark.sql.files.maxRecordsPerFile",
                "spark.sql.adaptive.enabled", "spark.sql.parquet.compression.codec",
                "spark.sql.execution.arrow.maxRecordsPerBatch", "spark.python.worker.module")
        metrics["spark"] = {key: spark.conf.get(key, None) for key in keys}
        # Sin default explícito, Spark devuelve el valor efectivo del ajuste SQL.
        metrics["spark"]["spark.sql.adaptive.enabled"] = spark.conf.get("spark.sql.adaptive.enabled")
        metrics["spark"]["version"] = spark.version
        metrics["spark"]["writer"] = "local-workers-mapInArrow"
        metrics["spark"]["jvm_max_heap_bytes"] = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
        calculated_at = datetime.now(timezone.utc)
        metrics["calculated_at"] = calculated_at.isoformat()
        stage("processing")
        result = process_parquet(spark, work / "ingested.parquet", run / "results", work,
                                 batch_id=run_id, calculated_at=calculated_at,
                                 config=config, progress=checkpoint)
        checkpoint(result.pop("counts"))
        metrics.update(result)
        counts = metrics["counts"]
        counts["rejected"] = counts["ingestion_rejected"] + counts["spark_rejected"]
        metrics["checks"].update({
            "requested_equals_generated": counts["requested"] == counts["generated"],
            "generated_equals_received": counts["generated"] == counts["received"],
            "ingestion_balance": counts["received"] == counts["ingestion_valid"] + counts["ingestion_rejected"],
            "ingested_parquet_matches": parquet_rows(work / "ingested.parquet") == counts["ingestion_valid"],
            "spark_received_all_ingested": counts["spark_received"] == counts["ingestion_valid"],
            "end_to_end_balance": counts["received"] == counts["valid"] + counts["rejected"],
            "all_requested_valid": counts["valid"] == counts["requested"] and counts["rejected"] == 0,
        })
        if not all(metrics["checks"].values()):
            raise ValueError(f"Conteos no verificados: {metrics['checks']}")
        metrics["status"] = "success"
    except KeyboardInterrupt:
        metrics["status"] = "interrupted"
        metrics["errors"].append({"stage": metrics["stage"], "type": "KeyboardInterrupt",
                                  "message": "Prueba detenida por el usuario; artefactos parciales."})
    except Exception as error:
        metrics["status"] = "failed"
        metrics["errors"].append({"stage": metrics["stage"], "type": type(error).__name__, "message": str(error)})
        (run / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
    finally:
        if metrics["stage"] in metrics["seconds"]:
            metrics["seconds"][metrics["stage"]] = perf_counter() - stage_started
        try:
            if spark is not None:
                spark.stop()
            metrics["sizes"]["intermediate_parquet_bytes"] = size_bytes(work, {".parquet"})
            if metrics["status"] == "success":
                # Única eliminación: temporales creados por esta misma corrida, tras verificarla.
                if work.resolve().parent != run.resolve():
                    raise ValueError("Ruta temporal fuera de la corrida.")
                shutil.rmtree(work)
        except Exception as error:
            metrics["status"] = "failed"
            metrics["errors"].append({"stage": "cleanup", "type": type(error).__name__, "message": str(error)})
        metrics["sizes"].update(jsonl_bytes=size_bytes(source) + size_bytes(run / "ingestion_rejected.jsonl")
                                + size_bytes(run / "results" / "alerts", {".jsonl"}),
                                source_jsonl_bytes=size_bytes(source),
                                parquet_bytes=size_bytes(run / "results", {".parquet"}),
                                retained_intermediate_bytes=size_bytes(work))
        metrics["seconds"]["total"] = perf_counter() - started
        metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
        save_metrics(run, metrics, final=True)
    return run, metrics
