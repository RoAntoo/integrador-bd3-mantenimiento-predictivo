"""Pruebas pequeñas del benchmark; ninguna inicia Docker ni una carga masiva."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from scripts.run_benchmark import main
from scripts.run_demo import MANIFEST, prepare_batch
from src.benchmark.generation import generate_jsonl
from src.benchmark.ingestion import ingest_jsonl
from src.benchmark.runner import BenchmarkConfig, run_benchmark
from src.ingestion.ingest import validate_and_normalize_record
from src.processing.risk_rules import RiskThresholds


@pytest.mark.parametrize("rate", [0, 0.05, 1])
def test_generation_exact_counts_reproducible_and_contract(tmp_path, rate):
    first, second = tmp_path / "first.jsonl", tmp_path / "second.jsonl"
    config = dict(machines=3, readings_per_machine=60, seed=73, anomaly_rate=rate)
    result = generate_jsonl(first, **config)
    generate_jsonl(second, **config)
    assert first.read_bytes() == second.read_bytes()
    rows = [json.loads(line) for line in first.read_text(encoding="utf-8").splitlines()]
    assert result["generated"] == len(rows) == 180
    assert result["anomalous"] == sum(row["is_anomaly"] for row in rows) == round(180 * rate)
    assert len({(row["machine_id"], row["timestamp"]) for row in rows}) == 180
    for row in rows:
        validate_and_normalize_record(json.dumps(row))
    with pytest.raises(FileExistsError):
        generate_jsonl(first, **config)


def test_ingestion_streams_limits_buffer_and_preserves_audit(tmp_path, monkeypatch):
    source = tmp_path / "input.jsonl"
    source.write_bytes((Path(__file__).parent / "fixtures" / "pipeline_sample.jsonl").read_bytes())
    # Una lectura masiva de texto accidental hace fallar esta prueba.
    monkeypatch.setattr(Path, "read_text", lambda *a, **kw: pytest.fail("Lectura completa del JSONL"))
    output, rejected = tmp_path / "ingested", tmp_path / "rejected.jsonl"
    counts = ingest_jsonl(source, output, rejected, batch_id="test", batch_size=2, rows_per_file=3)
    assert counts["received"] == 8
    assert counts["ingestion_valid"] == 7
    assert counts["ingestion_rejected"] == 1
    assert counts["max_buffered_records"] <= 2
    parts = sorted(output.glob("*.parquet"))
    assert [pq.ParquetFile(p).metadata.num_rows for p in parts] == [3, 3, 1]
    with rejected.open(encoding="utf-8") as stream:
        audit = json.loads(next(stream))
    assert audit["line_number"] == 8 and audit["batch_id"] == "test"
    assert audit["reason"] and audit["raw_line"]


def test_all_rejected_ingestion_writes_typed_empty_parquet(tmp_path):
    source = tmp_path / "input.jsonl"
    source.write_text("{broken\n\n", encoding="utf-8")
    counts = ingest_jsonl(source, tmp_path / "ingested", tmp_path / "rejects.jsonl", batch_id="empty")
    assert counts["received"] == counts["ingestion_rejected"] == 2
    empty = pq.ParquetFile(tmp_path / "ingested" / "part-000000.parquet")
    assert empty.metadata.num_rows == 0
    assert "timestamp" in empty.schema_arrow.names


@pytest.mark.parametrize("kwargs", [
    {"machines": 0}, {"readings_per_machine": -1}, {"machines": 1501, "readings_per_machine": 10000},
    {"batch_size": 100001}, {"rows_per_file": 2}, {"spark_memory": "invalid"}, {"anomaly_rate": 2},
])
def test_invalid_config_creates_no_run(tmp_path, kwargs):
    with pytest.raises(ValueError):
        BenchmarkConfig(**(dict(machines=3, readings_per_machine=60, output_dir=tmp_path) | kwargs))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("error, status", [(RuntimeError("fallo simulado"), "failed"),
                                          (KeyboardInterrupt(), "interrupted")])
def test_failure_and_interruption_never_report_success(tmp_path, monkeypatch, error, status):
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr("src.benchmark.runner.generate_jsonl", fail)
    config = BenchmarkConfig(3, 60, output_dir=tmp_path)
    first, metrics = run_benchmark(config)
    original = (first / "metrics.json").read_bytes()
    second, _ = run_benchmark(config)
    assert first != second
    assert (first / "metrics.json").read_bytes() == original
    assert metrics["status"] == status and metrics["errors"]
    assert metrics["seconds"]["generation"] is not None
    assert (first / "metrics.csv").is_file()
    assert not (first / "results").exists()


def test_cli_rejects_database_load_option():
    with pytest.raises(SystemExit) as error:
        main(["--machines", "3", "--readings-per-machine", "60", "--load"])
    assert error.value.code == 2


def test_generation_interruption_preserves_partial_count(tmp_path, monkeypatch):
    from src.benchmark import generation
    original = generation._simulate_reading
    calls = 0

    def interrupt(*args):
        nonlocal calls
        calls += 1
        if calls == 5:
            raise KeyboardInterrupt
        return original(*args)

    monkeypatch.setattr(generation, "_simulate_reading", interrupt)
    run, metrics = run_benchmark(BenchmarkConfig(3, 60, output_dir=tmp_path))
    assert metrics["status"] == "interrupted"
    assert metrics["counts"]["generated"] == 4
    assert len((run / "telemetry.jsonl").read_text(encoding="utf-8").splitlines()) == 4


def test_partial_generation_is_failure_before_starting_spark(tmp_path, monkeypatch):
    def partial(source, **kwargs):
        source.write_text('{}\n', encoding="utf-8")
        return {"generated": 1}
    monkeypatch.setattr("src.benchmark.runner.generate_jsonl", partial)
    run, metrics = run_benchmark(BenchmarkConfig(3, 60, output_dir=tmp_path))
    assert metrics["status"] == "failed"
    assert metrics["stage"] == "generation"
    assert metrics["counts"]["generated"] == 1
    assert metrics["seconds"]["spark_startup"] is None
    assert (run / "error.log").is_file()


def test_small_real_spark_benchmark_and_default_180_regression(tmp_path, monkeypatch):
    def forbid(*args, **kwargs):
        pytest.fail("El benchmark no debe conectar bases de datos")
    monkeypatch.setattr("scripts.run_demo.load_batch", forbid)
    monkeypatch.setattr("src.storage.load_results.upsert_postgres", forbid)
    monkeypatch.setattr("src.storage.load_results.upsert_mongodb", forbid)
    run, metrics = run_benchmark(BenchmarkConfig(3, 60, output_dir=tmp_path / "benchmarks",
                                                batch_size=17, rows_per_file=50, shuffle_partitions=2))
    assert metrics["status"] == "success", metrics["errors"]
    assert all(metrics["checks"].values())
    assert {k: metrics["counts"][k] for k in ("requested", "generated", "received", "valid")} == dict.fromkeys(
        ("requested", "generated", "received", "valid"), 180)
    assert metrics["counts"]["rejected"] == 0
    assert metrics["sizes"]["parquet_bytes"] > 0 and metrics["sizes"]["source_jsonl_bytes"] > 0
    assert not (run / "work").exists()
    assert metrics["window_readings_sum"] == 180
    assert all(pq.ParquetFile(part).metadata.num_rows <= 50
               for part in (run / "results" / "clean_telemetry.parquet").glob("*.parquet"))
    # prepare_batch usa exactamente el preset invocado por scripts.run_demo sin --input.
    normal = prepare_batch(tmp_path / "normal", None, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    manifest = json.loads((normal / MANIFEST).read_text(encoding="utf-8"))
    assert manifest["balance"] == {"received": 180, "ingestion_rejected": 0, "spark_rejected": 0, "valid": 180}
    assert pq.ParquetFile(normal / "results" / "clean_telemetry.parquet").metadata.num_rows == 180


def test_parquet_quality_preserves_duplicates_conflicts_and_line_numbers(tmp_path):
    from pyspark.sql import functions as F
    from src.benchmark.processing import read_parquet
    from src.processing.quality import clean_telemetry_frame
    from src.processing.spark import create_spark_session

    source = Path(__file__).parent / "fixtures" / "pipeline_sample.jsonl"
    ingest_jsonl(source, tmp_path / "ingested", tmp_path / "rejects.jsonl", batch_id="dirty",
                 batch_size=2, rows_per_file=3)
    spark = create_spark_session()
    try:
        parsed = (read_parquet(spark, tmp_path / "ingested")
                  .withColumn("batch_id", F.lit("dirty")).withColumn("_parse_error", F.lit(False)))
        quality = clean_telemetry_frame(parsed)
        assert (quality.metrics.received, quality.metrics.valid, quality.metrics.rejected,
                quality.metrics.duplicates) == (7, 4, 3, 3)
        rejected = list(quality.rejected.orderBy("line_number").toLocalIterator())
        assert [row.line_number for row in rejected] == [1, 6, 7]
        assert [row.rejection_reasons for row in rejected] == [
            ["DUPLICATE_KEY"], ["CONFLICTING_DUPLICATE_KEY"], ["CONFLICTING_DUPLICATE_KEY"]]
    finally:
        spark.stop()
