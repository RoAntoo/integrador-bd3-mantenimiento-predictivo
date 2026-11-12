"""Evidencia integrada: cuarentenas, reconciliación, curado y lotes sin válidos."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from scripts.run_demo import load_batch, prepare_batch, reconcile_counts
from src.generator.generate import GenerationConfig, generate_telemetry
from src.jsonl import read_jsonl_lines
from src.processing.quality import QualityMetrics
from src.processing.risk_rules import RiskThresholds
from src.storage.load_results import upsert_postgres


FIXTURE = Path(__file__).parent / "fixtures" / "dirty_pipeline_sample.jsonl"


def run_batch(tmp_path, source, **kwargs):
    return prepare_batch(tmp_path / "batches", source, "2026-10-25T00:00:00Z", 42,
                         RiskThresholds(), **kwargs)


def manifest(batch):
    return json.loads((batch / "batch.json").read_text(encoding="utf-8"))


def test_ten_lines_balance_audit_and_anomalies(tmp_path):
    batch = run_batch(tmp_path, FIXTURE)
    report = manifest(batch)
    assert report["reconciliation"] == {
        "received": 10, "ingestion_rejected": 5, "spark_rejected": 3,
        "final_valid": 2, "rejected_total": 8, "balanced": True,
    }
    assert report["quality"] == {"received": 5, "valid": 2, "rejected": 3, "duplicates": 3}
    assert report["contamination"]["selected_lines"] == report["contamination"]["modified_lines"] == 0
    originals = read_jsonl_lines(FIXTURE)
    rejected_ingestion = [json.loads(line) for line in (batch / "results" / "ingestion_rejected.jsonl").read_text(encoding="utf-8").splitlines()]
    rejected_spark = pq.read_table(batch / "results" / "rejected_telemetry.parquet").to_pylist()
    assert [r["rejection_reasons"][0] for r in rejected_ingestion] == [
        "MISSING_TEMPERATURE", "NULL_VIBRATION", "INVALID_TYPE_TEMPERATURE", "INVALID_TIMESTAMP", "MALFORMED_JSON",
    ]
    assert {r["line_number"] for r in rejected_spark} == {7, 8, 9}
    assert {r["stage"] for r in rejected_ingestion} == {"ingestion"}
    assert {r["stage"] for r in rejected_spark} == {"spark"}
    for row in rejected_ingestion + rejected_spark:
        assert row["batch_id"] == report["batch_id"]
        assert row["raw_line"] == originals[row["line_number"] - 1]
        assert row["rejection_reasons"]
    clean = pq.read_table(batch / "results" / "clean_telemetry.parquet").to_pylist()
    assert {r["machine_id"] for r in clean} == {"DIRTY-COPY", "DIRTY-ANOMALY"}
    assert all(r["data_quality_flag"] == "VALID" for r in clean)
    metrics = pq.read_table(batch / "results" / "sensor_metrics_5m.parquet").to_pylist()
    assert sum(row["n_readings"] for row in metrics) == 2
    assert {row["machine_id"] for row in metrics} == {"DIRTY-COPY", "DIRTY-ANOMALY"}
    risks = pq.read_table(batch / "results" / "machine_risk_latest.parquet").to_pylist()
    assert {row["machine_id"]: row["risk_level"] for row in risks} == {"DIRTY-COPY": "VERDE", "DIRTY-ANOMALY": "ROJO"}
    alerts = [json.loads(line) for line in (batch / "results" / "alerts.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(alerts) == 1 and alerts[0]["machine_id"] == "DIRTY-ANOMALY"
    historical = [row for path in (batch / "lake" / "historical").rglob("*.parquet") for row in pq.ParquetFile(path).read().to_pylist()]
    assert historical == clean
    # Reintentar devuelve el lote publicado y conserva exactamente su auditoría.
    assert run_batch(tmp_path, FIXTURE) == batch
    assert manifest(batch) == report


def test_optional_contamination_and_normal_generation(tmp_path):
    normal = run_batch(tmp_path, None)
    dirty = run_batch(tmp_path, None, dirty_rate=.1, dirty_seed=42)
    report = manifest(dirty)
    assert manifest(normal)["reconciliation"] == {
        "received": 180, "ingestion_rejected": 0, "spark_rejected": 0,
        "final_valid": 180, "rejected_total": 0, "balanced": True,
    }
    standalone = tmp_path / "standalone.jsonl"
    generate_telemetry(GenerationConfig(machines=3, days=1 / 24, seed=42, output=standalone))
    assert standalone.read_bytes() == (normal / "telemetry.jsonl").read_bytes()
    assert standalone.read_bytes() == (dirty / "telemetry_original.jsonl").read_bytes()
    assert report["contamination"]["selected_lines"] == 18
    assert report["contamination"]["modified_lines"] == 14
    assert report["reconciliation"] == {
        "received": 180, "ingestion_rejected": 10, "spark_rejected": 6,
        "final_valid": 164, "rejected_total": 16, "balanced": True,
    }
    assert sum(pq.read_table(dirty / "results" / "sensor_metrics_5m.parquet")["n_readings"].to_pylist()) == 164


@pytest.mark.parametrize("case", ["ingestion", "spark", "empty"])
def test_no_valid_outputs_keep_schemas_and_skip_storage(tmp_path, case):
    source = tmp_path / "input.jsonl"
    if case == "ingestion":
        source.write_text('bad json\n{"machine_id":null}\n', encoding="utf-8")
        expected = (2, 0)
    elif case == "spark":
        source.write_text("\n".join(read_jsonl_lines(FIXTURE)[7:9]) + "\n", encoding="utf-8")
        expected = (0, 2)
    else:
        source.write_bytes(b"")
        expected = (0, 0)
    batch = run_batch(tmp_path, source)
    report = manifest(batch)
    assert report["reconciliation"]["final_valid"] == 0
    assert report["reconciliation"]["balanced"]
    assert (report["reconciliation"]["ingestion_rejected"], report["reconciliation"]["spark_rejected"]) == expected
    for name, fields in (
        ("clean_telemetry.parquet", ["machine_id", "timestamp", "temperature", "vibration", "pressure", "data_quality_flag"]),
        ("sensor_metrics_5m.parquet", ["machine_id", "window_start", "avg_temp", "max_vibration", "avg_pressure", "n_readings"]),
        ("machine_risk_latest.parquet", ["machine_id", "window_start", "calculated_at", "risk_level", "risk_reason"]),
    ):
        table = pq.read_table(batch / "results" / name)
        assert table.num_rows == 0 and table.column_names == fields
        assert all(str(field.type) != "null" for field in table.schema)
    assert (batch / "results" / "alerts.jsonl").read_bytes() == b""
    with patch("src.storage.load_results.get_pg_connection") as connection, patch("src.storage.load_results.get_mongo_client") as mongo:
        load_batch(batch, report["batch_id"])
        upsert_postgres(batch / "results" / "sensor_metrics_5m.parquet", batch / "results" / "machine_risk_latest.parquet", batch_id=report["batch_id"])
        connection.assert_not_called()
        mongo.assert_not_called()


def test_unit_names_and_noncanonical_timestamp_keep_original_audit(tmp_path):
    source = tmp_path / "aliases.jsonl"
    records = [dict(machine_id="A", timestamp="2026-10-25 12:00:00+00:00", temperature_c=60, vibration_mm_s=2, pressure_bar=5),
               dict(machine_id="A", timestamp="2026-10-25T12:00:00Z", temperature=110, vibration=2, pressure=5)]
    lines = [json.dumps(record) for record in records]
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    batch = run_batch(tmp_path, source)
    assert manifest(batch)["quality"] == {"received": 2, "valid": 0, "rejected": 2, "duplicates": 2}
    rejected = pq.read_table(batch / "results" / "rejected_telemetry.parquet").to_pylist()
    assert {row["raw_line"] for row in rejected} == set(lines)
    assert all(row["rejection_reasons"] == ["CONFLICTING_DUPLICATE_KEY"] for row in rejected)


def test_failed_reconciliation_prevents_publication():
    ingestion = SimpleNamespace(total_received=10, invalid_records=5, valid_records=5)
    with pytest.raises(RuntimeError, match="no reconcilian"):
        reconcile_counts(ingestion, QualityMetrics(4, 2, 2, 2))
