import json
import hashlib
from dataclasses import asdict
from pathlib import Path

from scripts.run_demo import MANIFEST, prepare_batch
from src.processing.export_results import ALERTS_FILE, METRICS_FILE, RISK_FILE
from src.processing.risk_rules import RiskThresholds
from src.storage.load_results import load_dataframe


def test_export_preserves_quality_utc_and_historical_alerts(tmp_path):
    """Verificar los contratos de exportación y las alertas de ventanas anteriores."""
    source = Path(__file__).parent / "fixtures" / "pipeline_sample.jsonl"
    arguments = (tmp_path, source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    legacy_identity = {"source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                       "thresholds": asdict(RiskThresholds())}
    legacy_id = hashlib.sha256(json.dumps(legacy_identity, sort_keys=True).encode("utf-8")).hexdigest()
    legacy_dir = tmp_path / legacy_id
    legacy_dir.mkdir()
    (legacy_dir / MANIFEST).write_text(json.dumps(legacy_identity), encoding="utf-8")
    batch = prepare_batch(*arguments)
    assert batch != legacy_dir
    assert json.loads((legacy_dir / MANIFEST).read_text(encoding="utf-8")) == legacy_identity
    manifest_path = batch / MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["pipeline_version"] == 5
    assert prepare_batch(*arguments) == batch
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert manifest["ingestion"]["rejected"] == 1
    assert manifest["quality"] == {"received": 7, "valid": 4, "rejected": 3, "duplicates": 3}
    assert manifest["balance"] == {"received": 8, "ingestion_rejected": 1, "spark_rejected": 3, "valid": 4}
    ingestion_rejected = json.loads((batch / "results" / "ingestion_rejected.jsonl").read_text(encoding="utf-8"))
    assert ingestion_rejected["line_number"] == 8
    assert ingestion_rejected["raw_line"] == source.read_text(encoding="utf-8").splitlines()[7]
    assert list((batch / "lake" / "historical").rglob("*.parquet"))

    results = batch / "results"
    metrics = load_dataframe(results / METRICS_FILE)
    assert set(metrics.columns) == {"machine_id", "window_start", "avg_temp", "max_vibration", "avg_pressure", "n_readings"}
    assert str(metrics["window_start"].dt.tz) == "UTC"
    assert metrics["n_readings"].sum() == 4
    assert set(load_dataframe(results / RISK_FILE)["risk_level"]) == {"VERDE"}
    assert len(load_dataframe(results / "clean_telemetry.parquet")) == 4
    assert len(load_dataframe(results / "rejected_telemetry.parquet")) == 3
    rejected = load_dataframe(results / "rejected_telemetry.parquet")
    assert set(rejected["line_number"]) == {1, 6, 7}
    originals = source.read_text(encoding="utf-8").splitlines()
    assert all(row.raw_line == originals[row.line_number - 1] for row in rejected.itertuples())
    assert len(load_dataframe(results / "machine_statistics.parquet")) == 2

    alerts = [json.loads(line) for line in (results / ALERTS_FILE).read_text(encoding="utf-8").splitlines()]
    assert {(alert["machine_id"], alert["timestamp"], alert["level"]) for alert in alerts} == {
        ("PR4-A", "2026-10-25T12:00:00Z", "AMARILLO"), ("PR4-A", "2026-10-25T12:05:00Z", "ROJO"),
    }
    assert len(alerts) == 2 and all(alert["description"] for alert in alerts)


def test_green_batch_exports_an_empty_alert_file(tmp_path):
    """Verificar que un lote VERDE tenga un JSONL de alertas vacío."""
    source = tmp_path / "green.jsonl"
    source.write_text(json.dumps({
        "machine_id": "PR4-GREEN", "timestamp": "2026-10-25T12:00:00Z",
        "temperature": 60, "vibration": 2, "pressure": 5,
    }) + "\n", encoding="utf-8")
    batch = prepare_batch(tmp_path / "batches", source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    assert (batch / "results" / ALERTS_FILE).read_bytes() == b""


def test_unit_names_and_conflicts_through_pipeline(tmp_path):
    BASE = {"machine_id": "PR5-A", "timestamp": "2026-10-25T12:00:00Z"}
    SHORT = {"temperature": 60, "vibration": 2, "pressure": 5}
    UNITS = {"temperature_c": 60, "vibration_mm_s": 2, "pressure_bar": 5}

    source = tmp_path / "aliases.jsonl"
    rows = [BASE | UNITS, BASE | SHORT, BASE | SHORT | {"temperature_c": 99}]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    batch = prepare_batch(tmp_path / "batches", source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    manifest = json.loads((batch / MANIFEST).read_text(encoding="utf-8"))
    assert manifest["balance"] == {"received": 3, "ingestion_rejected": 1, "spark_rejected": 1, "valid": 1}
    rejected = load_dataframe(batch / "results" / "rejected_telemetry.parquet")
    assert json.loads(rejected.iloc[0].raw_line) in rows[:2]
    assert rejected.iloc[0].line_number in (1, 2)


def test_all_rejected_pipeline_publishes_audit(tmp_path):
    source = tmp_path / "invalid.jsonl"
    source.write_text("{broken\n", encoding="utf-8")
    batch = prepare_batch(tmp_path / "batches", source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    manifest = json.loads((batch / MANIFEST).read_text(encoding="utf-8"))
    assert manifest["balance"] == {"received": 1, "ingestion_rejected": 1, "spark_rejected": 0, "valid": 0}
    assert load_dataframe(batch / "results" / "clean_telemetry.parquet").empty
