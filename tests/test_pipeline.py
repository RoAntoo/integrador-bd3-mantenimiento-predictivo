import json
from pathlib import Path

from scripts.run_demo import MANIFEST, prepare_batch
from src.processing.export_results import ALERTS_FILE, METRICS_FILE, RISK_FILE
from src.processing.risk_rules import RiskThresholds
from src.storage.load_results import load_dataframe


def test_export_preserves_quality_utc_and_historical_alerts(tmp_path):
    """Verificar los contratos de exportación y las alertas de ventanas anteriores."""
    source = Path(__file__).parent / "fixtures" / "pipeline_sample.jsonl"
    arguments = (tmp_path, source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    batch = prepare_batch(*arguments)
    manifest_path = batch / MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert prepare_batch(*arguments) == batch
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert manifest["ingestion"]["rejected"] == 1
    assert manifest["quality"] == {"received": 7, "valid": 4, "rejected": 3, "duplicates": 3}
    assert list((batch / "lake" / "historical").rglob("*.parquet"))

    results = batch / "results"
    metrics = load_dataframe(results / METRICS_FILE)
    assert set(metrics.columns) == {"machine_id", "window_start", "avg_temp", "max_vibration", "avg_pressure", "n_readings"}
    assert str(metrics["window_start"].dt.tz) == "UTC"
    assert metrics["n_readings"].sum() == 4
    assert set(load_dataframe(results / RISK_FILE)["risk_level"]) == {"VERDE"}
    assert len(load_dataframe(results / "clean_telemetry.parquet")) == 4
    assert len(load_dataframe(results / "rejected_telemetry.parquet")) == 3
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
