from copy import deepcopy
from datetime import datetime, timezone
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.benchmark.presentation import build_dashboard, prepare_presentation, validate_presentation


@pytest.fixture
def run(tmp_path):
    report = {"run_id": "small", "status": "success", "errors": [], "checks": {"balanced": True},
              "counts": dict.fromkeys(("requested", "generated", "received", "valid"), 20) | {"rejected": 0},
              "config": {"machines": 2, "readings_per_machine": 10},
              "stored_rows": {"sensor_metrics_5m.parquet": 4}}
    (tmp_path / "metrics.json").write_text(json.dumps(report), encoding="utf-8")
    stamp = datetime(2026, 10, 25, tzinfo=timezone.utc)
    rows = {
        "machine_statistics.parquet": [{"machine_id": key, "n_readings": 10} for key in ("MAQ-001", "MAQ-002")],
        "machine_risk_latest.parquet": [{"machine_id": key, "risk_level": "VERDE"} for key in ("MAQ-001", "MAQ-002")],
        "sensor_metrics_5m.parquet": [{"machine_id": key, "window_start": stamp.replace(minute=minute),
            "n_readings": 5, "avg_temp": temperature, "max_vibration": 3., "avg_pressure": 5.}
            for key, temperature in (("MAQ-001", 60.), ("MAQ-002", 80.)) for minute in (0, 5)],
    }
    for name, data in rows.items():
        path = tmp_path / "results" / name
        path.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(data), path / "part-0.parquet")
    return tmp_path


def test_presentation_balances_saved_windows_and_uses_real_examples(run):
    payload = prepare_presentation(run)
    validate_presentation(payload)
    assert payload["verified_window_readings"] == 20
    assert [row["avg_temp"] for row in payload["fleet"]] == [70., 70.]
    assert len(payload["examples"]) == 4
    assert sum(row["n_readings"] for row in payload["fleet"]) == 20
    assert json.loads(json.dumps(payload)) == payload
    dashboard = build_dashboard(payload)
    assert len(dashboard["panels"]) == 11
    assert dashboard["time"]["from"].startswith("2026-10-25")
    assert dashboard["uid"] != "bd3-machines-dashboard"
    for panel in dashboard["panels"]:
        if panel["type"] == "timeseries":
            assert "WHERE $__timeFilter(time)" in panel["targets"][0]["rawSql"]


def test_partial_run_cannot_be_presented(run):
    path = run / "metrics.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    report["status"] = "running"
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="completas"):
        prepare_presentation(run)


def test_portable_summary_cannot_lose_readings(run):
    payload = deepcopy(prepare_presentation(run))
    payload["fleet"][0]["n_readings"] -= 1
    with pytest.raises(ValueError, match="conteos"):
        validate_presentation(payload)
