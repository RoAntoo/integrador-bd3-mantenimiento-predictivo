from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from scripts import run_benchmark_demo as demo
from src.benchmark.presentation import dashboard_url


def test_url_selects_the_run_and_actual_date_range(monkeypatch):
    monkeypatch.setenv("GRAFANA_URL", "http://localhost:3000/")
    payload = {"run_id": "example", "from": "2026-10-25T00:00:00+00:00",
               "to": "2026-10-25T08:20:00+00:00"}
    url = urlparse(dashboard_url(payload))
    query = parse_qs(url.query)
    assert url.path == "/d/bd3-benchmark-dashboard"
    assert query["var-run"] == ["example"]
    assert query["from"] == [payload["from"]]
    assert query["to"] == [payload["to"]]
    assert query["timezone"] == ["utc"]


def test_shortcut_publishes_only_its_completed_run(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(demo, "check_services", lambda: None)

    def run(config):
        seen["config"] = config
        return tmp_path, {"status": "success"}

    monkeypatch.setattr(demo, "run_benchmark", run)
    monkeypatch.setattr(demo, "publish_main", lambda options: seen.update(options=options) or 0)
    assert demo.main(["--machines", "10", "--readings-per-machine", "500", "--open"]) == 0
    assert seen["config"].machines * seen["config"].readings_per_machine == 5000
    assert seen["options"] == ["--run-dir", str(tmp_path), "--open"]


@pytest.mark.parametrize("status, code", [("failed", 1), ("interrupted", 130)])
def test_shortcut_never_publishes_failed_or_interrupted_runs(monkeypatch, status, code):
    monkeypatch.setattr(demo, "check_services", lambda: None)
    monkeypatch.setattr(demo, "run_benchmark", lambda config: (Path("test-run"), {"status": status}))
    monkeypatch.setattr(demo, "publish_main", lambda options: pytest.fail("Publicación de corrida incompleta"))
    assert demo.main(["--machines", "1", "--readings-per-machine", "1"]) == code


def test_services_checked_before_generation(monkeypatch):
    def fail():
        raise RuntimeError("servicios apagados")
    monkeypatch.setattr(demo, "check_services", fail)
    monkeypatch.setattr(demo, "run_benchmark", lambda config: pytest.fail("No debe generar"))
    assert demo.main(["--machines", "10", "--readings-per-machine", "500"]) == 1
