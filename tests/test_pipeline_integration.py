"""Prueba optativa con los tres servicios de Compose levantados."""

import base64
from contextlib import closing
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import pandas as pd
import pytest

from scripts.run_demo import load_batch, prepare_batch
from src.processing.export_results import ALERTS_FILE, METRICS_FILE, RISK_FILE
from src.processing.risk_rules import RiskThresholds
from src.storage.load_results import get_mongo_client, get_pg_connection, load_dataframe


pytestmark = pytest.mark.skipif(os.getenv("BD3_INTEGRATION") != "1", reason="Requiere Compose y BD3_INTEGRATION=1")


def fetch(query, params):
    with closing(get_pg_connection()) as connection, connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


def grafana_panels(machine_id):
    port = os.getenv("GRAFANA_PORT", "3000")
    base_url = os.getenv("GRAFANA_URL", f"http://localhost:{port}")
    user = os.getenv("GRAFANA_USER", "admin")
    auth = base64.b64encode(f'{user}:{os.environ["GRAFANA_PASSWORD"]}'.encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    with urlopen(Request(f"{base_url}/api/dashboards/uid/bd3-machines-dashboard", headers=headers), timeout=10) as response:
        panels = json.load(response)["dashboard"]["panels"]
    queries = []
    for panel in panels:
        query = panel["targets"][0].copy()
        query["refId"] = str(panel["id"])
        query["datasource"] = panel["datasource"]
        query["rawSql"] = query["rawSql"].replace("$machine", f"'{machine_id}'")
        queries.append(query)
    body = json.dumps({"from": "1792886400000", "to": "1792972800000", "queries": queries}).encode()
    with urlopen(Request(f"{base_url}/api/ds/query", data=body, headers=headers), timeout=10) as response:
        return json.load(response)["results"]


def test_real_pipeline_persistence_retry_and_dashboard(tmp_path, monkeypatch):
    suffix = uuid4().hex[:12]
    machine, other = f"PR4-A-{suffix}", f"PR4-B-{suffix}"
    new_machine = f"PR4-0-{suffix}"
    fixture = (Path(__file__).parent / "fixtures" / "pipeline_sample.jsonl").read_text(encoding="utf-8")
    source = tmp_path / "input.jsonl"
    source.write_text(fixture.replace("PR4-A", machine).replace("PR4-B", other), encoding="utf-8")
    batch = prepare_batch(tmp_path / "batches", source, "2026-10-25T00:00:00Z", 42, RiskThresholds())
    metrics = load_dataframe(batch / "results" / METRICS_FILE)
    risks = load_dataframe(batch / "results" / RISK_FILE)
    db_name = os.getenv("MONGO_DB", "mantenimiento_predictivo")
    with get_mongo_client() as mongo:
        alerts = mongo[db_name]["alerts"]
        try:
            # PostgreSQL confirma aun cuando el primer intento no encuentra MongoDB.
            with monkeypatch.context() as env:
                env.setenv("MONGO_PORT", "1")
                with pytest.raises(RuntimeError, match="MongoDB quedó pendiente"):
                    load_batch(batch, batch.name)
            pg_count = "SELECT COUNT(*) FROM sensor_metrics_5m WHERE machine_id = %s"
            assert fetch(pg_count, (machine,)) == [(3,)]
            assert alerts.count_documents({"machine_id": machine}) == 0
            load_batch(batch, batch.name)
            load_batch(batch, batch.name)
            assert fetch(pg_count, (machine,)) == [(3,)]
            assert {doc["level"] for doc in alerts.find({"machine_id": machine})} == {"AMARILLO", "ROJO"}
            assert alerts.count_documents({"machine_id": machine}) == 2

            older = tmp_path / "older" / "results"
            older.mkdir(parents=True)
            old_window = pd.Timestamp("2026-10-25T11:55:00Z")
            metric_row = metrics[metrics["machine_id"] == machine].iloc[[0]]
            metric_row.assign(window_start=old_window).to_parquet(older / METRICS_FILE, index=False)
            risks[risks["machine_id"] == machine].assign(
                window_start=old_window, calculated_at=pd.Timestamp("2030-01-01T00:00:00Z"), risk_level="ROJO",
            ).to_parquet(older / RISK_FILE, index=False)
            (older / ALERTS_FILE).write_text(json.dumps({
                "machine_id": machine, "timestamp": "2026-10-25T11:55:00Z", "level": "ROJO", "description": "Lote previo",
            }) + "\n", encoding="utf-8")
            load_batch(older.parent, "older-" + suffix)
            latest = fetch("SELECT risk_level, window_start FROM machine_risk_latest WHERE machine_id = %s", (machine,))
            assert latest == [("VERDE", pd.Timestamp("2026-10-25T12:10:00Z"))]
            assert fetch(pg_count, (machine,)) == [(4,)]
            assert alerts.count_documents({"machine_id": machine}) == 3

            overlap = tmp_path / "overlap" / "results"
            overlap.mkdir(parents=True)
            pd.concat([metric_row.assign(machine_id=new_machine), metric_row.assign(avg_temp=99.0)]).to_parquet(
                overlap / METRICS_FILE, index=False,
            )
            risks.iloc[:0].to_parquet(overlap / RISK_FILE, index=False)
            (overlap / ALERTS_FILE).write_text("", encoding="utf-8")
            with pytest.raises(ValueError, match="Ventana de otro lote"):
                load_batch(overlap.parent, "overlap-" + suffix)
            assert fetch("SELECT machine_id FROM machines WHERE machine_id = %s", (new_machine,)) == []
            assert fetch(pg_count, (machine,)) == [(4,)]

            panels = grafana_panels(machine)
            assert set(panels) == {"1", "2", "3"}
            assert all(result["status"] == 200 and result["frames"] for result in panels.values())
            values = panels["1"]["frames"][0]["data"]["values"]
            assert values[0] == [machine] and values[2] == ["VERDE"]
            assert all(len(panels[panel]["frames"][0]["data"]["values"][0]) == 4 for panel in ("2", "3"))
        finally:
            owned_ids = [machine, other, new_machine]
            with closing(get_pg_connection()) as connection, connection, connection.cursor() as cursor:
                cursor.execute("DELETE FROM machines WHERE machine_id = ANY(%s)", (owned_ids,))
            alerts.delete_many({"machine_id": {"$in": owned_ids}})
