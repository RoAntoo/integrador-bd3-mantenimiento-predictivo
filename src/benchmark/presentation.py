"""Resumir artefactos terminados para Grafana, sin Spark ni lecturas crudas."""

from collections import defaultdict
from datetime import timedelta
import json
from pathlib import Path
import base64
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pyarrow.parquet as pq


def iter_rows(directory: Path, columns=None):
    files = sorted(directory.glob("*.parquet"))
    if not files:
        raise ValueError(f"Faltan partes Parquet en {directory}")
    for part in files:
        for batch in pq.ParquetFile(part).iter_batches(batch_size=10000, columns=columns):
            yield from batch.to_pylist()


def prepare_presentation(run: Path) -> dict:
    report = json.loads((run / "metrics.json").read_text(encoding="utf-8"))
    counts, config = report["counts"], report["config"]
    if (report["status"] != "success" or not report["checks"]
            or not all(value is True for value in report["checks"].values())
            or report.get("errors") or counts["rejected"] != 0
            or len({counts[k] for k in ("requested", "generated", "received", "valid")}) != 1
            or counts["valid"] != config["machines"] * config["readings_per_machine"]):
        raise ValueError("Solo se publican corridas completas, verificadas y sin rechazos.")
    results = run / "results"
    statistics = {row["machine_id"]: row for row in iter_rows(results / "machine_statistics.parquet")}
    risks = {row["machine_id"]: row for row in iter_rows(results / "machine_risk_latest.parquet")}
    if (len(statistics) != config["machines"] or set(statistics) != set(risks)
            or sum(row["n_readings"] for row in statistics.values()) != counts["valid"]
            or any(row["n_readings"] != config["readings_per_machine"] for row in statistics.values())):
        raise ValueError("Los resúmenes de máquinas no concilian con el benchmark.")
    examples = set(sorted(statistics)[:3])
    machines = [dict(statistics[key], **{name: value for name, value in risks[key].items()
                                       if name != "machine_id"}) for key in sorted(statistics)]
    fleet = defaultdict(lambda: {"n_readings": 0, "temp_sum": 0.0, "pressure_sum": 0.0,
                                  "max_vibration": 0.0})
    detail, metric_rows, readings = [], 0, 0
    columns = ["machine_id", "window_start", "n_readings", "avg_temp", "max_vibration", "avg_pressure"]
    for row in iter_rows(results / "sensor_metrics_5m.parquet", columns):
        metric_rows += 1
        n = row["n_readings"]
        readings += n
        value = fleet[row["window_start"]]
        value["n_readings"] += n
        value["temp_sum"] += row["avg_temp"] * n
        value["pressure_sum"] += row["avg_pressure"] * n
        value["max_vibration"] = max(value["max_vibration"], row["max_vibration"])
        if row["machine_id"] in examples:
            detail.append(row)
    if readings != counts["valid"] or metric_rows != report["stored_rows"]["sensor_metrics_5m.parquet"]:
        raise ValueError("Las ventanas guardadas no cubren todas las lecturas verificadas.")
    fleet_rows = [dict(window_start=stamp, n_readings=v["n_readings"],
                       avg_temp=v["temp_sum"] / v["n_readings"],
                       avg_pressure=v["pressure_sum"] / v["n_readings"],
                       max_vibration=v["max_vibration"]) for stamp, v in sorted(fleet.items())]
    payload = {"format_version": 1, "run_id": report["run_id"], "report": report,
               "machines": machines, "fleet": fleet_rows,
               "examples": sorted(detail, key=lambda row: (row["window_start"], row["machine_id"])),
               "example_machines": sorted(examples), "verified_window_readings": readings,
               "from": fleet_rows[0]["window_start"],
               "to": fleet_rows[-1]["window_start"] + timedelta(minutes=5)}
    # Fechas ISO para transportar este resumen a otra computadora sin llevar los Parquet.
    return json.loads(json.dumps(payload, default=lambda value: value.isoformat()))


def publish(payload: dict):
    from psycopg2.extras import Json
    from src.storage.load_results import get_pg_connection

    validate_presentation(payload)
    connection = get_pg_connection()
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS benchmark_presentations (
                run_id TEXT PRIMARY KEY, body JSONB NOT NULL,
                published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
            cursor.execute("INSERT INTO benchmark_presentations(run_id, body) VALUES (%s,%s) "
                           "ON CONFLICT (run_id) DO NOTHING", (payload["run_id"], Json(payload)))
            cursor.execute("SELECT body = %s::jsonb FROM benchmark_presentations WHERE run_id=%s",
                           (Json(payload), payload["run_id"]))
            if cursor.fetchone() != (True,):
                raise ValueError("Ya existe una publicación diferente para esa corrida; no se reemplazó.")
    finally:
        connection.close()


def validate_presentation(payload: dict):
    """Comprobar también el resumen portátil antes de escribir en PostgreSQL."""
    report = payload["report"]
    n = report["counts"]["valid"]
    if (report["status"] != "success" or report.get("errors") or not report["checks"]
            or not all(value is True for value in report["checks"].values())
            or payload["run_id"] != report["run_id"]
            or report["counts"]["rejected"] != 0
            or any(report["counts"][key] != n for key in ("requested", "generated", "received"))
            or n != report["config"]["machines"] * report["config"]["readings_per_machine"]
            or len(payload["machines"]) != report["config"]["machines"]
            or sum(row["n_readings"] for row in payload["machines"]) != n
            or sum(row["n_readings"] for row in payload["fleet"]) != n
            or payload["verified_window_readings"] != n):
        raise ValueError("El resumen portátil no conserva los conteos de la corrida verificada.")


def grafana_request(path: str, data=None):
    """Usar las credenciales locales sin imprimirlas ni guardarlas en los artefactos."""
    from dotenv import load_dotenv
    load_dotenv()
    base = os.getenv("GRAFANA_URL", f"http://localhost:{os.getenv('GRAFANA_PORT', '3000')}")
    auth = base64.b64encode((os.getenv("GRAFANA_USER", "admin") + ":" + os.environ["GRAFANA_PASSWORD"]).encode()).decode()
    request = Request(base + path, data=None if data is None else json.dumps(data).encode(),
                      headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def ensure_dashboard(dashboard: dict):
    """Crear por API si la provisión por archivos aún no lo registró; nunca reemplazar otro."""
    try:
        existing = grafana_request("/api/dashboards/uid/" + dashboard["uid"])
    except HTTPError as error:
        if error.code != 404:
            raise
        return grafana_request("/api/dashboards/db", {"dashboard": dashboard, "overwrite": False})
    if existing["dashboard"]["title"] != dashboard["title"]:
        raise ValueError("El UID pertenece a un dashboard diferente; no se reemplazó.")
    if existing["dashboard"]["panels"] != dashboard["panels"]:
        definition = dict(dashboard, id=existing["dashboard"].get("id"),
                          version=existing["dashboard"].get("version", 0) + 1)
        return grafana_request("/api/dashboards/db", {"dashboard": definition, "overwrite": True})
    return existing["meta"]


def build_dashboard(payload: dict) -> dict:
    ds = {"type": "postgres", "uid": "pg_bd3_uid"}
    base = " FROM benchmark_presentations WHERE run_id = ${run:sqlstring}"
    dashboard = {"uid": "bd3-benchmark-dashboard", "title": "BD3 | Benchmark de telemetría industrial",
                 "description": "Resultados reales de una corrida verificada; datos sintéticos. Sin reprocesamiento en vivo.",
                 "schemaVersion": 39, "version": 1, "timezone": "utc", "refresh": "",
                 "time": {"from": payload["from"], "to": payload["to"]},
                 "templating": {"list": [{"name": "run", "label": "Corrida verificada", "type": "query",
                    "datasource": ds, "query": "SELECT run_id FROM benchmark_presentations ORDER BY run_id DESC",
                    "refresh": 1, "current": {"text": payload["run_id"], "value": payload["run_id"]}}]},
                 "panels": []}

    def panel(title, kind, position, query, unit=None, options=None):
        item = {"id": len(dashboard["panels"]) + 1, "title": title, "type": kind,
                "gridPos": dict(zip(("x", "y", "w", "h"), position)), "datasource": ds,
                "targets": [{"refId": "A", "rawQuery": True, "rawSql": query,
                             "format": "time_series" if kind == "timeseries" else "table"}],
                "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
                "options": options or {}}
        if unit:
            item["fieldConfig"]["defaults"]["unit"] = unit
        dashboard["panels"].append(item)
        return item

    for i, (title, key, unit) in enumerate([
        ("Lecturas válidas procesadas", "counts,valid", "locale"),
        ("Máquinas simuladas", "config,machines", "locale"),
        ("Lecturas por máquina", "config,readings_per_machine", "locale"),
        ("Registros rechazados", "counts,rejected", "locale"),
        ("Tiempo total de ejecución", "seconds,total", "s"),
        ("Parquet final en disco", "sizes,parquet_bytes", "bytes")]):
        stat = panel(title, "stat", (i * 4, 0, 4, 3),
                     "SELECT (body #>> '{report," + key + "}')::double precision AS valor" + base, unit,
                     {"colorMode": "value", "graphMode": "none", "textMode": "value",
                      "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}})
        stat["fieldConfig"]["defaults"].update(decimals=0, color={"mode": "fixed", "fixedColor": "green" if i == 3 else "blue"})

    phases = " UNION ALL ".join("SELECT '" + label + "' AS etapa, (body #>> '{report,seconds," + key + "}')::double precision AS segundos" + base
                                for key, label in (("generation", "1 · Generación"), ("ingestion", "2 · Ingesta"),
                                                   ("spark_startup", "3 · Arranque Spark"), ("processing", "4 · Procesamiento")))
    panel("Tiempo por etapa · corrida completa", "barchart", (0, 3, 12, 6), phases, "s",
          {"orientation": "horizontal", "showValue": "always", "legend": {"displayMode": "hidden"}, "xField": "etapa"})
    risk = "SELECT m->>'risk_level' AS estado, count(*)::int AS equipos FROM benchmark_presentations, jsonb_array_elements(body->'machines') m WHERE run_id=${run:sqlstring} GROUP BY 1 ORDER BY 1"
    pie = panel("Último riesgo · todas las máquinas", "piechart", (12, 3, 12, 6), risk,
                options={"pieType": "donut", "displayLabels": ["name", "value"],
                         "reduceOptions": {"values": True, "fields": "/^equipos$/", "calcs": ["lastNotNull"]},
                         "legend": {"displayMode": "table", "placement": "right", "values": ["value", "percent"]}})
    pie["fieldConfig"]["overrides"] = [{"matcher": {"id": "byName", "options": name},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
        for name, color in (("ROJO", "red"), ("AMARILLO", "yellow"), ("VERDE", "green"))]
    for title, field, x, unit in (("Temperatura · tres máquinas de ejemplo · ventanas de 5 min", "avg_temp", 0, "celsius"),
                                 ("Vibración máxima · tres máquinas de ejemplo · ventanas de 5 min", "max_vibration", 12, "suffix:mm/s")):
        query = "SELECT time, metric, value FROM (SELECT (v->>'window_start')::timestamptz AS time, v->>'machine_id' AS metric, (v->>'" + field + "')::double precision AS value FROM benchmark_presentations, jsonb_array_elements(body->'examples') v WHERE run_id=${run:sqlstring}) series WHERE $__timeFilter(time) ORDER BY time"
        graph = panel(title, "timeseries", (x, 9, 12, 7), query, unit,
                      {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}})
        graph["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 1, "fillOpacity": 8, "showPoints": "never"}
    note = panel("Alcance de la evidencia", "text", (0, 16, 24, 3), "SELECT 1")
    note.pop("targets")
    note["options"] = {"mode": "markdown", "content": "**Datos sintéticos · corrida terminada y verificada.** Los indicadores y el riesgo cubren todas las máquinas; las curvas muestran MAQ-001, MAQ-002 y MAQ-003. Riesgo académico por umbrales, no predicción de fallas. Los datos completos permanecen en Parquet; esta vista utiliza resúmenes guardados."}
    return dashboard
