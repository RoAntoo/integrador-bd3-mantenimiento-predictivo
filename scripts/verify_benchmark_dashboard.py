"""Verificar las consultas reales del dashboard sin Spark ni datos crudos."""

import argparse
from datetime import datetime
import json
from pathlib import Path

from src.benchmark.presentation import grafana_request, validate_presentation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.summary.read_text(encoding="utf-8"))
    validate_presentation(payload)
    dash = grafana_request("/api/dashboards/uid/bd3-benchmark-dashboard")["dashboard"]
    queries = []
    for panel in dash["panels"]:
        if not panel.get("targets"):
            continue
        query = panel["targets"][0].copy()
        query["datasource"] = panel["datasource"]
        query["refId"] = str(panel["id"])
        query["rawSql"] = query["rawSql"].replace("${run:sqlstring}", "'" + payload["run_id"].replace("'", "''") + "'")
        queries.append(query)
    result = grafana_request("/api/ds/query", {
        "from": str(int(datetime.fromisoformat(payload["from"]).timestamp() * 1000)),
        "to": str(int(datetime.fromisoformat(payload["to"]).timestamp() * 1000)), "queries": queries})["results"]
    for query in queries:
        response = result[query["refId"]]
        if response.get("error") or response.get("status", 200) != 200 or not response.get("frames"):
            raise RuntimeError(f"Panel {query['refId']} sin datos: {response.get('error')}")
        if not any(frame["data"]["values"] and len(frame["data"]["values"][0]) for frame in response["frames"]):
            raise RuntimeError(f"Panel {query['refId']} vacío")
    assert result["1"]["frames"][0]["data"]["values"][0][0] == payload["report"]["counts"]["valid"]
    print(f"Verificados {len(queries)} paneles con datos reales y sin errores; "
          f"{payload['verified_window_readings']:,} lecturas representadas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
