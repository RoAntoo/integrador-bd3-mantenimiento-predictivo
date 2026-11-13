"""Publicar resúmenes pequeños de un benchmark terminado para Grafana."""

import argparse
import json
from pathlib import Path
from uuid import uuid4

from src.benchmark.presentation import build_dashboard, ensure_dashboard, prepare_presentation, publish, validate_presentation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-dir", type=Path)
    source.add_argument("--summary", type=Path, help="JSON portátil previamente preparado.")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    payload = (prepare_presentation(args.run_dir.resolve()) if args.run_dir else
               json.loads(args.summary.read_text(encoding="utf-8")))
    validate_presentation(payload)
    if args.summary is None:
        directory = Path("artifacts/benchmark_presentations") / (payload["run_id"] + "-" + uuid4().hex[:8])
        directory.mkdir(parents=True)
        summary = directory / "presentation.json"
        summary.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Resumen portátil: {summary.resolve()}", flush=True)
    if not args.prepare_only:
        publish(payload)
        dashboard = Path("dashboards/benchmark.json")
        definition = build_dashboard(payload)
        dashboard.write_text(json.dumps(definition, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        ensure_dashboard(definition)
        print("Publicado. Grafana: /d/bd3-benchmark-dashboard", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
