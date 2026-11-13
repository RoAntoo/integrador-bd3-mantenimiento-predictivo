"""Publicar resúmenes pequeños de un benchmark terminado para Grafana."""

import argparse
import json
from pathlib import Path
from uuid import uuid4
import webbrowser

from src.benchmark.presentation import build_dashboard, dashboard_url, ensure_dashboard, prepare_presentation, publish, validate_presentation


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-dir", type=Path)
    source.add_argument("--summary", type=Path, help="JSON portátil previamente preparado.")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--open", action="store_true", help="Abrir Grafana con la corrida y su rango temporal exacto.")
    args = parser.parse_args(argv)
    if args.open and args.prepare_only:
        parser.error("--open requiere publicar; no se combina con --prepare-only.")
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
        url = dashboard_url(payload)
        print(f"Publicado. Grafana: {url}", flush=True)
        if args.open and not webbrowser.open(url):
            print("No se pudo abrir el navegador; abrí el enlace anterior manualmente.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
