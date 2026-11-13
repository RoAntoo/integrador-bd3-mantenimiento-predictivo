"""Generar, ingerir y procesar hasta 15 millones de lecturas, sin conectar bases."""

import argparse
from pathlib import Path

from src.benchmark.runner import BenchmarkConfig, run_benchmark


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machines", type=int, required=True)
    parser.add_argument("--readings-per-machine", type=int, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/benchmarks"))
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--rows-per-file", type=int, default=250000)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--start-time", default="2026-10-25T00:00:00Z")
    parser.add_argument("--interval", type=int, default=1, help="Minutos entre mediciones.")
    parser.add_argument("--spark-threads", type=int, default=2)
    parser.add_argument("--spark-memory", default="2g", help="Heap del driver/JVM, por ejemplo 2g o 4096m.")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--max-partition-mb", type=int, default=64)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        config = BenchmarkConfig(**vars(args))
    except ValueError as error:
        parser.error(str(error))
    run, metrics = run_benchmark(config)
    print(f"Estado: {metrics['status']} | Métricas: {run / 'metrics.json'}", flush=True)
    print(f"Conteos: {metrics['counts']}", flush=True)
    print(f"Tiempos (s): {metrics['seconds']}", flush=True)
    for error in metrics["errors"]:
        print(f"Error en {error['stage']}: {error['message']}", flush=True)
    return {"success": 0, "failed": 1, "interrupted": 130}[metrics["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
