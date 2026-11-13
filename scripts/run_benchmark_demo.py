"""Generar, verificar, publicar y abrir un benchmark con las fechas correctas."""

from scripts.run_benchmark import build_argument_parser
from scripts.publish_benchmark import main as publish_main
from src.benchmark.runner import BenchmarkConfig, run_benchmark


def check_services():
    from src.storage.load_results import get_pg_connection
    from src.benchmark.presentation import grafana_request

    connection = get_pg_connection()
    connection.close()
    grafana_request("/api/health")


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    parser.description = __doc__
    parser.add_argument("--open", action="store_true", help="Abrir el dashboard al terminar.")
    args = vars(parser.parse_args(argv))
    open_browser = args.pop("open")
    try:
        config = BenchmarkConfig(**args)
    except ValueError as error:
        parser.error(str(error))
    try:
        check_services()
    except Exception as error:
        print(f"No se inició la generación: PostgreSQL y Grafana deben estar disponibles. {error}", flush=True)
        print("Iniciá Docker Desktop y ejecutá: docker compose --env-file .env -f infra/docker-compose.yml up -d --wait postgres grafana", flush=True)
        return 1
    run, metrics = run_benchmark(config)
    print(f"Benchmark guardado en {run} | Estado: {metrics['status']}", flush=True)
    if metrics["status"] != "success":
        return 130 if metrics["status"] == "interrupted" else 1
    options = ["--run-dir", str(run)]
    if open_browser:
        options.append("--open")
    try:
        return publish_main(options)
    except Exception as error:
        print(f"El benchmark terminó, pero falló su publicación: {error}", flush=True)
        print(f'Reintentá sin regenerar: python -m scripts.publish_benchmark --run-dir "{run}" --open', flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
