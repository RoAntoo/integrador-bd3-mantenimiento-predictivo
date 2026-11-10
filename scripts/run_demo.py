"""Generar o leer un lote, procesarlo con Spark y cargar sus resultados opcionalmente."""

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from src.generator.generate import DEFAULT_START_TIME, GenerationConfig, generate_telemetry
from src.ingestion.ingest import ingest_telemetry_batch
from src.processing.analysis import analyze_telemetry
from src.processing.export_results import ALERTS_FILE, METRICS_FILE, RISK_FILE, export_results
from src.processing.risk_rules import RiskThresholds, execution_time, load_thresholds
from src.processing.spark import create_spark_session


MANIFEST = "batch.json"


def prepare_batch(work_dir: Path, input_path: Path | None, start_time: str, seed: int,
                  thresholds: RiskThresholds) -> Path:
    """Preparar los artefactos del lote o reutilizar una exportación idéntica."""
    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=work_dir) as temp:
        stage = Path(temp) / "batch"
        stage.mkdir()
        source = stage / "telemetry.jsonl"
        if input_path is None:
            generate_telemetry(GenerationConfig(machines=3, days=1 / 24, seed=seed, output=source,
                                               start_time=start_time))
        else:
            shutil.copyfile(input_path, source)

        with source.open("rb") as stream:
            source_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
        # La identidad vincula la entrada y las reglas, no el reloj de ejecución.
        identity = {"source_sha256": source_sha256, "thresholds": asdict(thresholds)}
        batch_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
        batch_dir = work_dir / batch_id
        if (batch_dir / MANIFEST).is_file():
            return batch_dir

        calculated_at = datetime.now(timezone.utc)
        ingestion = ingest_telemetry_batch(source, stage / "lake")
        with TemporaryDirectory(dir=stage) as raw_dir:
            raw_input = Path(raw_dir) / "raw.jsonl"
            with raw_input.open("wb") as target:
                for raw_file in ingestion.raw_files:
                    with raw_file.open("rb") as raw:
                        shutil.copyfileobj(raw, target)

            spark = create_spark_session()
            try:
                result = analyze_telemetry(spark, raw_input, calculated_at, thresholds)
                export_results(result, thresholds, stage / "results")
            finally:
                spark.stop()

        manifest = {
            **identity,
            "batch_id": batch_id,
            "calculated_at": calculated_at.isoformat(),
            "ingestion": {
                "received": ingestion.total_received,
                "valid": ingestion.valid_records,
                "rejected": ingestion.invalid_records,
                "min_timestamp": ingestion.min_timestamp,
                "max_timestamp": ingestion.max_timestamp,
            },
            "quality": asdict(result.quality.metrics),
        }
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        try:
            stage.rename(batch_dir)
        except OSError:
            # Otra corrida del mismo lote pudo terminar la publicación primero.
            if not (batch_dir / MANIFEST).is_file():
                raise
        return batch_dir


def load_batch(batch_dir: Path, batch_id: str) -> None:
    """Cargar un lote exportado en PostgreSQL antes de publicar sus alertas."""
    from psycopg2 import Error as PostgreSQLError

    from src.storage.load_results import upsert_mongodb, upsert_postgres

    results = batch_dir / "results"
    alerts = results / ALERTS_FILE
    if not alerts.is_file():
        raise FileNotFoundError(f"Falta el archivo de alertas del lote: {alerts}")
    try:
        upsert_postgres(results / METRICS_FILE, results / RISK_FILE, batch_id=batch_id)
    except PostgreSQLError as error:
        raise RuntimeError(
            f'Falló la carga en PostgreSQL. Corregí el error y reintentá con --load-only "{batch_dir}".'
        ) from error
    try:
        upsert_mongodb(alerts)
    except Exception as error:
        raise RuntimeError(
            f'PostgreSQL confirmó el lote; MongoDB quedó pendiente. Reintentá con --load-only "{batch_dir}".'
        ) from error


def main() -> int:
    """Interpretar las opciones de la CLI y mostrar el resultado de la corrida."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="JSONL de Leo. Por defecto genera una hora para tres máquinas.")
    parser.add_argument("--start-time", default=DEFAULT_START_TIME, help="Inicio ISO 8601 con zona explícita.")
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument("--thresholds", type=Path, help="JSON de umbrales. Por defecto usa las reglas del PR #3.")
    parser.add_argument("--work-dir", type=Path, default=Path("artifacts/pipeline"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--load", action="store_true", help="Cargar el lote exportado en PostgreSQL y MongoDB.")
    mode.add_argument("--load-only", type=Path, help="Cargar o reintentar a partir de un directorio de lote existente.")
    args = parser.parse_args()

    try:
        if args.load_only:
            batch_dir = args.load_only.resolve()
        else:
            start_time = execution_time(datetime.fromisoformat(args.start_time)).isoformat()
            thresholds = load_thresholds(args.thresholds) if args.thresholds else RiskThresholds()
            batch_dir = prepare_batch(args.work_dir, args.input, start_time, args.seed, thresholds)
        manifest = json.loads((batch_dir / MANIFEST).read_text(encoding="utf-8"))
        print(f"Lote exportado en {batch_dir}", flush=True)
        print(json.dumps(manifest, indent=2), flush=True)
        if args.load or args.load_only:
            load_batch(batch_dir, manifest["batch_id"])
    except (ValueError, OSError, RuntimeError) as error:
        parser.exit(1, f"Error: {error}\n" + (f"Detalle: {error.__cause__}\n" if error.__cause__ else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
