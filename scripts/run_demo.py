"""Generar o leer un lote, procesarlo con Spark y cargar sus resultados opcionalmente."""

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import time

from src.generator.generate import DEFAULT_START_TIME, GenerationConfig, generate_telemetry
from src.generator.contaminate import CONTAMINATION_VERSION, contaminate_jsonl
from src.ingestion.ingest import ingest_telemetry_batch
from src.jsonl import file_sha256
from src.processing.analysis import analyze_telemetry
from src.processing.export_results import ALERTS_FILE, METRICS_FILE, RISK_FILE, export_results
from src.processing.risk_rules import RiskThresholds, execution_time, load_thresholds
from src.processing.spark import create_spark_session


MANIFEST = "batch.json"
PIPELINE_VERSION = 2


def reconcile_counts(ingestion, quality) -> dict:
    """Verificar cada frontera; duplicados ya están incluidos en quality.rejected."""
    checks = (
        ingestion.total_received == ingestion.invalid_records + ingestion.valid_records,
        ingestion.valid_records == quality.received,
        quality.received == quality.rejected + quality.valid,
        ingestion.total_received == ingestion.invalid_records + quality.rejected + quality.valid,
    )
    if not all(checks):
        raise RuntimeError("Los conteos de ingesta y Spark no reconcilian; no se publica el lote.")
    return {"received": ingestion.total_received, "ingestion_rejected": ingestion.invalid_records,
            "spark_rejected": quality.rejected, "final_valid": quality.valid,
            "rejected_total": ingestion.invalid_records + quality.rejected, "balanced": True}


def write_curated_history(clean_file: Path, lake: Path, batch_id: str) -> None:
    """Histórico integrado particionado: solo filas curadas, nunca conflictos de raw."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    # Cerrar explícitamente el descriptor antes de renombrar el lote en Windows.
    with pq.ParquetFile(clean_file) as source:
        table = source.read()
    dates = pc.cast(table["timestamp"], "date32")
    for date in sorted(dates.unique().to_pylist()):
        target = lake / "historical" / "telemetry" / f"date={date}" / f"part-{batch_id}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table.filter(pc.equal(dates, date)), target, compression="snappy")


def prepare_batch(work_dir: Path, input_path: Path | None, start_time: str, seed: int,
                  thresholds: RiskThresholds, *, dirty_rate: float = 0.0,
                  dirty_seed: int = 42) -> Path:
    """Preparar los artefactos del lote o reutilizar una exportación idéntica."""
    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=work_dir) as temp:
        stage = Path(temp) / "batch"
        stage.mkdir()
        original = stage / "telemetry_original.jsonl"
        source = stage / "telemetry.jsonl"
        if input_path is None:
            generate_telemetry(GenerationConfig(machines=3, days=1 / 24, seed=seed, output=original,
                                               start_time=start_time))
        else:
            shutil.copyfile(input_path, original)

        contamination = contaminate_jsonl(original, source, dirty_rate, dirty_seed)
        (stage / "contamination.json").write_text(
            json.dumps(asdict(contamination), indent=2, ensure_ascii=False) + "\n", encoding="utf-8",
        )

        source_sha256 = file_sha256(source)
        # La identidad vincula la entrada y las reglas, no el reloj de ejecución.
        identity = {"source_sha256": source_sha256, "thresholds": asdict(thresholds),
                    "pipeline_version": PIPELINE_VERSION, "original_sha256": contamination.input_sha256,
                    "contamination_config": {"version": CONTAMINATION_VERSION,
                                             "dirty_rate": dirty_rate, "dirty_seed": dirty_seed}}
        batch_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
        batch_dir = work_dir / batch_id
        if (batch_dir / MANIFEST).is_file():
            return batch_dir

        calculated_at = datetime.now(timezone.utc)
        ingestion = ingest_telemetry_batch(source, stage / "lake", batch_id=batch_id,
                                          write_historical=False)
        # Spark puede conservar descriptores locales hasta completar su cierre.
        # Leer una copia fuera de stage permite publicar la carpeta en Windows.
        spark_input = Path(temp) / "accepted.jsonl"
        shutil.copyfile(ingestion.accepted_audit_path, spark_input)
        spark = create_spark_session()
        try:
            result = analyze_telemetry(spark, spark_input, calculated_at,
                                       thresholds, audited=True)
            reconciliation = reconcile_counts(ingestion, result.quality.metrics)
            export_results(result, thresholds, stage / "results")
        finally:
            spark.stop()
        shutil.copyfile(ingestion.rejected_audit_path, stage / "results" / "ingestion_rejected.jsonl")
        write_curated_history(stage / "results" / "clean_telemetry.parquet", stage / "lake", batch_id)

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
            "contamination": asdict(contamination),
            "reconciliation": reconciliation,
        }
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        for attempt in range(10):
            try:
                stage.rename(batch_dir)
                break
            except OSError as error:
                # Otra corrida del mismo lote pudo publicar primero.
                if (batch_dir / MANIFEST).is_file():
                    break
                if not isinstance(error, PermissionError) or attempt == 9:
                    raise
                time.sleep(0.05)
        return batch_dir


def load_batch(batch_dir: Path, batch_id: str) -> None:
    """Cargar un lote exportado en PostgreSQL antes de publicar sus alertas."""
    from psycopg2 import Error as PostgreSQLError
    import pyarrow.parquet as pq

    from src.storage.load_results import upsert_mongodb, upsert_postgres

    results = batch_dir / "results"
    alerts = results / ALERTS_FILE
    if not alerts.is_file():
        raise FileNotFoundError(f"Falta el archivo de alertas del lote: {alerts}")
    if (pq.read_metadata(results / METRICS_FILE).num_rows == 0
            and pq.read_metadata(results / RISK_FILE).num_rows == 0):
        if alerts.stat().st_size:
            raise ValueError("Un lote sin métricas ni riesgos no puede cargar alertas.")
        print("Lote sin válidos: auditoría conservada; no hay métricas, riesgos ni alertas para cargar.")
        return
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
    parser.add_argument("--dirty-rate", default=0.0, type=float, help="Proporción de líneas seleccionadas (0..1); desactivado por defecto.")
    parser.add_argument("--dirty-seed", default=42, type=int, help="Semilla independiente para contaminación reproducible.")
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
            batch_dir = prepare_batch(args.work_dir, args.input, start_time, args.seed, thresholds,
                                       dirty_rate=args.dirty_rate, dirty_seed=args.dirty_seed)
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
