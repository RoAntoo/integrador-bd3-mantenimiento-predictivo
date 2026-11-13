"""Validar línea a línea y escribir row groups Parquet con esquema explícito."""

from datetime import datetime
import json
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.ingest import validate_and_normalize_record


INGESTION_SCHEMA = pa.schema([
    ("machine_id", pa.string()), ("timestamp", pa.timestamp("us", tz="UTC")),
    *[(name, pa.float64()) for name in (
        "temperature", "vibration", "pressure", "temperature_c", "vibration_mm_s",
        "pressure_bar", "power_kw", "operating_hours")],
    ("rpm", pa.int64()), ("machine_state", pa.string()),
    ("is_anomaly", pa.bool_()), ("anomaly_type", pa.string()), ("date", pa.string()),
    ("line_number", pa.int64()), ("raw_line", pa.string()),
])


def ingest_jsonl(source: Path, output: Path, rejected_path: Path, *,
                 batch_size: int = 10000, rows_per_file: int = 250000,
                 batch_id: str, progress: Callable[[dict], None] = lambda counts: None) -> dict:
    """No deduplicar aquí: Spark conserva la semántica de conflictos del pipeline."""
    if not 1 <= batch_size <= 100000 or rows_per_file < batch_size:
        raise ValueError("Lote entre 1 y 100000; rows_per_file debe ser >= batch_size.")
    output.mkdir()
    counts = {"received": 0, "ingestion_valid": 0, "ingestion_rejected": 0,
              "max_buffered_records": 0, "ingestion_parquet_files": 0}
    buffer = []
    writer = None
    file_rows = 0

    def flush():
        nonlocal writer, file_rows
        if not buffer:
            return
        if writer is None:
            part = output / f"part-{counts['ingestion_parquet_files']:06d}.parquet"
            writer = pq.ParquetWriter(part, INGESTION_SCHEMA, compression="snappy")
            counts["ingestion_parquet_files"] += 1
        writer.write_table(pa.Table.from_pylist(buffer, schema=INGESTION_SCHEMA),
                           row_group_size=batch_size)
        file_rows += len(buffer)
        buffer.clear()
        if file_rows == rows_per_file:
            writer.close()
            writer = None
            file_rows = 0
        progress(counts)

    try:
        with source.open(encoding="utf-8", newline="") as stream, rejected_path.open(
                "x", encoding="utf-8", newline="\n") as rejected:
            for number, line in enumerate(stream, 1):
                raw = line.removesuffix("\n").removesuffix("\r")
                counts["received"] += 1
                try:
                    row, _ = validate_and_normalize_record(raw)
                except (ValueError, OverflowError) as error:
                    counts["ingestion_rejected"] += 1
                    rejected.write(json.dumps({"batch_id": batch_id, "line_number": number,
                                               "raw_line": raw, "reason": str(error)},
                                              ensure_ascii=False) + "\n")
                    continue
                row["timestamp"] = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                buffer.append({**row, "line_number": number, "raw_line": raw})
                counts["ingestion_valid"] += 1
                counts["max_buffered_records"] = max(counts["max_buffered_records"], len(buffer))
                if len(buffer) >= min(batch_size, rows_per_file - file_rows):
                    flush()
            flush()
        if counts["ingestion_parquet_files"] == 0:
            pq.write_table(pa.Table.from_pylist([], schema=INGESTION_SCHEMA),
                           output / "part-000000.parquet", compression="snappy")
            counts["ingestion_parquet_files"] = 1
    finally:
        if writer is not None:
            writer.close()
        progress(counts)
    return counts
