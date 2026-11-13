"""Escritura local desde workers Spark en lotes Arrow, sin winutils ni colectar filas."""

from pathlib import Path
import shutil

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import to_arrow_schema


def write_local_dataset(frame, output: Path, *, rows_per_file: int, jsonl: bool = False) -> None:
    """Publicar solo archivos de tareas confirmadas; aislar intentos fallidos.

    Requiere Spark local: los workers comparten el disco del driver. El driver
    recibe únicamente nombres y conteos de archivos. La memoria de cada worker
    se acota con spark.sql.execution.arrow.maxRecordsPerBatch.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = output.resolve()
    output.mkdir()
    attempts = output / "_attempts"
    attempts.mkdir()
    if jsonl:
        frame = frame.select(F.to_json(F.struct(*frame.columns)).alias("line"))
    arrow_schema = to_arrow_schema(frame.schema)
    attempt_path = str(attempts)
    metadata_schema = pa.schema([("file", pa.string()), ("rows", pa.int64())])

    def write_batches(batches):
        from pyspark import TaskContext
        from pathlib import Path
        import pyarrow as pa
        import pyarrow.parquet as pq

        task = TaskContext.get()
        prefix = f"part-{task.partitionId():06d}-{task.taskAttemptId():012d}"
        writer = None
        written, part = 0, 0
        filename = None
        try:
            for batch in batches:
                offset = 0
                while offset < batch.num_rows:
                    if writer is None:
                        filename = f"{prefix}-{part:06d}.{'jsonl' if jsonl else 'parquet'}"
                        path = Path(attempt_path) / filename
                        writer = (path.open("x", encoding="utf-8", newline="\n") if jsonl else
                                  pq.ParquetWriter(path, arrow_schema, compression="snappy"))
                    length = min(batch.num_rows - offset, rows_per_file - written)
                    chunk = batch.slice(offset, length)
                    if jsonl:
                        for line in chunk.column(0):
                            writer.write(line.as_py() + "\n")
                    else:
                        table = pa.Table.from_batches([chunk]).cast(arrow_schema)
                        writer.write_table(table, row_group_size=length)
                    written += length
                    offset += length
                    if written == rows_per_file:
                        writer.close()
                        writer = None
                        yield pa.RecordBatch.from_pylist([{"file": filename, "rows": written}],
                                                         schema=metadata_schema)
                        part += 1
                        written = 0
            if writer is not None:
                writer.close()
                writer = None
                yield pa.RecordBatch.from_pylist([{"file": filename, "rows": written}], schema=metadata_schema)
        finally:
            if writer is not None:
                writer.close()

    metadata = frame.mapInArrow(write_batches, "file string, rows long").persist(StorageLevel.MEMORY_AND_DISK)
    committed = 0
    try:
        # Ejecutar todas las particiones en paralelo; el cache solo contiene metadatos.
        expected_files = metadata.count()
        for entry in metadata.toLocalIterator(prefetchPartitions=False):
            candidate = attempts / entry.file
            target = output / entry.file
            if candidate.resolve().parent != attempts or target.exists():
                raise ValueError("Archivo de tarea fuera del directorio o ya publicado.")
            if not jsonl and pq.ParquetFile(candidate).metadata.num_rows != entry.rows:
                raise ValueError(f"Conteo Parquet de tarea incorrecto: {candidate}")
            candidate.rename(target)
            committed += 1
        if committed != expected_files:
            raise ValueError("No se publicaron todos los archivos de tareas confirmadas.")
        if committed == 0 and not jsonl:
            pq.write_table(pa.Table.from_batches([], schema=arrow_schema), output / "part-empty.parquet")
        if attempts.resolve().parent != output:
            raise ValueError("Directorio de intentos fuera de la salida.")
        shutil.rmtree(attempts)
        (output / "_SUCCESS").touch(exist_ok=False)
    finally:
        metadata.unpersist()
