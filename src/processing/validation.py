"""Reglas de lectura y campos obligatorios compartidos por ambos flujos."""

from functools import reduce
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql import functions

from src.processing.schema import TELEMETRY_SCHEMA


JSON_OPTIONS = {
    "allowSingleQuotes": "false",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX",
    "enableDateTimeParsingFallback": "false",
}
REQUIRED_FIELDS = tuple(TELEMETRY_SCHEMA.fieldNames())


def resolve_input_path(input_path: str | Path) -> Path:
    """Resolver un archivo local existente para el lector de Spark."""
    dataset_path = Path(input_path).resolve()
    if not dataset_path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {dataset_path}")
    return dataset_path


def missing_required_value() -> Column:
    """Detectar cualquier campo obligatorio ausente o nulo tras el parseo."""
    return reduce(
        lambda previous, current: previous | current,
        required_field_null_checks().values(),
    )


def required_field_null_checks() -> dict[str, Column]:
    """Dar una condición por campo para explicar los rechazos por nulos."""
    return {field: functions.col(field).isNull() for field in REQUIRED_FIELDS}
