import json
from pathlib import Path

import pytest

from src.ingestion.ingest import ingest_telemetry_batch, validate_and_normalize_record


BASE = {"machine_id": "PR5-A", "timestamp": "2026-10-25T12:00:00Z"}
SHORT = {"temperature": 60, "vibration": 2, "pressure": 5}
UNITS = {"temperature_c": 60.0, "vibration_mm_s": 2.0, "pressure_bar": 5.0}


@pytest.mark.parametrize("measurements", [SHORT, UNITS, SHORT | UNITS])
def test_supported_names(measurements):
    """Obtener las mismas mediciones con nombres cortos, con unidades o ambos."""
    record, _ = validate_and_normalize_record(json.dumps(BASE | measurements))
    assert {key: record[key] for key in SHORT} == SHORT


@pytest.mark.parametrize("alias", list(UNITS))
@pytest.mark.parametrize("value", [99, None, True, "60"])
def test_alias_never_hides_conflicting_or_invalid_value(alias, value):
    """Rechazar alias contradictorios o inválidos aunque el campo corto sea válido."""
    with pytest.raises(ValueError):
        validate_and_normalize_record(json.dumps(BASE | SHORT | UNITS | {alias: value}))


def test_ingestion_rejects_keep_source_and_line_numbers(tmp_path):
    """Conservar el texto y la posición de cada línea en los archivos de auditoría."""
    lines = [json.dumps(BASE | UNITS), "", "{broken", json.dumps(BASE | SHORT | {"temperature_c": 99})]
    source = tmp_path / "input.jsonl"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    result = ingest_telemetry_batch(source, tmp_path / "lake")
    rejected = [json.loads(line) for line in result.rejected_path.read_text(encoding="utf-8").splitlines()]
    assert [row["line_number"] for row in rejected] == [2, 3, 4]
    assert [row["raw_line"] for row in rejected] == lines[1:]
    assert all(row["reason"] for row in rejected)
    assert result.total_received == result.valid_records + result.invalid_records == 4
    normalized = json.loads(result.normalized_path.read_text(encoding="utf-8"))
    assert normalized["source_raw_line"] == lines[0]
    assert normalized["source_line_number"] == 1
    assert normalized["temperature"] == 60
    replay = ingest_telemetry_batch(source, tmp_path / "lake")
    assert replay.rejected_path.read_bytes() == result.rejected_path.read_bytes()


def test_all_rejected_and_strict_audit(tmp_path):
    """Escribir la auditoría de un lote inválido incluso cuando el modo estricto falla."""
    source = tmp_path / "bad.jsonl"
    source.write_text("{broken\n", encoding="utf-8")
    result = ingest_telemetry_batch(source, tmp_path / "lake")
    assert result.invalid_records == result.total_received == 1
    assert result.normalized_path.read_bytes() == b""
    with pytest.raises(ValueError, match="Auditoría"):
        ingest_telemetry_batch(source, tmp_path / "strict", strict=True)
    assert len(list((tmp_path / "strict" / "audit").rglob("*_rejected.jsonl"))) == 1


@pytest.mark.parametrize("field", list(SHORT) + list(UNITS))
@pytest.mark.parametrize("value", [10**400, -(10**400), float("inf")])
def test_unrepresentable_measurement_keeps_valid_lines_and_audit(tmp_path, field, value):
    """Auditar desbordamientos y valores no finitos sin interrumpir las lecturas válidas."""
    valid = json.dumps(BASE | SHORT)
    invalid = json.dumps(BASE | SHORT | {field: value})
    source = tmp_path / "overflow.jsonl"
    source.write_text(valid + "\n" + invalid + "\n" + valid + "\n", encoding="utf-8")
    result = ingest_telemetry_batch(source, tmp_path / "lake")
    assert (result.total_received, result.valid_records, result.invalid_records) == (3, 2, 1)
    rejected = json.loads(result.rejected_path.read_text(encoding="utf-8"))
    assert rejected == {"batch_id": result.sha256, "line_number": 2, "reason": f"NON_FINITE_MEASUREMENT: {field}",
                        "raw_line": invalid}
    normalized = [json.loads(line) for line in result.normalized_path.read_text(encoding="utf-8").splitlines()]
    assert [row["source_line_number"] for row in normalized] == [1, 3]
    assert all(row["source_raw_line"] == valid for row in normalized)


def test_dashboard_time_filter_configuration():
    """Filtrar las tendencias por tiempo y conservar el último estado en el ranking."""
    dashboard = json.loads((Path(__file__).parents[1] / "dashboards" / "machines.json").read_text(encoding="utf-8"))
    ranking, *trends = dashboard["panels"]
    ranking_sql = ranking["targets"][0]["rawSql"]
    assert "$__timeFilter" not in ranking_sql
    assert "r.calculated_at" in ranking_sql and "r.window_start" in ranking_sql
    assert all("$__timeFilter(window_start)" in panel["targets"][0]["rawSql"] for panel in trends)
