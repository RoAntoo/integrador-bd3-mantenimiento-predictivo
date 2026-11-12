"""Propiedades del contaminador sin iniciar Spark ni servicios externos."""

from dataclasses import asdict
import json
from pathlib import Path

import pytest

from src.generator.contaminate import DIRTY_TYPES, contaminate_jsonl
from src.generator.generate import GenerationConfig, generate_telemetry
from src.ingestion.ingest import ingest_telemetry_batch, validate_and_normalize_record
from src.jsonl import read_jsonl_lines


def source_file(tmp_path, count=18):
    source = tmp_path / "original.jsonl"
    generate_telemetry(GenerationConfig(machines=1, days=count / 1440, interval=1,
                                        seed=42, anomaly_rate=1, output=source))
    return source


def test_zero_is_byte_identical_even_for_dirty_input(tmp_path):
    source = tmp_path / "source.jsonl"
    source.write_bytes(b'not json\r\n\r\n{"missing":null}')
    original = source.read_bytes()
    output = tmp_path / "out.jsonl"
    report = contaminate_jsonl(source, output, 0, 99)
    assert output.read_bytes() == source.read_bytes() == original
    assert report.total_lines == 3
    assert report.selected_lines == report.modified_lines == 0


def test_reproducible_budget_and_all_scenarios(tmp_path):
    source = source_file(tmp_path)
    original_bytes = source.read_bytes()
    a, b, c = (tmp_path / name for name in ("a.jsonl", "b.jsonl", "c.jsonl"))
    first = contaminate_jsonl(source, a, 1, 42)
    second = contaminate_jsonl(source, b, 1, 42)
    changed = contaminate_jsonl(source, c, 1, 43)
    assert asdict(first) == asdict(second)
    assert a.read_bytes() == b.read_bytes() != c.read_bytes()
    assert changed.output_sha256 != first.output_sha256
    assert source.read_bytes() == original_bytes
    assert len(read_jsonl_lines(a)) == first.total_lines == 18
    assert first.selected_lines == 18 and first.modified_lines == 14
    assert not first.uncovered_types
    assert set(first.breakdown) == set(DIRTY_TYPES)
    assert set(first.breakdown.values()) == {2}
    assert sorted(n for scenario in first.scenarios for n in scenario["line_numbers"]) == list(range(1, 19))

    originals, polluted = read_jsonl_lines(source), read_jsonl_lines(a)
    for scenario in first.scenarios:
        indices = [n - 1 for n in scenario["line_numbers"]]
        kind = scenario["type"]
        if kind == "identical_duplicate":
            assert polluted[indices[0]] == polluted[indices[1]] == originals[indices[0]]
        elif kind == "conflicting_duplicate":
            anchor, conflict = [json.loads(polluted[i]) for i in indices]
            assert (anchor["machine_id"], anchor["timestamp"]) == (conflict["machine_id"], conflict["timestamp"])
            assert any(anchor[field] != conflict[field] for field in ("temperature", "vibration", "pressure"))
            for field, alias in (("temperature", "temperature_c"), ("vibration", "vibration_mm_s"), ("pressure", "pressure_bar")):
                assert conflict[field] == conflict[alias]
        else:
            with pytest.raises(ValueError):
                validate_and_normalize_record(polluted[indices[0]])


def test_partial_budget_is_reported_and_small_rate_rounds_down(tmp_path):
    source = source_file(tmp_path, 10)
    report = contaminate_jsonl(source, tmp_path / "small.jsonl", .2, 42)
    assert report.selected_lines == report.modified_lines == 2
    assert report.effective_rate == .2
    assert len(report.uncovered_types) == 5
    zero = contaminate_jsonl(source, tmp_path / "round.jsonl", .01, 42)
    assert zero.selected_lines == 0
    assert zero.requested_rate == .01 and zero.effective_rate == 0


@pytest.mark.parametrize("rate", [-1, 1.1, float("nan"), float("inf"), True])
def test_invalid_rate(tmp_path, rate):
    with pytest.raises(ValueError, match="dirty_rate"):
        contaminate_jsonl(tmp_path / "unused", tmp_path / "out", rate)


def test_source_protected_and_positive_mode_requires_valid_unique_keys(tmp_path):
    source = source_file(tmp_path, 10)
    with pytest.raises(ValueError, match="distinta"):
        contaminate_jsonl(source, source, 0)
    first = read_jsonl_lines(source)[0]
    source.write_text(first + "\n" + first + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="únicas"):
        contaminate_jsonl(source, tmp_path / "dup.jsonl", 1)
    source.write_text("bad json\n", encoding="utf-8")
    with pytest.raises(ValueError, match="entrada válida"):
        contaminate_jsonl(source, tmp_path / "bad.jsonl", 1)


@pytest.mark.parametrize("field,alias", [("temperature", "temperature_c"), ("vibration", "vibration_mm_s"), ("pressure", "pressure_bar"), ("machine_id", None), ("timestamp", None)])
@pytest.mark.parametrize("problem", ["missing", "null", "type"])
def test_ingestion_required_field_rejections_are_audited(tmp_path, field, alias, problem):
    record = dict(machine_id="A", timestamp="2026-10-25T00:00:00Z", temperature=60, vibration=2, pressure=5)
    if alias:
        record[alias] = record[field]
    for name in (field, alias) if alias else (field,):
        if problem == "missing":
            record.pop(name)
        else:
            record[name] = None if problem == "null" else {"bad": True}
    raw = " \t" + json.dumps(record) + "  "
    source = tmp_path / "bad.jsonl"
    source.write_text(raw + "\n", encoding="utf-8")
    result = ingest_telemetry_batch(source, tmp_path / "lake", batch_id="test")
    assert (result.total_received, result.invalid_records, result.valid_records) == (1, 1, 0)
    row = json.loads(result.rejected_audit_path.read_text(encoding="utf-8"))
    prefix = {"missing": "MISSING", "null": "NULL", "type": "INVALID_TYPE"}[problem]
    expected = "INVALID_TIMESTAMP" if field == "timestamp" and problem == "type" else f"{prefix}_{field.upper()}"
    assert row["rejection_reasons"] == [expected]
    assert row["raw_line"] == raw
    assert (row["batch_id"], row["line_number"], row["stage"]) == ("test", 1, "ingestion")
    assert result.accepted_audit_path.read_bytes() == b""


def test_physical_lines_preserve_unicode_and_blank_records(tmp_path):
    source = tmp_path / "unicode.jsonl"
    source.write_text('{"note":"a\u2028b\u0085c"}\n\n', encoding="utf-8")
    assert len(read_jsonl_lines(source)) == 2
    result = ingest_telemetry_batch(source, tmp_path / "lake")
    assert (result.total_received, result.invalid_records) == (2, 2)
    rows = [json.loads(line) for line in result.rejected_audit_path.read_text(encoding="utf-8").split("\n") if line]
    assert [row["line_number"] for row in rows] == [1, 2]
    assert rows[0]["raw_line"] == '{"note":"a\u2028b\u0085c"}'
