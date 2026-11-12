"""Regresiones de protección del original y aislamiento de auditorías por lote."""

import hashlib
import json

import pytest

from src.generator.contaminate import contaminate_jsonl
from src.ingestion.ingest import ingest_telemetry_batch


@pytest.mark.parametrize("rate", [0.0, 1.0])
def test_hard_link_output_cannot_overwrite_original(tmp_path, rate):
    source = tmp_path / "source.jsonl"
    source.write_bytes(b'{"machine_id":"A"}\r\n')
    output = tmp_path / "hard-link.jsonl"
    output.hardlink_to(source)
    before = source.read_bytes()
    with pytest.raises(ValueError, match="distinta del original"):
        contaminate_jsonl(source, output, dirty_rate=rate)
    assert source.read_bytes() == output.read_bytes() == before
    assert source.samefile(output)


def test_audits_are_preserved_per_batch_and_replayed_idempotently(tmp_path):
    source = tmp_path / "source.jsonl"
    good = dict(machine_id="A", timestamp="2026-10-25T00:00:00Z",
                temperature=60, vibration=2, pressure=5)
    source.write_text(json.dumps(good) + "\n{broken\n", encoding="utf-8")
    lake = tmp_path / "lake"
    first = ingest_telemetry_batch(source, lake, batch_id="batch/A")
    original = (first.normalized_path.read_bytes(), first.rejected_path.read_bytes())
    second = ingest_telemetry_batch(source, lake, batch_id="batch/B")
    assert first.normalized_path != second.normalized_path
    assert first.rejected_path != second.rejected_path
    assert (first.normalized_path.read_bytes(), first.rejected_path.read_bytes()) == original
    for result, batch_id in ((first, "batch/A"), (second, "batch/B")):
        audit_key = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()
        assert result.normalized_path.parent == lake.resolve() / "audit" / audit_key
        assert result.rejected_path.parent == result.normalized_path.parent
        assert json.loads(result.rejected_path.read_text(encoding="utf-8"))["batch_id"] == batch_id
    assert first.normalized_path.name == second.normalized_path.name
    assert first.rejected_path.name == second.rejected_path.name
    assert first.raw_files == second.raw_files
    assert first.parquet_files == second.parquet_files
    replay = ingest_telemetry_batch(source, lake, batch_id="batch/A")
    assert replay.idempotent_replay
    assert replay.normalized_path == first.normalized_path
    assert replay.rejected_path == first.rejected_path
    assert (replay.normalized_path.read_bytes(), replay.rejected_path.read_bytes()) == original
    assert json.loads(second.rejected_path.read_text(encoding="utf-8"))["batch_id"] == "batch/B"


def test_default_audit_identity_uses_input_hash(tmp_path):
    source = tmp_path / "bad.jsonl"
    source.write_bytes(b'{broken\n')
    result = ingest_telemetry_batch(source, tmp_path / "lake")
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    assert json.loads(result.rejected_path.read_text(encoding="utf-8"))["batch_id"] == source_hash
    replay = ingest_telemetry_batch(source, tmp_path / "lake")
    assert replay.rejected_path == result.rejected_path
    assert replay.normalized_path == result.normalized_path
