"""Contaminación opcional y determinista de JSONL, independiente de la simulación."""

import argparse
from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import random
import shutil

from src.jsonl import file_sha256, read_jsonl_lines


CONTAMINATION_VERSION = 1
DIRTY_TYPES = (
    "missing_required", "null_value", "incompatible_type", "invalid_timestamp",
    "malformed_json", "identical_duplicate", "conflicting_duplicate",
)
ALIASES = {"temperature": "temperature_c", "vibration": "vibration_mm_s", "pressure": "pressure_bar"}


@dataclass(frozen=True)
class ContaminationSummary:
    version: int
    seed: int
    requested_rate: float
    effective_rate: float
    total_lines: int
    selected_lines: int
    modified_lines: int
    input_sha256: str
    output_sha256: str
    breakdown: dict[str, int]
    uncovered_types: list[str]
    scenarios: list[dict]


def contaminate_jsonl(input_path: str | Path, output_path: str | Path,
                      dirty_rate: float = 0.0, dirty_seed: int = 42) -> ContaminationSummary:
    """Seleccionar floor(N*tasa) participantes sin agregar ni eliminar líneas.

    Un par duplicado consume dos posiciones seleccionadas: su ancla y su copia.
    El ancla permanece intacta, por lo que seleccionadas y modificadas difieren.
    Con presupuesto >= 9 se cubren los siete tipos. Entrada positiva: objetos de
    telemetría válidos con claves únicas. Tasa cero copia cualquier entrada sin validarla.
    """
    if isinstance(dirty_rate, bool) or not isinstance(dirty_rate, (int, float)) or not math.isfinite(dirty_rate) or not 0 <= dirty_rate <= 1:
        raise ValueError("dirty_rate debe ser un número entre 0 y 1.")
    if isinstance(dirty_seed, bool) or not isinstance(dirty_seed, int):
        raise ValueError("dirty_seed debe ser un entero.")
    source, output = Path(input_path).resolve(), Path(output_path).resolve()
    if source == output:
        raise ValueError("La salida contaminada debe ser distinta del original.")
    lines = read_jsonl_lines(source)
    total = len(lines)
    budget = math.floor(total * dirty_rate)
    source_hash = file_sha256(source)
    contaminated = lines.copy()
    scenarios = []
    breakdown = {kind: 0 for kind in DIRTY_TYPES}
    rng = random.Random(dirty_seed)
    if budget:
        # Importación local: el simulador no depende de este módulo ni de Spark.
        from src.ingestion.ingest import validate_and_normalize_record

        records, keys = [], set()
        for number, line in enumerate(lines, 1):
            try:
                normalized, _ = validate_and_normalize_record(line)
            except (ValueError, OverflowError) as error:
                raise ValueError(f"La contaminación requiere entrada válida; línea {number}: {error}") from error
            key = (normalized["machine_id"], normalized["timestamp"])
            if key in keys:
                raise ValueError("La contaminación requiere claves de entrada únicas.")
            keys.add(key)
            records.append(json.loads(line))

        positions = rng.sample(range(total), budget)
        cursor = 0
        cycle = 0
        while cursor < budget:
            kind = DIRTY_TYPES[cycle % len(DIRTY_TYPES)]
            cycle += 1
            pair = kind in ("identical_duplicate", "conflicting_duplicate")
            if pair and budget - cursor < 2:
                kind, pair = "missing_required", False
            count = 2 if pair else 1
            selected = positions[cursor:cursor + count]
            cursor += count
            index = selected[0]
            record = records[index].copy()
            field = rng.choice(tuple(ALIASES))
            if pair:
                target = selected[1]
                if kind == "identical_duplicate":
                    contaminated[target] = lines[index]
                else:
                    # Cambiar una medición segura, finita, y mantener ambos aliases.
                    changed = 123.456 if record.get(field, record.get(ALIASES[field])) != 123.456 else 124.456
                    for name in (field, ALIASES[field]):
                        record[name] = changed
                    contaminated[target] = json.dumps(record, ensure_ascii=False, allow_nan=False)
            elif kind == "missing_required":
                record.pop(field, None)
                record.pop(ALIASES[field], None)
            elif kind in ("null_value", "incompatible_type"):
                for name in (field, ALIASES[field]):
                    record[name] = None if kind == "null_value" else {"invalid": True}
            elif kind == "invalid_timestamp":
                record["timestamp"] = "2026-02-30T12:00:00Z"
            elif kind == "malformed_json":
                contaminated[index] = '{"machine_id":'
            if not pair and kind != "malformed_json":
                contaminated[index] = json.dumps(record, ensure_ascii=False, allow_nan=False)
            breakdown[kind] += 1
            scenarios.append({
                "type": kind, "line_numbers": [i + 1 for i in selected],
                "modified_line_numbers": [i + 1 for i in selected if contaminated[i] != lines[i]],
            })

    output.parent.mkdir(parents=True, exist_ok=True)
    if budget == 0:
        shutil.copyfile(source, output)
    else:
        with output.open("w", encoding="utf-8", newline="\n") as handle:
            for line in contaminated:
                handle.write(line + "\n")
    return ContaminationSummary(
        CONTAMINATION_VERSION, dirty_seed, dirty_rate, budget / total if total else 0.0,
        total, budget, sum(a != b for a, b in zip(lines, contaminated)),
        source_hash, file_sha256(output), breakdown,
        [kind for kind, count in breakdown.items() if not count], scenarios,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dirty-rate", type=float, default=0.0)
    parser.add_argument("--dirty-seed", type=int, default=42)
    args = parser.parse_args()
    try:
        summary = contaminate_jsonl(args.input, args.output, args.dirty_rate, args.dirty_seed)
    except (ValueError, OSError) as error:
        parser.exit(1, f"Error de contaminación: {error}\n")
    print(json.dumps(asdict(summary), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
