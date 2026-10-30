"""Simulador determinista de telemetría industrial con inyección de anomalías (LEO-01)."""

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import random
from typing import Any

from src.generator.catalog import (
    DEFAULT_CATALOG_PATH,
    MachineProfile,
    build_machine_catalog,
    save_machine_catalog,
)


DEFAULT_START_TIME = "2026-10-25T00:00:00Z"
DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "sample" / "telemetry_sample.jsonl"
)

ANOMALY_TYPES = (
    "overheating_spike",
    "vibration_spike",
    "pressure_anomaly",
    "sustained_degradation",
)


@dataclass(frozen=True)
class GenerationConfig:
    """Parámetros de configuración para la simulación por lote."""

    machines: int = 10
    days: float = 1.0
    interval: int = 1
    seed: int = 42
    anomaly_rate: float = 0.05
    output: Path = DEFAULT_OUTPUT_PATH
    start_time: str = DEFAULT_START_TIME

    def __post_init__(self) -> None:
        if self.machines <= 0:
            raise ValueError("El parámetro 'machines' debe ser mayor a 0.")
        if self.days <= 0:
            raise ValueError("El parámetro 'days' debe ser mayor a 0.")
        if self.interval <= 0:
            raise ValueError("El parámetro 'interval' debe ser mayor a 0.")
        if not 0.0 <= self.anomaly_rate <= 1.0:
            raise ValueError("El parámetro 'anomaly_rate' debe estar entre 0.0 y 1.0.")


@dataclass(frozen=True)
class GenerationSummary:
    """Resumen verificable de una corrida del generador."""

    output_path: Path
    machines_count: int
    steps_per_machine: int
    total_events: int
    normal_events: int
    anomalous_events: int
    seed: int
    anomaly_rate: float
    start_timestamp: str
    end_timestamp: str
    anomaly_breakdown: dict[str, int] = field(default_factory=dict)
    injected_anomalies: list[dict[str, Any]] = field(default_factory=list)


def _parse_utc_timestamp(iso_timestamp: str) -> datetime:
    """Convertir un timestamp ISO 8601 a datetime en UTC."""
    normalized = iso_timestamp.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc_timestamp(moment: datetime) -> str:
    """Formatear un datetime en UTC con sufijo Z."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _plan_anomalies(
    machines_count: int,
    steps_per_machine: int,
    anomaly_rate: float,
    rng: random.Random,
) -> dict[tuple[int, int], tuple[str, int]]:
    """Planificar de forma determinista qué lecturas (máquina, paso) tendrán fallas.

    Devuelve un diccionario {(machine_idx, step_idx): (anomaly_type, progression_step)}.
    Incluye tanto anomalías puntuales como episodios de degradación sostenida.
    """
    total_events = machines_count * steps_per_machine
    if anomaly_rate <= 0.0 or total_events == 0:
        return {}

    target_anomalies = round(total_events * anomaly_rate)
    if target_anomalies == 0 and anomaly_rate > 0.0:
        target_anomalies = 1
    if anomaly_rate < 1.0 and total_events > 1 and target_anomalies >= total_events:
        target_anomalies = total_events - 1

    planned: dict[tuple[int, int], tuple[str, int]] = {}

    # Si hay suficientes pasos y eventos, reservar una fracción para degradación sostenida
    sustained_budget = target_anomalies // 3 if steps_per_machine >= 4 else 0
    while sustained_budget >= 3:
        burst_len = min(rng.randint(3, 5), sustained_budget, steps_per_machine)
        m_idx = rng.randrange(machines_count)
        max_start = max(0, steps_per_machine - burst_len)
        start_step = rng.randrange(max_start + 1)
        slots = [(m_idx, start_step + offset) for offset in range(burst_len)]
        if any(slot in planned for slot in slots):
            sustained_budget -= 1
            continue
        for offset, slot in enumerate(slots):
            planned[slot] = ("sustained_degradation", offset)
        sustained_budget -= burst_len

    # Completar el presupuesto restante con anomalías puntuales
    point_types = ("overheating_spike", "vibration_spike", "pressure_anomaly")
    all_candidates = [
        (m_idx, s_idx)
        for s_idx in range(steps_per_machine)
        for m_idx in range(machines_count)
        if (m_idx, s_idx) not in planned
    ]
    rng.shuffle(all_candidates)

    remaining = max(0, target_anomalies - len(planned))
    for slot in all_candidates[:remaining]:
        chosen_type = point_types[rng.randrange(len(point_types))]
        planned[slot] = (chosen_type, 0)

    return planned


def _simulate_reading(
    profile: MachineProfile,
    step_idx: int,
    interval_minutes: int,
    timestamp_str: str,
    anomaly_info: tuple[str, int] | None,
    rng: random.Random,
) -> dict[str, Any]:
    """Generar un único evento de sensor siguiendo el Contrato A y el esquema Spark."""
    # Variación armónica suave para simular el ciclo operativo diario
    elapsed_minutes = step_idx * interval_minutes
    daily_phase = (2.0 * math.pi * elapsed_minutes) / 1440.0
    harmonic = math.sin(daily_phase)

    # Estado operativo base
    state_roll = rng.random()
    if anomaly_info is not None:
        machine_state = "operating"
    elif state_roll < 0.90:
        machine_state = "operating"
    elif state_roll < 0.97:
        machine_state = "idle"
    else:
        machine_state = "stopped"

    # Valores normales según estado
    if machine_state == "stopped":
        temperature_c = round(25.0 + rng.uniform(-1.5, 2.0), 2)
        vibration_mm_s = round(rng.uniform(0.02, 0.15), 2)
        pressure_bar = round(1.0 + rng.uniform(-0.05, 0.10), 2)
        rpm = 0
        power_kw = round(rng.uniform(0.1, 0.5), 2)
    elif machine_state == "idle":
        temperature_c = round(
            profile.base_temperature_c * 0.78 + rng.gauss(0.0, 0.8), 2
        )
        vibration_mm_s = round(
            max(0.2, profile.base_vibration_mm_s * 0.45 + rng.gauss(0.0, 0.1)), 2
        )
        pressure_bar = round(
            profile.base_pressure_bar * 0.70 + rng.gauss(0.0, 0.12), 2
        )
        rpm = int(round(profile.base_rpm * 0.35 + rng.uniform(-25, 25)))
        power_kw = round(profile.base_power_kw * 0.30 + rng.uniform(-0.8, 0.8), 2)
    else:
        temperature_c = round(
            profile.base_temperature_c + 2.2 * harmonic + rng.gauss(0.0, 1.1), 2
        )
        vibration_mm_s = round(
            max(
                0.5,
                profile.base_vibration_mm_s + 0.18 * harmonic + rng.gauss(0.0, 0.15),
            ),
            2,
        )
        pressure_bar = round(
            max(
                2.0,
                profile.base_pressure_bar + 0.15 * harmonic + rng.gauss(0.0, 0.12),
            ),
            2,
        )
        rpm = int(round(profile.base_rpm + 20 * harmonic + rng.uniform(-30, 30)))
        power_kw = round(
            profile.base_power_kw + 1.5 * harmonic + rng.gauss(0.0, 0.7), 2
        )

    # Inyección de anomalía si corresponde
    is_anomaly = anomaly_info is not None
    anomaly_type: str | None = None

    if anomaly_info is not None:
        anomaly_type, progression = anomaly_info
        if anomaly_type == "overheating_spike":
            temperature_c = round(rng.uniform(94.0, 114.0), 2)
            power_kw = round(power_kw * rng.uniform(1.20, 1.40), 2)
        elif anomaly_type == "vibration_spike":
            vibration_mm_s = round(rng.uniform(6.8, 11.5), 2)
            rpm = int(round(rpm * rng.uniform(0.88, 1.12)))
        elif anomaly_type == "pressure_anomaly":
            # Puede ser sobrepresión alta o caída brusca por fuga
            if rng.random() < 0.5:
                pressure_bar = round(rng.uniform(1.1, 2.2), 2)
            else:
                pressure_bar = round(rng.uniform(8.6, 10.8), 2)
        elif anomaly_type == "sustained_degradation":
            # Incremento progresivo en ráfaga
            temperature_c = round(89.0 + progression * 3.8 + rng.uniform(0.5, 2.5), 2)
            vibration_mm_s = round(5.8 + progression * 0.95 + rng.uniform(0.2, 0.8), 2)
            pressure_bar = round(7.9 + progression * 0.45 + rng.uniform(0.1, 0.4), 2)
            power_kw = round(power_kw * (1.15 + progression * 0.05), 2)

    operating_hours = round(
        profile.initial_operating_hours + (elapsed_minutes / 60.0), 2
    )

    # Se incluyen los nombres cortos esperados por src/processing/schema.py
    # y los nombres explícitos del Contrato A del plan de implementación.
    return {
        "machine_id": profile.machine_id,
        "timestamp": timestamp_str,
        "temperature": temperature_c,
        "vibration": vibration_mm_s,
        "pressure": pressure_bar,
        "temperature_c": temperature_c,
        "vibration_mm_s": vibration_mm_s,
        "pressure_bar": pressure_bar,
        "rpm": rpm,
        "power_kw": power_kw,
        "operating_hours": operating_hours,
        "machine_state": machine_state,
        "is_anomaly": is_anomaly,
        "anomaly_type": anomaly_type,
    }


def generate_telemetry(config: GenerationConfig) -> GenerationSummary:
    """Generar archivo JSONL de telemetría sintética y devolver métricas del lote."""
    rng = random.Random(config.seed)
    catalog = build_machine_catalog(config.machines)
    if not DEFAULT_CATALOG_PATH.is_file():
        save_machine_catalog(max(10, config.machines), DEFAULT_CATALOG_PATH)
    steps_per_machine = int((config.days * 24 * 60) // config.interval)
    if steps_per_machine <= 0:
        raise ValueError(
            "La combinación de 'days' e 'interval' produce 0 lecturas por máquina."
        )

    start_dt = _parse_utc_timestamp(config.start_time)
    planned_anomalies = _plan_anomalies(
        machines_count=config.machines,
        steps_per_machine=steps_per_machine,
        anomaly_rate=config.anomaly_rate,
        rng=rng,
    )

    output_path = Path(config.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    anomaly_breakdown: dict[str, int] = {kind: 0 for kind in ANOMALY_TYPES}
    injected_anomalies: list[dict[str, Any]] = []
    normal_count = 0
    anomalous_count = 0

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for step_idx in range(steps_per_machine):
            current_dt = start_dt + timedelta(minutes=step_idx * config.interval)
            timestamp_str = _format_utc_timestamp(current_dt)

            for m_idx, profile in enumerate(catalog):
                anomaly_info = planned_anomalies.get((m_idx, step_idx))
                record = _simulate_reading(
                    profile=profile,
                    step_idx=step_idx,
                    interval_minutes=config.interval,
                    timestamp_str=timestamp_str,
                    anomaly_info=anomaly_info,
                    rng=rng,
                )
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")

                if record["is_anomaly"]:
                    anomalous_count += 1
                    a_type = str(record["anomaly_type"])
                    anomaly_breakdown[a_type] = anomaly_breakdown.get(a_type, 0) + 1
                    injected_anomalies.append(
                        {
                            "machine_id": record["machine_id"],
                            "timestamp": record["timestamp"],
                            "anomaly_type": a_type,
                            "temperature_c": record["temperature_c"],
                            "vibration_mm_s": record["vibration_mm_s"],
                            "pressure_bar": record["pressure_bar"],
                        }
                    )
                else:
                    normal_count += 1

    end_dt = start_dt + timedelta(minutes=(steps_per_machine - 1) * config.interval)
    return GenerationSummary(
        output_path=output_path.resolve(),
        machines_count=config.machines,
        steps_per_machine=steps_per_machine,
        total_events=normal_count + anomalous_count,
        normal_events=normal_count,
        anomalous_events=anomalous_count,
        seed=config.seed,
        anomaly_rate=config.anomaly_rate,
        start_timestamp=_format_utc_timestamp(start_dt),
        end_timestamp=_format_utc_timestamp(end_dt),
        anomaly_breakdown={k: v for k, v in anomaly_breakdown.items() if v > 0},
        injected_anomalies=injected_anomalies,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    """Construir el parser CLI con los argumentos definidos en LEO-01."""
    parser = argparse.ArgumentParser(
        description="Generador determinista de telemetría industrial en formato JSONL."
    )
    parser.add_argument(
        "--machines",
        type=int,
        default=10,
        help="Cantidad de máquinas a simular (ej. 10, 50).",
    )
    parser.add_argument(
        "--days",
        type=float,
        default=1.0,
        help="Cantidad de días a simular (ej. 1, 7).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="Intervalo entre lecturas en minutos (ej. 1, 5, 60).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla aleatoria para garantizar reproducibilidad.",
    )
    parser.add_argument(
        "--anomaly_rate",
        "--anomaly-rate",
        dest="anomaly_rate",
        type=float,
        default=0.05,
        help="Proporción de eventos anómalos entre 0.0 y 1.0 (ej. 0.05).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Ruta del archivo JSONL de salida.",
    )
    parser.add_argument(
        "--start-time",
        "--start_time",
        dest="start_time",
        type=str,
        default=DEFAULT_START_TIME,
        help="Timestamp UTC inicial en formato ISO 8601 (por defecto: 2026-10-25T00:00:00Z).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Punto de entrada por línea de comandos para generar muestras de sensores."""
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        config = GenerationConfig(
            machines=args.machines,
            days=args.days,
            interval=args.interval,
            seed=args.seed,
            anomaly_rate=args.anomaly_rate,
            output=args.output,
            start_time=args.start_time,
        )
        summary = generate_telemetry(config)
    except ValueError as error:
        parser.exit(status=1, message=f"Error de configuración: {error}\n")

    print("=== Resumen de generación de telemetría (LEO-01) ===")
    print(f"Archivo de salida: {summary.output_path}")
    print(f"Máquinas simuladas: {summary.machines_count} (MAQ-001 .. MAQ-{summary.machines_count:03d})")
    print(f"Lecturas por máquina: {summary.steps_per_machine}")
    print(f"Rango temporal UTC: {summary.start_timestamp} -> {summary.end_timestamp}")
    print(f"Semilla (seed): {summary.seed} | Tasa de anomalías objetivo: {summary.anomaly_rate}")
    print(f"Total de eventos generados: {summary.total_events}")
    print(f"Eventos normales: {summary.normal_events}")
    print(f"Eventos anómalos: {summary.anomalous_events}")
    if summary.anomaly_breakdown:
        print("Desglose de anomalías inyectadas por tipo:")
        for anomaly_type, count in sorted(summary.anomaly_breakdown.items()):
            print(f"  - {anomaly_type}: {count}")
    if summary.injected_anomalies:
        preview = summary.injected_anomalies[:5]
        print(f"Muestra de anomalías inyectadas (primeras {len(preview)}):")
        for item in preview:
            print(
                f"  * [{item['timestamp']}] {item['machine_id']} -> {item['anomaly_type']} "
                f"(temp={item['temperature_c']}°C, vib={item['vibration_mm_s']}mm/s, pres={item['pressure_bar']}bar)"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
