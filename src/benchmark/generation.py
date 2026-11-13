"""Generación acotada en memoria que reutiliza la simulación de Leo."""

from datetime import timedelta
import json
from pathlib import Path
import random
from typing import Callable

from src.generator.catalog import build_machine_catalog
from src.generator.generate import (
    ANOMALY_TYPES, DEFAULT_START_TIME, _format_utc_timestamp,
    _parse_utc_timestamp, _simulate_reading,
)


def generate_jsonl(output: Path, *, machines: int, readings_per_machine: int,
                   seed: int = 42, anomaly_rate: float = 0.05,
                   start_time: str = DEFAULT_START_TIME, interval: int = 1,
                   progress: Callable[[dict], None] = lambda counts: None) -> dict:
    """Escribir una lectura por vez, sin plan ni lista de tamaño proporcional a N.

    El muestreo secuencial reserva exactamente el presupuesto de anomalías.
    Las degradaciones duran hasta cinco pasos consecutivos de una máquina.
    El orden máquina/paso y el algoritmo difieren del generador de la demo;
    una misma configuración de benchmark reproduce los mismos bytes.
    """
    rng = random.Random(seed)
    anomaly_rng = random.Random(seed)
    start = _parse_utc_timestamp(start_time)
    total = machines * readings_per_machine
    budget = round(total * anomaly_rate)
    if anomaly_rate > 0 and budget == 0:
        budget = 1
    if anomaly_rate < 1 and total > 1:
        budget = min(budget, total - 1)
    counts = {"generated": 0, "anomalous": 0,
              "anomaly_breakdown": dict.fromkeys(ANOMALY_TYPES, 0)}
    # Solo el catálogo depende de las máquinas (1.500 perfiles en el escenario mayor).
    try:
        with output.open("x", encoding="utf-8", newline="\n", buffering=1024 * 1024) as target:
            for profile in build_machine_catalog(machines):
                burst_left = 0
                progression = 0
                for step in range(readings_per_machine):
                    anomaly = None
                    remaining = total - counts["generated"]
                    if burst_left:
                        anomaly = ("sustained_degradation", progression)
                        burst_left -= 1
                        progression += 1
                    elif anomaly_rng.randrange(remaining) < budget:
                        kind = anomaly_rng.choice(ANOMALY_TYPES)
                        anomaly = (kind, 0)
                        if kind == "sustained_degradation":
                            length = min(anomaly_rng.randint(3, 5), budget,
                                         readings_per_machine - step)
                            burst_left = length - 1
                            progression = 1
                    timestamp = _format_utc_timestamp(start + timedelta(minutes=step * interval))
                    record = _simulate_reading(profile, step, interval, timestamp, anomaly, rng)
                    target.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
                    counts["generated"] += 1
                    if anomaly is not None:
                        budget -= 1
                        counts["anomalous"] += 1
                        counts["anomaly_breakdown"][anomaly[0]] += 1
                    if counts["generated"] % 10000 == 0:
                        progress(counts)
    finally:
        progress(counts)
    return counts
