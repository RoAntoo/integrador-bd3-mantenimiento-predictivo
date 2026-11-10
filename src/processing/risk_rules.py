"""Semáforo académico por umbrales; no predice fallas ni usa etiquetas sintéticas."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


@dataclass(frozen=True)
class SensorThresholds:
    yellow: float
    red: float

    def __post_init__(self):
        if any(isinstance(value, bool) or not isinstance(value, (int, float))
               or not math.isfinite(value) for value in (self.yellow, self.red)):
            raise ValueError("Los umbrales deben ser números finitos.")
        if self.yellow >= self.red:
            raise ValueError("El umbral amarillo debe ser menor que el rojo.")


@dataclass(frozen=True)
class RiskThresholds:
    temperature: SensorThresholds = SensorThresholds(89.5, 94.0)
    vibration: SensorThresholds = SensorThresholds(6.0, 6.8)
    pressure: SensorThresholds = SensorThresholds(8.0, 8.6)

    def __post_init__(self):
        if not all(isinstance(value, SensorThresholds)
                   for value in (self.temperature, self.vibration, self.pressure)):
            raise ValueError("Cada sensor requiere umbrales amarillo y rojo.")


def load_thresholds(path: str | Path) -> RiskThresholds:
    """Exigir las tres reglas completas; rechazar errores de claves o tipos."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"temperature", "vibration", "pressure"}:
        raise ValueError("La configuración requiere temperature, vibration y pressure.")
    rules = {}
    for sensor, values in payload.items():
        if not isinstance(values, dict) or set(values) != {"yellow", "red"}:
            raise ValueError(f"{sensor} requiere exactamente yellow y red.")
        rules[sensor] = SensorThresholds(**values)
    return RiskThresholds(**rules)


def execution_time(value: datetime) -> datetime:
    """Aceptar solo instantes conscientes de zona horaria y normalizar a UTC."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("calculated_at debe incluir una zona horaria explícita.")
    return value.astimezone(timezone.utc)


def classify_windows(metrics: DataFrame, thresholds: RiskThresholds = RiskThresholds()) -> DataFrame:
    """Clasificar cada ventana a partir de sus máximos y conservar los motivos."""
    levels, reasons = [], []
    for column, label, unit, rule in (
        ("max_temp", "Temperatura", "°C", thresholds.temperature),
        ("max_vibration", "Vibración", "mm/s", thresholds.vibration),
        ("max_pressure", "Presión alta", "bar", thresholds.pressure),
    ):
        value = F.col(column)
        level = F.when(value >= rule.red, 2).when(value >= rule.yellow, 1).otherwise(0)
        levels.append(level)
        reasons.append(F.when(level > 0, F.concat(
            F.lit(f"{label}: máximo "), value.cast("string"), F.lit(f" {unit}; "),
            F.when(level == 2, F.lit(f"ROJO (>= {rule.red} {unit})"))
             .otherwise(F.lit(f"AMARILLO (>= {rule.yellow} {unit})")),
        )))
    severity = F.greatest(*levels)
    return metrics.select(
        "machine_id", "window_start", "window_end",
        F.when(severity == 2, "ROJO").when(severity == 1, "AMARILLO")
         .otherwise("VERDE").alias("risk_level"),
        F.when(severity == 0, F.lit("Sin umbrales altos activados; presión baja no evaluada."))
         .otherwise(F.concat_ws("; ", *reasons)).alias("risk_reason"),
    )


def classify_risk(metrics: DataFrame, calculated_at: datetime,
                  thresholds: RiskThresholds = RiskThresholds()) -> DataFrame:
    """Clasificar máximos de la última ventana disponible de cada máquina."""
    instant = execution_time(calculated_at)
    latest = metrics.withColumn(
        "_position", F.row_number().over(
            Window.partitionBy("machine_id").orderBy(F.col("window_start").desc())
        ),
    ).filter(F.col("_position") == 1)
    return classify_windows(latest, thresholds).select(
        "machine_id", "window_start", "window_end",
        F.lit(instant).cast("timestamp").alias("calculated_at"),
        "risk_level", "risk_reason",
    )
