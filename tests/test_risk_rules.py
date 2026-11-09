"""Configuración y semáforo en los límites aprobados, con Spark real."""

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.processing.risk_rules import (
    RiskThresholds, SensorThresholds, classify_risk, execution_time, load_thresholds,
)
from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session
from src.processing.statistics import sensor_metrics_5m


INSTANT = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)


class ThresholdTests(unittest.TestCase):
    def test_defaults_match_example(self):
        self.assertEqual(load_thresholds(Path(__file__).parents[1] / "config/risk_thresholds.example.json"), RiskThresholds())

    def test_reject_invalid_thresholds(self):
        for yellow, red in ((1., 1.), (2., 1.), (float("nan"), 3.), (1., float("inf")), (True, 2.), ("1", 2.)):
            with self.subTest(yellow=yellow, red=red), self.assertRaises(ValueError):
                SensorThresholds(yellow, red)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "thresholds.json"
            for payload in ({}, [], {"temperature": {}},
                            {sensor: {"yellow": 1, "red": 2, "typo": 3}
                             for sensor in ("temperature", "vibration", "pressure")}):
                path.write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_thresholds(path)

    def test_execution_time_requires_zone_and_normalizes(self):
        with self.assertRaises(ValueError):
            execution_time(datetime(2026, 11, 9))
        self.assertEqual(execution_time(datetime.fromisoformat("2026-11-09T09:00:00-03:00")), INSTANT)


class RiskTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "readings.jsonl"
        path.write_text("\n".join(json.dumps(dict(zip(
            TELEMETRY_SCHEMA.fieldNames(),
            (machine, timestamp.isoformat(), temp, vibration, pressure),
        ))) for machine, timestamp, temp, vibration, pressure in rows), encoding="utf-8")
        return self.spark.read.schema(TELEMETRY_SCHEMA).json(path.as_uri())

    def test_all_sensor_boundaries_and_reasons(self):
        rows, expected = [], {}
        for index, (sensor, yellow, red, label) in enumerate((
            ("temperature", 89.5, 94., "Temperatura"),
            ("vibration", 6., 6.8, "Vibración"),
            ("pressure", 8., 8.6, "Presión alta"),
        )):
            for value, color in ((yellow - .001, "VERDE"), (yellow, "AMARILLO"),
                                 (yellow + .001, "AMARILLO"), (red - .001, "AMARILLO"),
                                 (red, "ROJO"), (red + .001, "ROJO")):
                machine = f"{sensor}-{value}"
                readings = [60., 2., 5.]
                readings[index] = value
                rows.append((machine, INSTANT, *readings))
                expected[machine] = (color, label, value)
        result = classify_risk(sensor_metrics_5m(self.frame(rows)), INSTANT).collect()
        self.assertEqual(len(result), len(expected))
        for row in result:
            color, label, value = expected[row.machine_id]
            with self.subTest(machine=row.machine_id):
                self.assertEqual(row.risk_level, color)
                if color != "VERDE":
                    self.assertIn(label, row.risk_reason)
                    self.assertIn(str(value), row.risk_reason)
                    self.assertIn(color, row.risk_reason)

    def test_latest_window_priority_maximum_and_custom_rules(self):
        earlier = datetime.fromisoformat("2026-11-09T10:00:00Z")
        later = datetime.fromisoformat("2026-11-09T11:00:00Z")
        valid = self.frame([
            ("recovered", earlier, 100., 9., 10.),
            ("recovered", later, 60., 2., 1.),
            ("mixed", earlier, 89.5, 6.8, 8.),
            ("mixed", earlier.replace(minute=1), 20., 1., 1.),
            ("yellow", earlier, 89.5, 2., 5.),
        ])
        metrics = sensor_metrics_5m(valid)
        rows = {r.machine_id: r for r in classify_risk(metrics, INSTANT).collect()}
        self.assertEqual(rows["recovered"].risk_level, "VERDE")
        self.assertEqual(rows["yellow"].risk_level, "AMARILLO")
        self.assertEqual(rows["mixed"].risk_level, "ROJO")
        for label in ("Temperatura", "Vibración", "Presión alta", "AMARILLO", "ROJO"):
            self.assertIn(label, rows["mixed"].risk_reason)
        self.assertGreater(rows["recovered"].window_start, rows["mixed"].window_start)
        # Comparar instantes dentro de Spark evita depender de la zona del host Python.
        result = classify_risk(metrics, INSTANT)
        from pyspark.sql import functions as F
        self.assertEqual(result.filter(F.col("calculated_at") != F.lit(INSTANT)).count(), 0)
        relaxed = RiskThresholds(*(SensorThresholds(200., 300.) for _ in range(3)))
        self.assertEqual({r.risk_level for r in classify_risk(metrics, INSTANT, relaxed).collect()}, {"VERDE"})

    def test_empty_keeps_full_schema(self):
        empty = sensor_metrics_5m(self.frame([]))
        full = sensor_metrics_5m(self.frame([("A", INSTANT, 60., 2., 5.)]))
        self.assertEqual(classify_risk(empty, INSTANT).schema, classify_risk(full, INSTANT).schema)
        self.assertEqual(classify_risk(empty, INSTANT).count(), 0)


if __name__ == "__main__":
    unittest.main()
