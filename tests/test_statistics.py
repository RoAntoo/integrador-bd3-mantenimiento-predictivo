"""Agregaciones verificadas contra resultados manuales con Spark real."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session
from src.processing.statistics import machine_statistics, sensor_metrics_5m


class StatisticsTests(unittest.TestCase):
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
        path.write_text("\n".join(json.dumps(dict(zip(TELEMETRY_SCHEMA.fieldNames(), row)))
                                  for row in rows), encoding="utf-8")
        return self.spark.read.schema(TELEMETRY_SCHEMA).json(path.as_uri())

    def test_statistics_and_half_open_windows(self):
        valid = self.frame([
            ("A", "2026-11-09T00:00:00Z", 10., 1., 2.),
            ("A", "2026-11-09T00:04:59.999Z", 30., 3., 6.),
            ("A", "2026-11-09T00:05:00Z", 80., 8., 10.),
            ("B", "2026-11-08T21:00:00-03:00", 20., 2., 4.),
        ])
        machines = {r.machine_id: r for r in machine_statistics(valid).collect()}
        self.assertEqual(machines["A"].n_readings, 3)
        for suffix, expected in (("temp", (40., 10., 80.)),
                                 ("vibration", (4., 1., 8.)),
                                 ("pressure", (6., 2., 10.))):
            for operation, value in zip(("avg", "min", "max"), expected):
                self.assertAlmostEqual(machines["A"][f"{operation}_{suffix}"], value)
        self.assertEqual(machines["B"].n_readings, 1)
        windows = sensor_metrics_5m(valid).orderBy("machine_id", "window_start").collect()
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0].n_readings, 2)
        self.assertEqual(windows[0].avg_temp, 20.)
        self.assertEqual(windows[0].min_vibration, 1.)
        self.assertEqual(windows[0].max_pressure, 6.)
        self.assertEqual(windows[0].window_end, windows[1].window_start)
        self.assertEqual(windows[0].window_start, windows[2].window_start)
        self.assertEqual((windows[0].window_end - windows[0].window_start).total_seconds(), 300)

    def test_empty_preserves_schema_and_creates_no_machine(self):
        empty = self.frame([])
        populated = self.frame([("A", "2026-11-09T00:00:00Z", 1., 2., 3.)])
        for function in (machine_statistics, sensor_metrics_5m):
            with self.subTest(function=function.__name__):
                self.assertEqual(function(empty).schema, function(populated).schema)
                self.assertEqual(function(empty).count(), 0)


if __name__ == "__main__":
    unittest.main()
