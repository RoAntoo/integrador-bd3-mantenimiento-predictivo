"""Integración real JSONL → calidad existente → estadísticas → riesgo."""

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.processing.analysis import analyze_telemetry
from src.processing.quality import QualityMetrics
from src.processing.spark import create_spark_session


class AnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def analyze(self, lines):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "input.jsonl"
        path.write_text("\n".join(lines), encoding="utf-8")
        return analyze_telemetry(self.spark, path, datetime(2026, 11, 9, tzinfo=timezone.utc))

    def test_duplicates_rejections_and_extra_labels_do_not_change_statistics(self):
        base = dict(machine_id="A", timestamp="2026-11-08T12:00:00Z",
                    temperature=60., vibration=2., pressure=5.)
        row = json.dumps(base)
        result = self.analyze([
            row, row,
            json.dumps({**base, "timestamp": "2026-11-08T12:01:00Z", "temperature": 94., "is_anomaly": False}),
            json.dumps({**base, "machine_id": "conflict", "temperature": 100.}),
            json.dumps({**base, "machine_id": "conflict", "temperature": 200.}),
            json.dumps({**base, "machine_id": "null", "pressure": None}),
            "not json",
        ])
        self.assertEqual(result.quality.metrics, QualityMetrics(7, 2, 5, 3))
        self.assertEqual(result.quality.valid.columns, ["machine_id", "timestamp", "temperature", "vibration", "pressure"])
        self.assertEqual(result.quality.rejected.count(), 5)
        stats = result.machine_statistics.collect()
        self.assertEqual(len(stats), 1)
        self.assertEqual((stats[0].n_readings, stats[0].avg_temp, stats[0].max_temp), (2, 77., 94.))
        self.assertEqual(result.sensor_metrics_5m.first().n_readings, 2)
        self.assertEqual(result.machine_risk_latest.first().risk_level, "ROJO")

    def test_empty_and_all_rejected_preserve_schemas(self):
        empty = self.analyze([])
        rejected = self.analyze(["bad json"])
        self.assertEqual(empty.quality.metrics, QualityMetrics(0, 0, 0, 0))
        self.assertEqual(rejected.quality.metrics, QualityMetrics(1, 0, 1, 0))
        for name in ("machine_statistics", "sensor_metrics_5m", "machine_risk_latest"):
            self.assertEqual(getattr(empty, name).schema, getattr(rejected, name).schema)
            self.assertEqual(getattr(empty, name).count(), 0)
            self.assertEqual(getattr(rejected, name).count(), 0)


if __name__ == "__main__":
    unittest.main()
