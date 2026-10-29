"""Pruebas con una sesión real de Spark local, sin servicios externos."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.processing.etl import read_telemetry
from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session


SAMPLE_PATH = Path(__file__).parent / "fixtures" / "processing_sample.jsonl"


class ProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

    def write_input(self, content):
        input_path = Path(self.temporary_directory.name) / "telemetry.jsonl"
        input_path.write_text(content, encoding="utf-8")
        return input_path

    def test_reads_sample_with_expected_values_and_schema(self):
        telemetry = read_telemetry(self.spark, SAMPLE_PATH)
        self.assertEqual(telemetry.count(), 5)
        self.assertEqual(telemetry.schema, TELEMETRY_SCHEMA)
        self.assertEqual(self.spark.sparkContext.master, "local[2]")
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
        first_reading = telemetry.orderBy("timestamp", "machine_id").first()
        self.assertEqual(first_reading.machine_id, "MAQ-001")
        self.assertEqual(first_reading.temperature, 65.5)
        self.assertEqual(first_reading.vibration, 2.1)
        self.assertEqual(first_reading.pressure, 5.0)
        timestamp = (
            telemetry.orderBy("timestamp")
            .selectExpr("CAST(timestamp AS STRING)")
            .first()[0]
        )
        self.assertEqual(timestamp, "2026-10-25 12:00:00")

    def test_rejects_missing_or_null_required_fields(self):
        valid_record = json.loads(SAMPLE_PATH.read_text(encoding="utf-8").splitlines()[0])
        for field in TELEMETRY_SCHEMA.fieldNames():
            for missing in (True, False):
                with self.subTest(field=field, missing=missing):
                    invalid_record = valid_record.copy()
                    if missing:
                        del invalid_record[field]
                    else:
                        invalid_record[field] = None
                    input_path = self.write_input(json.dumps(invalid_record) + "\n")
                    with self.assertRaisesRegex(ValueError, "ausentes o nulos"):
                        read_telemetry(self.spark, input_path)

    def test_rejects_malformed_json(self):
        input_path = self.write_input('{"machine_id":\n')
        with self.assertRaisesRegex(ValueError, "esquema esperado"):
            read_telemetry(self.spark, input_path)

    def test_rejects_invalid_measurement_or_timestamp(self):
        valid_record = json.loads(SAMPLE_PATH.read_text(encoding="utf-8").splitlines()[0])
        for field, value in (
            ("temperature", "dato inválido"),
            ("timestamp", "fecha inválida"),
            ("timestamp", "2026-10-25T12:00:00"),
        ):
            with self.subTest(field=field, value=value):
                invalid_record = {**valid_record, field: value}
                # El error aparece después de registros válidos: no basta
                # con validar solamente el primer registro de la entrada.
                content = SAMPLE_PATH.read_text(encoding="utf-8")
                input_path = self.write_input(content + json.dumps(invalid_record) + "\n")
                with self.assertRaisesRegex(ValueError, "esquema esperado"):
                    read_telemetry(self.spark, input_path)

    def test_preserves_duplicates_and_ignores_extra_fields(self):
        record = json.loads(SAMPLE_PATH.read_text(encoding="utf-8").splitlines()[0])
        record["sensor_description"] = "Campo adicional de ejemplo"
        input_path = self.write_input((json.dumps(record) + "\n") * 2)
        telemetry = read_telemetry(self.spark, input_path)
        self.assertEqual(telemetry.count(), 2)
        self.assertEqual(telemetry.columns, TELEMETRY_SCHEMA.fieldNames())

    def test_accepts_empty_file(self):
        telemetry = read_telemetry(self.spark, self.write_input(""))
        self.assertEqual(telemetry.count(), 0)
        self.assertEqual(telemetry.schema, TELEMETRY_SCHEMA)

    def test_reports_missing_file(self):
        with self.assertRaisesRegex(FileNotFoundError, "archivo de entrada"):
            read_telemetry(self.spark, SAMPLE_PATH.with_name("nonexistent.jsonl"))


if __name__ == "__main__":
    unittest.main()
