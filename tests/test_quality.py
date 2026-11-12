"""Pruebas del flujo de calidad con una sesión real de Spark local."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.processing.quality import QualityMetrics, clean_telemetry
from src.processing.schema import TELEMETRY_SCHEMA
from src.processing.spark import create_spark_session
from src.jsonl import file_sha256


SAMPLE_PATH = Path(__file__).parent / "fixtures" / "processing_sample.jsonl"
BASE_RECORD = json.loads(SAMPLE_PATH.read_text(encoding="utf-8").splitlines()[0])


class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

    def write_lines(self, lines):
        path = Path(self.temporary_directory.name) / "telemetry.jsonl"
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return path

    def record(self, **changes):
        return json.dumps({**BASE_RECORD, **changes}, ensure_ascii=False)

    def assert_counts(self, result, received, valid, rejected, duplicates):
        self.assertEqual(
            result.metrics, QualityMetrics(received, valid, rejected, duplicates)
        )
        self.assertEqual(result.valid.count(), valid)
        self.assertEqual(result.rejected.count(), rejected)
        self.assertEqual(received, valid + rejected)

    def test_sample_is_all_valid(self):
        result = clean_telemetry(self.spark, SAMPLE_PATH)
        self.assert_counts(result, 5, 5, 0, 0)
        self.assertEqual(result.valid.schema, TELEMETRY_SCHEMA)

    def test_identical_duplicates_keep_one_and_preserve_rejected_lines(self):
        first = self.record()
        extra = json.dumps({**BASE_RECORD, "sensor_description": "ignorado"})
        result = clean_telemetry(self.spark, self.write_lines([first, first, extra]))
        self.assert_counts(result, 3, 1, 2, 2)
        self.assertEqual(
            {tuple(row.rejection_reasons) for row in result.rejected.collect()},
            {("DUPLICATE_KEY",)},
        )
        self.assertCountEqual(
            [row.raw_line for row in result.rejected.collect()], [first, first]
            if extra < first else [first, extra],
        )

    def test_rejection_batch_id_defaults_to_hash_or_uses_explicit_identity(self):
        path = self.write_lines([self.record(pressure=None)])
        default = clean_telemetry(self.spark, path)
        self.assertEqual(default.rejected.first().batch_id, file_sha256(path))
        explicit = clean_telemetry(self.spark, path, batch_id="manifest-batch")
        self.assertEqual(explicit.rejected.first().batch_id, "manifest-batch")

    def test_identical_normalized_duplicates_retain_lowest_source_line_number(self):
        original = self.record()
        path = self.write_lines([
            self.record(source_raw_line=original, source_line_number=number)
            for number in (20, 3, 8)
        ])
        result = clean_telemetry(self.spark, path, normalized_input=True, batch_id="manifest-batch")
        self.assertEqual(result.metrics, QualityMetrics(3, 1, 2, 2))
        rejected = result.rejected.collect()
        self.assertEqual({row.line_number for row in rejected}, {8, 20})
        self.assertTrue(all(row.raw_line == original and row.batch_id == "manifest-batch" for row in rejected))

    def test_conflicting_measurements_reject_every_row_of_key(self):
        first = self.record()
        different = self.record(temperature=66.0)
        unrelated = self.record(machine_id="MAQ-002")
        result = clean_telemetry(
            self.spark, self.write_lines([first, first, different, unrelated])
        )
        self.assert_counts(result, 4, 1, 3, 3)
        self.assertCountEqual(
            [row.raw_line for row in result.rejected.collect()], [first, first, different]
        )
        self.assertTrue(
            all(
                row.rejection_reasons == ["CONFLICTING_DUPLICATE_KEY"]
                for row in result.rejected.collect()
            )
        )

    def test_missing_and_null_fields_get_field_reasons(self):
        lines = []
        expected = {}
        for field in TELEMETRY_SCHEMA.fieldNames():
            missing = BASE_RECORD.copy()
            del missing[field]
            null = {**BASE_RECORD, field: None}
            for record in (missing, null):
                line = json.dumps(record)
                lines.append(line)
                expected[line] = [f"NULL_{field.upper()}"]
        result = clean_telemetry(self.spark, self.write_lines(lines))
        self.assert_counts(result, 10, 0, 10, 0)
        self.assertEqual(
            {row.raw_line: row.rejection_reasons for row in result.rejected.collect()},
            expected,
        )

    def test_malformed_type_date_and_blank_lines_are_rejected(self):
        valid = self.record(machine_id="MAQ-002")
        lines = [
            valid,
            '{"machine_id":',
            self.record(temperature="dato inválido"),
            self.record(timestamp="fecha inválida"),
            self.record(timestamp="2026-10-25T12:00:00"),
            valid.replace('"machine_id"', "'machine_id'"),
            "",
        ]
        result = clean_telemetry(self.spark, self.write_lines(lines))
        self.assert_counts(result, 7, 1, 6, 0)
        self.assertCountEqual(
            [row.raw_line for row in result.rejected.collect()], lines[1:]
        )
        self.assertTrue(
            all(row.rejection_reasons == ["PARSE_ERROR"] for row in result.rejected.collect())
        )

    def test_normalized_rejects_keep_input_text_without_source_metadata(self):
        """Conservar el texto recibido si la línea de origen es nula o está ausente."""
        missing = self.record(pressure=None, source_line_number=1)
        null = self.record(pressure=None, source_line_number=2, source_raw_line=None)
        result = clean_telemetry(self.spark, self.write_lines([missing, null]), normalized_input=True)
        self.assertEqual(
            {row.line_number: row.raw_line for row in result.rejected.collect()},
            {1: missing, 2: null},
        )

    def test_rejects_trailing_content_after_json_object(self):
        valid = self.record()
        lines = [
            valid,
            valid + " " + valid,
            valid + " trailing",
            valid + ", " + valid,
            valid + "] " + valid,
            "[" + valid + "]",
        ]
        result = clean_telemetry(self.spark, self.write_lines(lines))
        self.assert_counts(result, 6, 1, 5, 0)
        self.assertCountEqual(
            [row.raw_line for row in result.rejected.collect()], lines[1:]
        )
        self.assertTrue(
            all(row.rejection_reasons == ["PARSE_ERROR"] for row in result.rejected.collect())
        )

    def test_accepts_nested_extra_field_and_quoted_braces(self):
        nested = json.dumps(
            {**BASE_RECORD, "metadata": {"note": 'llave } y comilla " escapada'}},
            ensure_ascii=False,
        )
        padded = " \t" + nested + " \t"
        result = clean_telemetry(self.spark, self.write_lines([padded]))
        self.assert_counts(result, 1, 1, 0, 0)

    def test_blank_machine_id_and_non_finite_measurements(self):
        lines = [
            self.record(machine_id="   "),
            self.record(temperature=float("nan")),
            self.record(vibration=float("inf")),
            self.record(pressure=float("-inf")),
        ]
        result = clean_telemetry(self.spark, self.write_lines(lines))
        self.assert_counts(result, 4, 0, 4, 0)
        reasons = {row.raw_line: row.rejection_reasons for row in result.rejected.collect()}
        self.assertEqual(reasons[lines[0]], ["EMPTY_MACHINE_ID"])
        for field, line in zip(("TEMPERATURE", "VIBRATION", "PRESSURE"), lines[1:]):
            self.assertEqual(reasons[line], [f"NON_FINITE_{field}"])

    def test_tab_and_unicode_whitespace_machine_ids_are_rejected(self):
        lines = [
            self.record(machine_id="\t"),
            self.record(machine_id=" \t "),
            self.record(machine_id="\u2003"),
        ]
        result = clean_telemetry(self.spark, self.write_lines(lines))
        self.assert_counts(result, 3, 0, 3, 0)
        self.assertTrue(
            all(
                row.rejection_reasons == ["EMPTY_MACHINE_ID"]
                for row in result.rejected.collect()
            )
        )

    def test_multiple_content_reasons_and_finite_anomaly(self):
        invalid = self.record(machine_id=" ", pressure=None)
        extreme_but_finite = self.record(machine_id="MAQ-002", temperature=9999.0)
        result = clean_telemetry(
            self.spark, self.write_lines([invalid, extreme_but_finite])
        )
        self.assert_counts(result, 2, 1, 1, 0)
        self.assertEqual(
            result.rejected.first().rejection_reasons,
            ["NULL_PRESSURE", "EMPTY_MACHINE_ID"],
        )
        self.assertEqual(result.valid.first().temperature, 9999.0)

    def test_invalid_row_does_not_compete_with_valid_duplicate_key(self):
        valid = self.record()
        invalid = self.record(pressure=None)
        result = clean_telemetry(self.spark, self.write_lines([valid, invalid]))
        self.assert_counts(result, 2, 1, 1, 0)
        rejected = result.rejected.first()
        self.assertEqual(rejected.raw_line, invalid)
        self.assertEqual(rejected.rejection_reasons, ["NULL_PRESSURE"])

    def test_equal_instant_with_different_offsets_is_duplicate(self):
        utc = self.record()
        offset = self.record(timestamp="2026-10-25T09:00:00-03:00")
        result = clean_telemetry(self.spark, self.write_lines([utc, offset]))
        self.assert_counts(result, 2, 1, 1, 1)
        self.assertEqual(result.rejected.first().rejection_reasons, ["DUPLICATE_KEY"])

    def test_empty_file_and_missing_path(self):
        empty = clean_telemetry(self.spark, self.write_lines([]))
        self.assert_counts(empty, 0, 0, 0, 0)
        with self.assertRaisesRegex(FileNotFoundError, "archivo de entrada"):
            clean_telemetry(self.spark, SAMPLE_PATH.with_name("nonexistent.jsonl"))

    def test_trailing_blank_lines_and_unicode_keep_original_line_numbers(self):
        first = self.record(pressure=None, note="a\u2028b\u0085c")
        path = self.write_lines([first, "", ""])
        # También conservar el conteo con terminadores Windows.
        path.write_bytes(("\r\n".join([first, "", ""]) + "\r\n").encode("utf-8"))
        result = clean_telemetry(self.spark, path)
        self.assert_counts(result, 3, 0, 3, 0)
        rejected = result.rejected.orderBy("line_number").collect()
        self.assertEqual([row.line_number for row in rejected], [1, 2, 3])
        self.assertEqual([row.raw_line for row in rejected], [first, "", ""])
        self.assertTrue(all(row.stage == "spark" and row.batch_id for row in rejected))


if __name__ == "__main__":
    unittest.main()
