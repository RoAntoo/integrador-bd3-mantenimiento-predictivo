"""Pruebas automáticas de generación, ingesta, zona raw y Parquet (LEO-01 a LEO-05)."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.generator.catalog import (
    DEFAULT_CATALOG_PATH,
    build_machine_catalog,
    load_machine_catalog,
    save_machine_catalog,
)
from src.generator.generate import (
    DEFAULT_OUTPUT_PATH,
    GenerationConfig,
    generate_telemetry,
    main as generator_main,
)
from src.ingestion.ingest import (
    ingest_telemetry_batch,
    main as ingest_main,
    read_parquet_partition,
    validate_and_normalize_record,
)

try:
    from pyspark.sql import SparkSession
    from src.processing.etl import read_telemetry
    from src.processing.quality import clean_telemetry
    from src.processing.spark import create_spark_session

    HAS_PYSPARK = True
except ModuleNotFoundError:
    HAS_PYSPARK = False

try:
    import pyarrow.parquet as pq  # noqa: F401

    HAS_PYARROW = True
except ModuleNotFoundError:
    HAS_PYARROW = False

HAS_PARQUET_ENGINE = HAS_PYSPARK or HAS_PYARROW


def assert_standard_parquet_row_count(root: Path, expected: int) -> None:
    """Count rows with Spark (or PyArrow), which fails on files without a valid Parquet footer."""
    if HAS_PYSPARK:
        spark = SparkSession.builder.getOrCreate()
        total = sum(
            spark.read.parquet(str(path)).count()
            for path in sorted(root.rglob("*.parquet"))
        )
        assert total == expected
    elif HAS_PYARROW:
        total = sum(
            pq.read_table(path).num_rows
            for path in sorted(root.rglob("*.parquet"))
        )
        assert total == expected


CONTRACT_A_FIELDS = (
    "machine_id",
    "timestamp",
    "temperature_c",
    "vibration_mm_s",
    "pressure_bar",
    "rpm",
    "power_kw",
    "operating_hours",
    "machine_state",
)

SPARK_COMPAT_FIELDS = (
    "machine_id",
    "timestamp",
    "temperature",
    "vibration",
    "pressure",
)


class GeneratorAndIngestionTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.temp_dir = Path(self.temporary_directory.name)

    def test_expected_event_counts_and_preset_formula(self):
        output_file = self.temp_dir / "batch.jsonl"
        config = GenerationConfig(
            machines=10,
            days=1,
            interval=15,
            seed=42,
            anomaly_rate=0.05,
            output=output_file,
        )
        summary = generate_telemetry(config)
        # 10 máquinas * (1440 min / 15 min) = 10 * 96 = 960 eventos
        self.assertEqual(summary.steps_per_machine, 96)
        self.assertEqual(summary.total_events, 960)
        self.assertEqual(summary.normal_events + summary.anomalous_events, 960)

        lines = output_file.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 960)

    def test_full_anomaly_rate_allows_all_anomalous_events(self):
        output_file = self.temp_dir / "all_anomalies.jsonl"
        summary = generate_telemetry(
            GenerationConfig(
                machines=2,
                days=1,
                interval=360,
                seed=42,
                anomaly_rate=1.0,
                output=output_file,
            )
        )
        self.assertEqual(summary.total_events, 8)
        self.assertEqual(summary.anomalous_events, 8)
        self.assertEqual(summary.normal_events, 0)

    def test_deterministic_seed_produces_identical_output(self):
        first_out = self.temp_dir / "run1.jsonl"
        second_out = self.temp_dir / "run2.jsonl"
        different_out = self.temp_dir / "run3.jsonl"

        generate_telemetry(
            GenerationConfig(
                machines=4,
                days=1,
                interval=60,
                seed=12345,
                anomaly_rate=0.10,
                output=first_out,
            )
        )
        generate_telemetry(
            GenerationConfig(
                machines=4,
                days=1,
                interval=60,
                seed=12345,
                anomaly_rate=0.10,
                output=second_out,
            )
        )
        generate_telemetry(
            GenerationConfig(
                machines=4,
                days=1,
                interval=60,
                seed=99999,
                anomaly_rate=0.10,
                output=different_out,
            )
        )

        content_1 = first_out.read_text(encoding="utf-8")
        content_2 = second_out.read_text(encoding="utf-8")
        content_3 = different_out.read_text(encoding="utf-8")

        self.assertEqual(content_1, content_2)
        self.assertNotEqual(content_1, content_3)

    def test_contract_a_types_catalog_and_anomalies_presence(self):
        output_file = self.temp_dir / "contract.jsonl"
        catalog = build_machine_catalog(5)
        valid_ids = {m.machine_id for m in catalog}

        summary = generate_telemetry(
            GenerationConfig(
                machines=5,
                days=1,
                interval=30,
                seed=42,
                anomaly_rate=0.12,
                output=output_file,
            )
        )

        self.assertGreater(summary.normal_events, 0)
        self.assertGreater(summary.anomalous_events, 0)
        self.assertEqual(len(summary.injected_anomalies), summary.anomalous_events)
        self.assertIn("sustained_degradation", summary.anomaly_breakdown)

        records = [
            json.loads(line)
            for line in output_file.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(records), summary.total_events)

        for row in records:
            for field in (*CONTRACT_A_FIELDS, *SPARK_COMPAT_FIELDS):
                self.assertIn(field, row)
                self.assertIsNotNone(row[field])

            self.assertIn(row["machine_id"], valid_ids)
            self.assertTrue(row["timestamp"].endswith("Z"))
            self.assertIsInstance(row["temperature_c"], float)
            self.assertIsInstance(row["vibration_mm_s"], float)
            self.assertIsInstance(row["pressure_bar"], float)
            self.assertIsInstance(row["rpm"], int)
            self.assertIsInstance(row["power_kw"], float)
            self.assertIsInstance(row["operating_hours"], float)
            self.assertIn(row["machine_state"], ("operating", "idle", "stopped"))
            self.assertEqual(row["temperature"], row["temperature_c"])
            self.assertEqual(row["vibration"], row["vibration_mm_s"])
            self.assertEqual(row["pressure"], row["pressure_bar"])

        normal_rows = [r for r in records if not r["is_anomaly"]]
        anomaly_rows = [r for r in records if r["is_anomaly"]]
        self.assertEqual(len(normal_rows), summary.normal_events)
        self.assertEqual(len(anomaly_rows), summary.anomalous_events)

    def test_cli_entry_point_and_sample_files(self):
        cli_output = self.temp_dir / "cli_sample.jsonl"
        exit_code = generator_main(
            [
                "--machines",
                "3",
                "--days",
                "1",
                "--interval",
                "120",
                "--seed",
                "7",
                "--anomaly_rate",
                "0.15",
                "--output",
                str(cli_output),
            ]
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue(cli_output.is_file())
        lines = cli_output.read_text(encoding="utf-8").splitlines()
        # 3 máquinas * 12 lecturas = 36 eventos
        self.assertEqual(len(lines), 36)

        self.assertTrue(DEFAULT_CATALOG_PATH.is_file())
        catalog = load_machine_catalog(DEFAULT_CATALOG_PATH)
        self.assertGreaterEqual(len(catalog), 10)
        self.assertEqual(catalog[0].machine_id, "MAQ-001")

        self.assertTrue(DEFAULT_OUTPUT_PATH.is_file())
        sample_lines = DEFAULT_OUTPUT_PATH.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(sample_lines), 120)

    def test_save_and_load_catalog_roundtrip(self):
        custom_catalog_path = self.temp_dir / "machines.json"
        save_machine_catalog(machines=12, output_path=custom_catalog_path)
        loaded = load_machine_catalog(custom_catalog_path)
        self.assertEqual(len(loaded), 12)
        self.assertEqual(loaded[-1].machine_id, "MAQ-012")

    def test_record_validation_milliseconds_and_invalid_fields(self):
        base = {
            "machine_id": "MAQ-001",
            "timestamp": "2026-10-25T12:00:00.125Z",
            "temperature": 65.5,
            "vibration": 2.1,
            "pressure": 5.0,
            "rpm": 1500,
            "power_kw": 20.0,
            "operating_hours": 100.0,
            "machine_state": "operating",
        }
        normalized, part_date = validate_and_normalize_record(json.dumps(base))
        self.assertEqual(part_date, "2026-10-25")
        self.assertEqual(normalized["timestamp"], "2026-10-25T12:00:00.125Z")

        for invalid_patch in (
            {"rpm": float("inf")},
            {"power_kw": float("nan")},
            {"operating_hours": float("-inf")},
            {"machine_state": None},
            {"machine_state": "broken"},
        ):
            payload = {**base, **invalid_patch}
            with self.assertRaises(ValueError):
                validate_and_normalize_record(json.dumps(payload))

    @unittest.skipUnless(HAS_PARQUET_ENGINE, "Requiere pyarrow o pyspark para escribir Parquet")
    def test_ingestion_raw_zone_parquet_partitions_and_idempotency(self):
        # Generar lote de 2 días para comprobar múltiples particiones date=YYYY-MM-DD
        batch_jsonl = self.temp_dir / "two_days.jsonl"
        lake_root = self.temp_dir / "datalake"
        generate_telemetry(
            GenerationConfig(
                machines=3,
                days=2,
                interval=180,
                seed=42,
                anomaly_rate=0.10,
                output=batch_jsonl,
                start_time="2026-10-25T00:00:00Z",
            )
        )

        # Primera corrida de ingesta
        first_result = ingest_telemetry_batch(batch_jsonl, lake_root=lake_root)
        # 3 máquinas * (2 días * 1440 / 180) = 3 * 16 = 48 eventos
        self.assertEqual(first_result.total_received, 48)
        self.assertEqual(first_result.valid_records, 48)
        self.assertEqual(first_result.invalid_records, 0)
        self.assertFalse(first_result.idempotent_replay)
        self.assertEqual(first_result.dates, ["2026-10-25", "2026-10-26"])
        self.assertEqual(len(first_result.raw_files), 2)
        self.assertEqual(len(first_result.parquet_files), 2)

        for part_date in ("2026-10-25", "2026-10-26"):
            raw_dir = lake_root / "raw" / f"date={part_date}"
            pq_dir = lake_root / "historical" / "telemetry" / f"date={part_date}"
            self.assertTrue(raw_dir.is_dir())
            self.assertTrue(pq_dir.is_dir())
            part_rows = read_parquet_partition(pq_dir)
            assert_standard_parquet_row_count(pq_dir, 24)
            self.assertEqual(len(part_rows), 24)
            self.assertTrue(all(r["date"] == part_date for r in part_rows))

        # Segunda corrida del mismo lote (idempotencia)
        second_result = ingest_telemetry_batch(batch_jsonl, lake_root=lake_root)
        self.assertTrue(second_result.idempotent_replay)
        all_parquet_rows = read_parquet_partition(
            lake_root / "historical" / "telemetry"
        )
        assert_standard_parquet_row_count(lake_root / "historical" / "telemetry", 48)
        self.assertEqual(len(all_parquet_rows), 48)

        manifest_lines = (
            first_result.manifest_path.read_text(encoding="utf-8").splitlines()
        )
        self.assertEqual(len(manifest_lines), 1)

    @unittest.skipUnless(HAS_PARQUET_ENGINE, "Requiere pyarrow o pyspark para escribir Parquet")
    def test_ingestion_cli_and_corrupt_record_handling(self):
        mixed_file = self.temp_dir / "mixed.jsonl"
        lake_root = self.temp_dir / "lake_cli"
        valid_line = DEFAULT_OUTPUT_PATH.read_text(encoding="utf-8").splitlines()[0]
        mixed_file.write_text(
            valid_line + "\n" + "{json_invalido\n", encoding="utf-8"
        )

        # Modo permisivo (por defecto): contabiliza inválidos y procesa válidos
        res = ingest_telemetry_batch(mixed_file, lake_root=lake_root, strict=False)
        self.assertEqual(res.total_received, 2)
        self.assertEqual(res.valid_records, 1)
        self.assertEqual(res.invalid_records, 1)

        # Modo estricto: lanza ValueError ante línea inválida
        with self.assertRaises(ValueError):
            ingest_telemetry_batch(mixed_file, lake_root=lake_root, strict=True)

        # CLI con la muestra oficial
        exit_code = ingest_main(
            [
                "--input",
                str(DEFAULT_OUTPUT_PATH),
                "--lake-root",
                str(lake_root / "official"),
            ]
        )
        self.assertEqual(exit_code, 0)

    @unittest.skipUnless(HAS_PYSPARK, "PySpark no está instalado en este intérprete")
    def test_pyspark_reads_generated_sample_cleanly(self):
        spark = create_spark_session()
        try:
            lines = DEFAULT_OUTPUT_PATH.read_text(encoding="utf-8").splitlines()
            df_strict = read_telemetry(spark, DEFAULT_OUTPUT_PATH)
            self.assertEqual(df_strict.count(), len(lines))

            quality_result = clean_telemetry(spark, DEFAULT_OUTPUT_PATH)
            self.assertEqual(quality_result.metrics.received, len(lines))
            self.assertEqual(quality_result.metrics.valid, len(lines))
            self.assertEqual(quality_result.metrics.rejected, 0)
            self.assertEqual(quality_result.metrics.duplicates, 0)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
