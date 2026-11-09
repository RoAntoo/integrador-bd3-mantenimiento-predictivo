from pathlib import Path
from src.storage.load_results import (
    get_pg_connection,
    get_mongo_client,
    upsert_postgres,
    upsert_mongodb,
)


def test_idempotent_storage_load():
    """Verifica que correr la carga dos veces seguidas no duplique filas en PG ni en Mongo."""
    base_sample = Path("data/sample")
    metrics_csv = base_sample / "sensor_metrics_5m_sample.csv"
    risk_csv = base_sample / "machine_risk_latest_sample.csv"
    alerts_jsonl = base_sample / "alerts_sample.jsonl"

    # Primera y segunda ejecucion con el mismo lote
    upsert_postgres(metrics_csv, risk_csv)
    upsert_postgres(metrics_csv, risk_csv)

    upsert_mongodb(alerts_jsonl)
    upsert_mongodb(alerts_jsonl)

    # Verificar conteos en PostgreSQL
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM machines;")
            assert cur.fetchone()[0] >= 3

            cur.execute("SELECT COUNT(*) FROM machine_risk_latest WHERE machine_id IN ('MAQ-001', 'MAQ-002', 'MAQ-003');")
            assert cur.fetchone()[0] == 3

            cur.execute("SELECT COUNT(*) FROM sensor_metrics_5m WHERE machine_id = 'MAQ-001' AND window_start = '2026-10-31T10:00:00Z';")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()

    # Verificar conteos en MongoDB
    client = get_mongo_client()
    try:
        col = client["mantenimiento_predictivo"]["alerts"]
        count_alert = col.count_documents({"machine_id": "MAQ-001", "timestamp": "2026-10-31T10:10:00Z"})
        assert count_alert == 1
    finally:
        client.close()