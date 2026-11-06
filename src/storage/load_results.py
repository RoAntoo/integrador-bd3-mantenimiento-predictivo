import os
import json
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "admin_bd3"),
        password=os.getenv("POSTGRES_PASSWORD", "admin_bd3_pass"),
        dbname=os.getenv("POSTGRES_DB", "mantenimiento_predictivo"),
    )


def get_mongo_client():
    user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin_mongo")
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin_mongo_pass")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin"
    return MongoClient(uri)


def load_dataframe(file_path: Path) -> pd.DataFrame:
    """Lee tanto Parquet (salida de PySpark) como CSV (fixtures de prueba)."""
    if file_path.suffix == ".parquet" or file_path.is_dir():
        return pd.read_parquet(file_path)
    return pd.read_csv(file_path)


def upsert_postgres(metrics_path: Path, risk_path: Path):
    df_metrics = load_dataframe(metrics_path)
    df_risk = load_dataframe(risk_path)

    # Extraer todas las maquinas unicas para asegurar que existan en el catalogo
    machines = sorted(
        set(df_metrics["machine_id"].unique()).union(set(df_risk["machine_id"].unique()))
    )

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. UPSERT catalogo de maquinas
                machine_rows = [(m_id, f"Equipo {m_id}", "Planta Principal") for m_id in machines]
                execute_values(
                    cur,
                    """
                    INSERT INTO machines (machine_id, machine_name, location)
                    VALUES %s
                    ON CONFLICT (machine_id) DO NOTHING;
                    """,
                    machine_rows,
                )

                # 2. UPSERT sensor_metrics_5m (Idempotente por machine_id + window_start)
                metrics_rows = [
                    (
                        row.machine_id,
                        row.window_start,
                        float(row.avg_temp),
                        float(row.max_vibration),
                        float(row.avg_pressure),
                        int(row.n_readings),
                    )
                    for row in df_metrics.itertuples(index=False)
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO sensor_metrics_5m 
                        (machine_id, window_start, avg_temp, max_vibration, avg_pressure, n_readings)
                    VALUES %s
                    ON CONFLICT (machine_id, window_start) DO UPDATE SET
                        avg_temp = EXCLUDED.avg_temp,
                        max_vibration = EXCLUDED.max_vibration,
                        avg_pressure = EXCLUDED.avg_pressure,
                        n_readings = EXCLUDED.n_readings;
                    """,
                    metrics_rows,
                )

                # 3. UPSERT machine_risk_latest (Idempotente por machine_id)
                risk_rows = [
                    (
                        row.machine_id,
                        row.calculated_at,
                        str(row.risk_level).upper(),
                        str(row.risk_reason),
                    )
                    for row in df_risk.itertuples(index=False)
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO machine_risk_latest 
                        (machine_id, calculated_at, risk_level, risk_reason)
                    VALUES %s
                    ON CONFLICT (machine_id) DO UPDATE SET
                        calculated_at = EXCLUDED.calculated_at,
                        risk_level = EXCLUDED.risk_level,
                        risk_reason = EXCLUDED.risk_reason;
                    """,
                    risk_rows,
                )
        print(f"[PostgreSQL] Carga exitosa: {len(metrics_rows)} metricas y {len(risk_rows)} estados de riesgo.")
    finally:
        conn.close()


def upsert_mongodb(alerts_path: Path):
    client = get_mongo_client()
    try:
        db = client["mantenimiento_predictivo"]
        alerts_col = db["alerts"]
        # Indice unico para garantizar idempotencia en MongoDB
        alerts_col.create_index([("machine_id", 1), ("timestamp", 1)], unique=True)

        operations = []
        with open(alerts_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    operations.append(
                        UpdateOne(
                            {"machine_id": doc["machine_id"], "timestamp": doc["timestamp"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )

        if operations:
            result = alerts_col.bulk_write(operations)
            print(
                f"[MongoDB] Alertas procesadas: {len(operations)} "
                f"(Nuevas: {result.upserted_count}, Actualizadas: {result.modified_count})."
            )
    finally:
        client.close()


if __name__ == "__main__":
    base_sample = Path("data/sample")
    upsert_postgres(
        base_sample / "sensor_metrics_5m_sample.csv",
        base_sample / "machine_risk_latest_sample.csv",
    )
    upsert_mongodb(base_sample / "alerts_sample.jsonl")