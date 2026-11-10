import os
import json
import math
from numbers import Integral
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()


def required_env(name: str) -> str:
    """Leer una variable obligatoria del entorno."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta configurar {name} en el entorno o en .env.")
    return value


def get_pg_connection():
    """Abrir una conexión a PostgreSQL con la configuración local."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "admin_bd3"),
        password=required_env("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB", "mantenimiento_predictivo"),
        connect_timeout=5,
    )


def get_mongo_client():
    """Crear un cliente autenticado de MongoDB con la configuración local."""
    return MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin_mongo"),
        password=required_env("MONGO_INITDB_ROOT_PASSWORD"),
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )


def load_dataframe(file_path: Path) -> pd.DataFrame:
    """Lee tanto Parquet (salida de PySpark) como CSV (fixtures de prueba)."""
    if file_path.suffix == ".parquet" or file_path.is_dir():
        return pd.read_parquet(file_path)
    return pd.read_csv(file_path, dtype={"machine_id": str})


def utc_datetime(value):
    """Normalizar un timestamp de consumo a un datetime UTC para PostgreSQL."""
    timestamp = pd.to_datetime(value, utc=True)
    if pd.isna(timestamp):
        raise ValueError("Los timestamps de consumo no pueden estar vacíos.")
    return timestamp.to_pydatetime()


def upsert_postgres(metrics_path: Path, risk_path: Path, *, batch_id: str | None = None):
    """Cargar las salidas juntas y reservar sus ventanas si se identifica el lote."""
    df_metrics = load_dataframe(metrics_path)
    df_risk = load_dataframe(risk_path)

    # Extraer todas las maquinas unicas para asegurar que existan en el catalogo
    machine_ids = set(df_metrics["machine_id"]).union(df_risk["machine_id"])
    if any(not isinstance(machine_id, str) or not machine_id.strip() or len(machine_id) > 50
           for machine_id in machine_ids):
        raise ValueError("machine_id debe ser texto de 1 a 50 caracteres.")
    machines = sorted(machine_ids)

    metrics_rows = []
    for row in df_metrics.itertuples(index=False):
        measurements = (float(row.avg_temp), float(row.max_vibration), float(row.avg_pressure))
        if not all(math.isfinite(value) for value in measurements):
            raise ValueError("Las métricas agregadas deben ser números finitos.")
        if not isinstance(row.n_readings, Integral) or not 0 <= row.n_readings <= 2_147_483_647:
            raise ValueError("n_readings debe ser un entero no negativo compatible con PostgreSQL.")
        metrics_rows.append((row.machine_id, utc_datetime(row.window_start), *measurements,
                             int(row.n_readings), batch_id))
    metrics_rows.sort(key=lambda row: row[:2])
    risk_rows = []
    for row in df_risk.itertuples(index=False):
        if not isinstance(row.risk_reason, str) or not row.risk_reason.strip():
            raise ValueError("risk_reason debe contener el motivo de la clasificación.")
        risk_rows.append((row.machine_id, utc_datetime(row.calculated_at), str(row.risk_level).upper(),
                          row.risk_reason, utc_datetime(row.window_start) if batch_id is not None else None))
    risk_rows.sort(key=lambda row: row[0])

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

                accepted = execute_values(
                    cur,
                    """
                    INSERT INTO sensor_metrics_5m AS current
                        (machine_id, window_start, avg_temp, max_vibration, avg_pressure, n_readings, batch_id)
                    VALUES %s
                    ON CONFLICT (machine_id, window_start) DO UPDATE SET
                        avg_temp = EXCLUDED.avg_temp,
                        max_vibration = EXCLUDED.max_vibration,
                        avg_pressure = EXCLUDED.avg_pressure,
                        n_readings = EXCLUDED.n_readings
                    WHERE current.batch_id IS NOT DISTINCT FROM EXCLUDED.batch_id
                    RETURNING machine_id, window_start;
                    """,
                    metrics_rows,
                    fetch=True,
                )
                if len(accepted) != len(metrics_rows):
                    conflicts = {row[:2] for row in metrics_rows} - set(accepted)
                    machine, window = min(conflicts)
                    raise ValueError(f"Ventana de otro lote o sin propietario: {machine} / {window.isoformat()}.")

                execute_values(
                    cur,
                    """
                    INSERT INTO machine_risk_latest AS current
                        (machine_id, calculated_at, risk_level, risk_reason, window_start)
                    VALUES %s
                    ON CONFLICT (machine_id) DO UPDATE SET
                        calculated_at = EXCLUDED.calculated_at,
                        risk_level = EXCLUDED.risk_level,
                        risk_reason = EXCLUDED.risk_reason,
                        window_start = EXCLUDED.window_start
                    WHERE EXCLUDED.window_start > current.window_start
                        OR (EXCLUDED.window_start IS NULL AND current.window_start IS NULL);
                    """,
                    risk_rows,
                )
                if batch_id is not None and risk_rows:
                    cur.execute(
                        "SELECT machine_id FROM machine_risk_latest "
                        "WHERE window_start IS NULL AND machine_id = ANY(%s) LIMIT 1",
                        ([row[0] for row in risk_rows],),
                    )
                    legacy = cur.fetchone()
                    if legacy:
                        raise ValueError(f"El riesgo previo de {legacy[0]} no tiene ventana de origen verificable.")
        print(f"[PostgreSQL] Carga exitosa: {len(metrics_rows)} metricas y {len(risk_rows)} estados de riesgo.")
    finally:
        conn.close()


def upsert_mongodb(alerts_path: Path):
    """Cargar alertas JSONL con una clave única por máquina e instante."""
    client = get_mongo_client()
    try:
        db = client[os.getenv("MONGO_DB", "mantenimiento_predictivo")]
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
