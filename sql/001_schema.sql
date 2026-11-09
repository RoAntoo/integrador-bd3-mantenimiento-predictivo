-- 1. Catálogo de máquinas
CREATE TABLE IF NOT EXISTS machines (
    machine_id VARCHAR(50) PRIMARY KEY,
    machine_name VARCHAR(100),
    location VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Métricas agregadas en ventanas de 5 minutos (Contrato B)
CREATE TABLE IF NOT EXISTS sensor_metrics_5m (
    machine_id VARCHAR(50) NOT NULL REFERENCES machines(machine_id) ON DELETE CASCADE,
    window_start TIMESTAMPTZ NOT NULL,
    avg_temp NUMERIC(8, 2) NOT NULL,
    max_vibration NUMERIC(8, 2) NOT NULL,
    avg_pressure NUMERIC(8, 2) NOT NULL,
    n_readings INTEGER NOT NULL CHECK (n_readings >= 0),
    PRIMARY KEY (machine_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_5m_window_machine 
    ON sensor_metrics_5m (window_start DESC, machine_id);

-- 3. Último estado de riesgo calculado por máquina (Contrato B)
CREATE TABLE IF NOT EXISTS machine_risk_latest (
    machine_id VARCHAR(50) PRIMARY KEY REFERENCES machines(machine_id) ON DELETE CASCADE,
    calculated_at TIMESTAMPTZ NOT NULL,
    risk_level VARCHAR(15) NOT NULL CHECK (risk_level IN ('VERDE', 'AMARILLO', 'ROJO')),
    risk_reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_latest_level 
    ON machine_risk_latest (risk_level, calculated_at DESC);