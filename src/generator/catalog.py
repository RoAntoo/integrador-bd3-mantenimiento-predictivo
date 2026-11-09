"""Catálogo compartido de máquinas industriales sintéticas (LEO-02)."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path


DEFAULT_CATALOG_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "sample" / "machines_catalog.json"
)


@dataclass(frozen=True)
class MachineProfile:
    """Perfil operativo nominal de una máquina del catálogo sintético."""

    machine_id: str
    name: str
    machine_type: str
    sector: str
    base_temperature_c: float
    base_vibration_mm_s: float
    base_pressure_bar: float
    base_rpm: int
    base_power_kw: float
    initial_operating_hours: float


_MACHINE_TEMPLATES = (
    {
        "name_prefix": "Compresor Tornillo",
        "machine_type": "compressor",
        "sector": "Sector A - Compresión",
        "base_temperature_c": 66.0,
        "base_vibration_mm_s": 2.1,
        "base_pressure_bar": 5.2,
        "base_rpm": 1480,
        "base_power_kw": 22.5,
        "initial_operating_hours": 1240.0,
    },
    {
        "name_prefix": "Bomba Centrífuga",
        "machine_type": "pump",
        "sector": "Sector B - Hidráulica",
        "base_temperature_c": 70.0,
        "base_vibration_mm_s": 2.6,
        "base_pressure_bar": 5.5,
        "base_rpm": 1750,
        "base_power_kw": 18.0,
        "initial_operating_hours": 2150.5,
    },
    {
        "name_prefix": "Turbina de Extracción",
        "machine_type": "turbine",
        "sector": "Sector C - Energía",
        "base_temperature_c": 74.5,
        "base_vibration_mm_s": 2.9,
        "base_pressure_bar": 6.0,
        "base_rpm": 2850,
        "base_power_kw": 42.0,
        "initial_operating_hours": 3420.0,
    },
    {
        "name_prefix": "Motor Trifásico Principal",
        "machine_type": "motor",
        "sector": "Sector D - Tracción",
        "base_temperature_c": 64.0,
        "base_vibration_mm_s": 1.9,
        "base_pressure_bar": 4.8,
        "base_rpm": 1450,
        "base_power_kw": 15.5,
        "initial_operating_hours": 980.0,
    },
    {
        "name_prefix": "Prensa Neumática",
        "machine_type": "press",
        "sector": "Sector E - Estampado",
        "base_temperature_c": 68.5,
        "base_vibration_mm_s": 2.4,
        "base_pressure_bar": 5.8,
        "base_rpm": 1600,
        "base_power_kw": 27.0,
        "initial_operating_hours": 1630.0,
    },
)


def build_machine_catalog(machines: int = 10) -> list[MachineProfile]:
    """Construir de forma determinista el catálogo para N máquinas (MAQ-001..MAQ-NNN)."""
    if machines <= 0:
        raise ValueError("La cantidad de máquinas debe ser un entero positivo.")

    catalog: list[MachineProfile] = []
    for index in range(1, machines + 1):
        template = _MACHINE_TEMPLATES[(index - 1) % len(_MACHINE_TEMPLATES)]
        series_number = ((index - 1) // len(_MACHINE_TEMPLATES)) + 1
        offset = ((index - 1) % 7) * 0.35

        catalog.append(
            MachineProfile(
                machine_id=f"MAQ-{index:03d}",
                name=f"{template['name_prefix']} #{series_number}",
                machine_type=str(template["machine_type"]),
                sector=str(template["sector"]),
                base_temperature_c=round(float(template["base_temperature_c"]) + offset, 2),
                base_vibration_mm_s=round(
                    float(template["base_vibration_mm_s"]) + (offset * 0.15), 2
                ),
                base_pressure_bar=round(
                    float(template["base_pressure_bar"]) + (offset * 0.1), 2
                ),
                base_rpm=int(template["base_rpm"]) + ((index - 1) % 5) * 15,
                base_power_kw=round(float(template["base_power_kw"]) + offset * 0.8, 2),
                initial_operating_hours=round(
                    float(template["initial_operating_hours"]) + (index - 1) * 45.0, 2
                ),
            )
        )
    return catalog


def save_machine_catalog(
    machines: int = 10, output_path: str | Path = DEFAULT_CATALOG_PATH
) -> Path:
    """Guardar el catálogo compartido de máquinas en formato JSON."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    catalog_data = [asdict(profile) for profile in build_machine_catalog(machines)]
    target.write_text(
        json.dumps(catalog_data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return target


def load_machine_catalog(
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
) -> list[MachineProfile]:
    """Cargar el catálogo compartido desde un archivo JSON."""
    path = Path(catalog_path)
    raw_items = json.loads(path.read_text(encoding="utf-8"))
    return [MachineProfile(**item) for item in raw_items]

