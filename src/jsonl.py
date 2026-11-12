"""Lectura de líneas físicas y hashes de archivos locales JSONL."""

import hashlib
from pathlib import Path


def read_jsonl_lines(path: Path) -> list[str]:
    """Conservar contenido sin terminador; no dividir por separadores Unicode."""
    with path.open(encoding="utf-8", newline="") as handle:
        return [line.removesuffix("\n").removesuffix("\r") for line in handle]


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()
