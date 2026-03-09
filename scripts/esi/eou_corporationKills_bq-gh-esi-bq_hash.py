from __future__ import annotations

"""
Normalización y hashing del dataset final.

El hash se calcula sobre filas canónicas ordenadas por corporation_id para que
el resultado sea estable y comparable entre ejecuciones.
"""

import hashlib
import json
from typing import Iterable, Mapping


def canonical_rows(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    """
    Convierte las filas a un formato homogéneo y ordenado.
    """
    normalized = [
        {
            "corporation_id": int(row["corporation_id"]),
            "corporation": str(row["corporation"]),
        }
        for row in rows
    ]
    normalized.sort(key=lambda item: item["corporation_id"])
    return normalized


def compute_hash(rows: Iterable[Mapping[str, object]]) -> tuple[str, list[dict[str, object]]]:
    """
    Devuelve:
    - el hash SHA-256 del contenido final,
    - las filas ya canonicalizadas.
    """
    normalized = canonical_rows(rows)
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest, normalized
