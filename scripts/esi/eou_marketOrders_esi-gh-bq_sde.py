#!/usr/bin/env python3
"""
Lectura de inputs SDE / ESI cacheados del repositorio.

Este archivo:
- carga regiones desde data/sde/regions.jsonl.gz
- carga estructuras con mercado desde data/esi/structures.jsonl.gz
- carga estaciones desde data/sde/stations.jsonl.gz
- detecta de forma robusta un archivo de tipos reutilizable para fases futuras

Aunque este workflow no calcula precios todavía, conservamos el detector de tipos
para mantener una base reutilizable y compatible con la filosofía del workflow actual.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Dict, Iterator, List, Optional


def iter_jsonl_gz(path: str | Path) -> Iterator[dict]:
    """
    Itera un .jsonl.gz línea a línea como objetos JSON.

    Las líneas vacías se ignoran.
    """
    p = Path(path)
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _as_int(value) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def load_regions(path: str | Path) -> List[dict]:
    """
    Devuelve regiones como:
      [{"kind": "region", "entity_id": ..., "name": ...}, ...]
    """
    out: List[dict] = []
    for row in iter_jsonl_gz(path):
        region_id = (
            _as_int(row.get("regionID"))
            or _as_int(row.get("region_id"))
            or _as_int(row.get("id"))
        )
        name = (
            row.get("region")
            or row.get("regionName")
            or row.get("name")
            or f"region_{region_id}"
        )
        if region_id is None:
            continue
        out.append({
            "kind": "region",
            "entity_id": region_id,
            "name": str(name),
        })
    out.sort(key=lambda x: x["entity_id"])
    return out


def load_stations(path: str | Path) -> List[dict]:
    """
    Carga estaciones SDE para tener compatibilidad con la filosofía del workflow base,
    aunque en esta fase no sean imprescindibles para la ingesta pura.
    """
    out: List[dict] = []
    p = Path(path)
    if not p.exists():
        return out

    for row in iter_jsonl_gz(p):
        station_id = (
            _as_int(row.get("stationID"))
            or _as_int(row.get("station_id"))
            or _as_int(row.get("id"))
        )
        name = row.get("station") or row.get("stationName") or row.get("name")
        if station_id is None:
            continue
        out.append({
            "station_id": station_id,
            "name": str(name or f"station_{station_id}"),
        })
    out.sort(key=lambda x: x["station_id"])
    return out


def load_structures(path: str | Path) -> List[dict]:
    """
    Devuelve estructuras con mercado como:
      [{"kind": "structure", "entity_id": ..., "name": ...}, ...]
    """
    out: List[dict] = []
    p = Path(path)
    if not p.exists():
        return out

    for row in iter_jsonl_gz(p):
        structure_id = (
            _as_int(row.get("stationID"))
            or _as_int(row.get("structure_id"))
            or _as_int(row.get("structureID"))
            or _as_int(row.get("id"))
        )
        name = (
            row.get("station")
            or row.get("structure")
            or row.get("name")
            or f"structure_{structure_id}"
        )
        if structure_id is None:
            continue
        out.append({
            "kind": "structure",
            "entity_id": structure_id,
            "name": str(name),
        })
    out.sort(key=lambda x: x["entity_id"])
    return out


def detect_types_file(sde_dir: str | Path) -> Optional[str]:
    """
    Detector robusto de tipos reutilizable para fases futuras.

    Prioridad:
      1) nombres candidatos conocidos
      2) cualquier .jsonl.gz en data/sde con claves tipo (typeID/type)

    Devuelve la ruta elegida o None si no encuentra ninguna.
    """
    sde_path = Path(sde_dir)
    candidates = [
        "types.jsonl.gz",
        "typeIDs.jsonl.gz",
        "items.jsonl.gz",
        "types_min.jsonl.gz",
    ]
    for name in candidates:
        p = sde_path / name
        if p.exists():
            return str(p)

    for p in sorted(sde_path.glob("*.jsonl.gz")):
        try:
            for row in iter_jsonl_gz(p):
                keys = set(row.keys())
                if (
                    ("typeID" in keys and "type" in keys)
                    or ("type_id" in keys and "type" in keys)
                    or ("typeID" in keys and "name" in keys)
                ):
                    return str(p)
                break
        except Exception:
            continue
    return None
