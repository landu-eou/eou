"""Build compressed SQLite distances between stations and stargates per system."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import sqlite3
import tempfile
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SYSTEMS_SOURCE = ROOT / "data" / "sde" / "mapSolarSystems.db"
STARGATES_SOURCE = ROOT / "data" / "sde" / "mapStargates.db"
STATIONS_SOURCE = ROOT / "data" / "sde" / "npcStations.db"
OUTPUT = ROOT / "data" / "derived" / "uni" / "stationgateRoutes.db"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_records(path: Path, label: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
        for ordinal, (document,) in enumerate(
            connection.execute("SELECT document FROM records ORDER BY ordinal"), start=1
        ):
            try:
                record = json.loads(document)
                if not isinstance(record, dict):
                    raise ValueError("el documento debe ser un objeto JSON")
                records.append(record)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"{label}, registro ordinal={ordinal}: {exc}") from exc
    if not records:
        raise ValueError(f"{path.name} no contiene registros")
    return records


def positive_id(record: dict[str, Any], field: str, label: str) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label}: {field} debe ser un entero positivo")
    return value


def position(record: dict[str, Any], label: str) -> tuple[float, float, float]:
    coordinates = record.get("position")
    if not isinstance(coordinates, dict):
        raise ValueError(f"{label}: falta position")
    values = (coordinates.get("x"), coordinates.get("y"), coordinates.get("z"))
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in values):
        raise ValueError(f"{label}: position debe incluir x, y, z numéricos")
    result = tuple(float(value) for value in values)
    if not all(math.isfinite(value) for value in result):
        raise ValueError(f"{label}: position incluye coordenadas no finitas")
    return result


def read_stored_hashes(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        with tempfile.TemporaryDirectory(prefix="stationgate-routes-check-") as temp_dir:
            database = Path(temp_dir) / "stationgateRoutes.sqlite"
            decompressor = zlib.decompressobj()
            with path.open("rb") as source, database.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    output.write(decompressor.decompress(chunk))
                output.write(decompressor.flush())
            with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
                rows = connection.execute(
                    "SELECT key, value FROM metadata WHERE key IN "
                    "('mapSolarSystems_sha256', 'mapStargates_sha256', 'npcStations_sha256')"
                )
                return {str(key): str(value) for key, value in rows}
    except (OSError, sqlite3.Error, zlib.error):
        return {}


def collect_points(
    source: Path,
    label: str,
    known_systems: set[int],
    points_by_system: dict[int, list[tuple[int, tuple[float, float, float]]]],
    all_point_ids: set[int],
) -> int:
    count = 0
    for ordinal, record in enumerate(read_records(source, label), start=1):
        record_label = f"{label} ordinal={ordinal}"
        point_id = positive_id(record, "_key", record_label)
        system_id = positive_id(record, "solarSystemID", f"{record_label} id={point_id}")
        if system_id not in known_systems:
            raise ValueError(f"{label} id={point_id}: sistema inexistente {system_id}")
        if point_id in all_point_ids:
            raise ValueError(f"ID de punto duplicado entre estaciones y stargates: {point_id}")
        all_point_ids.add(point_id)
        points_by_system[system_id].append(
            (point_id, position(record, f"{label} id={point_id}"))
        )
        count += 1
    return count


def create_database(
    path: Path,
    points_by_system: dict[int, list[tuple[int, tuple[float, float, float]]]],
    source_hashes: dict[str, str],
) -> tuple[int, int]:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID"
        )
        connection.execute(
            """CREATE TABLE distances (
                origin_id INTEGER NOT NULL,
                destination_id INTEGER NOT NULL,
                distance_m INTEGER NOT NULL CHECK(distance_m >= 0),
                PRIMARY KEY (origin_id, destination_id),
                CHECK (origin_id < destination_id)
            ) WITHOUT ROWID"""
        )
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            sorted(source_hashes.items()),
        )

        batch: list[tuple[int, int, int]] = []
        pair_count = 0
        for system_id in sorted(points_by_system):
            points = sorted(points_by_system[system_id], key=lambda item: item[0])
            for (origin_id, origin_position), (destination_id, destination_position) in itertools.combinations(points, 2):
                # Positions from stations and gates in a system share that system's local frame.
                distance_m = int(math.dist(origin_position, destination_position))
                batch.append((origin_id, destination_id, distance_m))
                pair_count += 1
                if len(batch) >= 50_000:
                    connection.executemany("INSERT INTO distances VALUES (?, ?, ?)", batch)
                    batch.clear()
        if batch:
            connection.executemany("INSERT INTO distances VALUES (?, ?, ?)", batch)

        connection.commit()
        connection.execute("VACUUM")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise RuntimeError(f"SQLite integrity_check falló: {integrity!r}")
        stored_pairs = int(connection.execute("SELECT COUNT(*) FROM distances").fetchone()[0])
        return len(points_by_system), stored_pairs
    finally:
        connection.close()


def compress_zlib(source: Path, destination: Path) -> None:
    compressor = zlib.compressobj(level=9)
    with source.open("rb") as input_file, destination.open("wb") as output_file:
        while chunk := input_file.read(1024 * 1024):
            output_file.write(compressor.compress(chunk))
        output_file.write(compressor.flush())
        output_file.flush()
        os.fsync(output_file.fileno())


def main(force: bool = False) -> int:
    sources = (SYSTEMS_SOURCE, STARGATES_SOURCE, STATIONS_SOURCE)
    for source in sources:
        if not source.is_file():
            raise FileNotFoundError(f"No existe la fuente SDE: {source}")

    source_hashes = {
        "mapSolarSystems_sha256": sha256_file(SYSTEMS_SOURCE),
        "mapStargates_sha256": sha256_file(STARGATES_SOURCE),
        "npcStations_sha256": sha256_file(STATIONS_SOURCE),
    }
    if not force and read_stored_hashes(OUTPUT) == source_hashes:
        print(
            "Sin cambios: coinciden los SHA-256 de mapSolarSystems.db, "
            "mapStargates.db y npcStations.db; no se recalculan las distancias. "
            "Usa --force para reconstruirlas.",
            flush=True,
        )
        return 0
    if force:
        print(
            "Modo force activo: se reconstruye stationgateRoutes.db aunque los SHA coincidan.",
            flush=True,
        )

    known_systems = {
        positive_id(record, "_key", f"mapSolarSystems ordinal={ordinal}")
        for ordinal, record in enumerate(
            read_records(SYSTEMS_SOURCE, "mapSolarSystems"), start=1
        )
    }
    points_by_system: dict[int, list[tuple[int, tuple[float, float, float]]]] = defaultdict(list)
    all_point_ids: set[int] = set()
    stargate_count = collect_points(
        STARGATES_SOURCE,
        "mapStargates",
        known_systems,
        points_by_system,
        all_point_ids,
    )
    station_count = collect_points(
        STATIONS_SOURCE,
        "npcStations",
        known_systems,
        points_by_system,
        all_point_ids,
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stationgate-routes-build-", dir=OUTPUT.parent) as temp_dir:
        plain_database = Path(temp_dir) / "stationgateRoutes.sqlite"
        compressed_database = Path(temp_dir) / "stationgateRoutes.zlib"
        system_count, pair_count = create_database(
            plain_database, points_by_system, source_hashes
        )
        compress_zlib(plain_database, compressed_database)
        os.replace(compressed_database, OUTPUT)

    print(
        f"SQLite creada: {system_count:,} sistemas con puntos; {stargate_count:,} stargates; "
        f"{station_count:,} estaciones; {pair_count:,} parejas locales sin duplicar; "
        f"{OUTPUT.stat().st_size:,} bytes zlib; "
        f"mapSolarSystems_sha256={source_hashes['mapSolarSystems_sha256']}; "
        f"mapStargates_sha256={source_hashes['mapStargates_sha256']}; "
        f"npcStations_sha256={source_hashes['npcStations_sha256']}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="reconstruye la base aunque los SHA de las fuentes coincidan",
    )
    try:
        raise SystemExit(main(force=parser.parse_args().force))
    except (OSError, sqlite3.Error, ValueError, RuntimeError, zlib.error) as exc:
        print(f"Error construyendo stationgateRoutes.db: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
