"""Build a compressed SQLite graph of SDE stargate links and their 3D distances."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import tempfile
import zlib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STARGATES_SOURCE = ROOT / "data" / "sde" / "mapStargates.db"
SYSTEMS_SOURCE = ROOT / "data" / "sde" / "mapSolarSystems.db"
OUTPUT = ROOT / "data" / "derived" / "uni" / "stargateRoutes.db"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_records(path: Path, label: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
        rows = connection.execute("SELECT document FROM records ORDER BY ordinal")
        for ordinal, (document,) in enumerate(rows, start=1):
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


def integer_field(record: dict[str, Any], key: str, label: str) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label}: {key} debe ser un entero positivo")
    return value


def coordinates(record: dict[str, Any], label: str) -> tuple[float, float, float]:
    position = record.get("position")
    if not isinstance(position, dict):
        raise ValueError(f"{label}: falta position")
    values = (position.get("x"), position.get("y"), position.get("z"))
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
        with tempfile.TemporaryDirectory(prefix="stargate-routes-check-") as temp_dir:
            database = Path(temp_dir) / "stargateRoutes.db"
            decompressor = zlib.decompressobj()
            with path.open("rb") as source, database.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    output.write(decompressor.decompress(chunk))
                output.write(decompressor.flush())
            with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
                rows = connection.execute(
                    "SELECT key, value FROM metadata WHERE key IN "
                    "('mapStargates_sha256', 'mapSolarSystems_sha256')"
                )
                return {str(key): str(value) for key, value in rows}
    except (OSError, sqlite3.Error, zlib.error):
        return {}


def create_database(
    path: Path,
    stargates: dict[int, tuple[int, int, int, tuple[float, float, float]]],
    source_hashes: dict[str, str],
) -> tuple[int, int, int]:
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
            "CREATE TABLE origins (stargate_id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        connection.execute(
            """CREATE TABLE destinations (
                origin_stargate_id INTEGER NOT NULL,
                stargate_id INTEGER NOT NULL,
                distance_m INTEGER NOT NULL CHECK(distance_m >= 0),
                PRIMARY KEY (origin_stargate_id, stargate_id)
            ) WITHOUT ROWID"""
        )
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            sorted(source_hashes.items()),
        )
        connection.executemany(
            "INSERT INTO origins(stargate_id) VALUES (?)",
            ((stargate_id,) for stargate_id in sorted(stargates)),
        )

        destinations: list[tuple[int, int, int]] = []
        for origin_id, (_, destination_id, _, origin_position) in stargates.items():
            destination = stargates.get(destination_id)
            if destination is None:
                raise ValueError(
                    f"La stargate {origin_id} apunta a destino inexistente {destination_id}"
                )
            destination_position = destination[3]
            distance_m = int(math.dist(origin_position, destination_position))
            destinations.append((origin_id, destination_id, distance_m))
        connection.executemany(
            "INSERT INTO destinations VALUES (?, ?, ?)", destinations
        )
        connection.commit()
        connection.execute("VACUUM")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise RuntimeError(f"SQLite integrity_check falló: {integrity!r}")
        bidirectional_pairs = int(
            connection.execute(
                """SELECT COUNT(*) FROM destinations a
                   JOIN destinations b
                     ON b.origin_stargate_id = a.stargate_id
                    AND b.stargate_id = a.origin_stargate_id
                    AND b.distance_m = a.distance_m"""
            ).fetchone()[0]
        )
        destination_count = int(
            connection.execute("SELECT COUNT(*) FROM destinations").fetchone()[0]
        )
        return len(stargates), destination_count, bidirectional_pairs
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


def main() -> int:
    for source in (STARGATES_SOURCE, SYSTEMS_SOURCE):
        if not source.is_file():
            raise FileNotFoundError(f"No existe la fuente SDE: {source}")

    source_hashes = {
        "mapStargates_sha256": sha256_file(STARGATES_SOURCE),
        "mapSolarSystems_sha256": sha256_file(SYSTEMS_SOURCE),
    }
    if read_stored_hashes(OUTPUT) == source_hashes:
        print(
            "Sin cambios: los SHA-256 de mapStargates.db y mapSolarSystems.db "
            "coinciden; no se recalculan las distancias.",
            flush=True,
        )
        return 0

    systems: dict[int, tuple[float, float, float]] = {}
    for ordinal, record in enumerate(read_records(SYSTEMS_SOURCE, "mapSolarSystems"), start=1):
        system_id = integer_field(record, "_key", f"mapSolarSystems ordinal={ordinal}")
        if system_id in systems:
            raise ValueError(f"mapSolarSystems: system_id duplicado {system_id}")
        systems[system_id] = coordinates(record, f"mapSolarSystems system_id={system_id}")

    gates: dict[int, tuple[int, int, int, tuple[float, float, float]]] = {}
    for ordinal, record in enumerate(read_records(STARGATES_SOURCE, "mapStargates"), start=1):
        label = f"mapStargates ordinal={ordinal}"
        gate_id = integer_field(record, "_key", label)
        system_id = integer_field(record, "solarSystemID", f"{label} stargate_id={gate_id}")
        destination = record.get("destination")
        if not isinstance(destination, dict):
            raise ValueError(f"{label} stargate_id={gate_id}: falta destination")
        destination_id = integer_field(
            destination, "stargateID", f"{label} stargate_id={gate_id} destination"
        )
        destination_system_id = integer_field(
            destination,
            "solarSystemID",
            f"{label} stargate_id={gate_id} destination",
        )
        if gate_id in gates:
            raise ValueError(f"mapStargates: stargate_id duplicado {gate_id}")
        if system_id not in systems:
            raise ValueError(f"Stargate {gate_id}: sistema origen inexistente {system_id}")
        if destination_system_id not in systems:
            raise ValueError(
                f"Stargate {gate_id}: sistema destino inexistente {destination_system_id}"
            )

        local_x, local_y, local_z = coordinates(record, f"mapStargates stargate_id={gate_id}")
        system_x, system_y, system_z = systems[system_id]
        # CCP uses the opposite X direction for celestial coordinates when combining
        # a system position with an object position inside that system.
        global_position = (
            system_x - local_x,
            system_y + local_y,
            system_z + local_z,
        )
        gates[gate_id] = (
            system_id,
            destination_id,
            destination_system_id,
            global_position,
        )

    for gate_id, (_, destination_id, destination_system_id, _) in gates.items():
        destination = gates.get(destination_id)
        if destination is None:
            raise ValueError(f"La stargate {gate_id} apunta a destino inexistente {destination_id}")
        if destination[0] != destination_system_id:
            raise ValueError(
                f"Stargate {gate_id}: destino {destination_id} pertenece al sistema "
                f"{destination[0]}, pero el SDE declara {destination_system_id}"
            )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stargate-routes-build-", dir=OUTPUT.parent) as temp_dir:
        plain_database = Path(temp_dir) / "stargateRoutes.sqlite"
        compressed_database = Path(temp_dir) / "stargateRoutes.zlib"
        origin_count, destination_count, bidirectional_count = create_database(
            plain_database, gates, source_hashes
        )
        compress_zlib(plain_database, compressed_database)
        os.replace(compressed_database, OUTPUT)

    print(
        f"SQLite creada: {origin_count:,} stargates origen; "
        f"{destination_count:,} conexiones dirigidas; "
        f"{bidirectional_count:,} conexiones con inversa y distancia coincidente; "
        f"{OUTPUT.stat().st_size:,} bytes zlib; "
        f"mapStargates_sha256={source_hashes['mapStargates_sha256']}; "
        f"mapSolarSystems_sha256={source_hashes['mapSolarSystems_sha256']}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, sqlite3.Error, ValueError, RuntimeError, zlib.error) as exc:
        print(f"Error construyendo stargateRoutes.db: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
