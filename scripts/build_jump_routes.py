"""Build a compressed SQLite graph of all sub-lightyear system pairs from the SDE."""

from __future__ import annotations

import hashlib
import argparse
import json
import math
import os
import sqlite3
import tempfile
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "sde" / "mapSolarSystems.db"
OUTPUT = ROOT / "data" / "derived" / "uni" / "jumpRoutes.db"
MAX_DISTANCE_M = 9_460_000_000_000_000
MAX_DISTANCE_SQUARED = float(MAX_DISTANCE_M) ** 2


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


SCHEMA_VERSION = "2"


def read_stored_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        with tempfile.TemporaryDirectory(prefix="jump-routes-check-") as temp_dir:
            database = Path(temp_dir) / "jumpRoutes.db"
            decompressor = zlib.decompressobj()
            with path.open("rb") as source, database.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    output.write(decompressor.decompress(chunk))
                output.write(decompressor.flush())
            with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
                rows = connection.execute(
                    "SELECT key, value FROM metadata WHERE key IN "
                    "('mapSolarSystem_sha256', 'schema_version')"
                )
                return {str(key): str(value) for key, value in rows}
    except (OSError, sqlite3.Error, zlib.error):
        return {}


def load_systems(path: Path) -> list[tuple[int, float, float, float]]:
    systems: list[tuple[int, float, float, float]] = []
    with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
        rows = connection.execute("SELECT document FROM records ORDER BY ordinal")
        for ordinal, (document,) in enumerate(rows, start=1):
            try:
                record = json.loads(document)
                system_id = record.get("_key")
                position = record.get("position")
                coordinates = (position["x"], position["y"], position["z"])
                if isinstance(system_id, bool) or not isinstance(system_id, int) or system_id <= 0:
                    raise ValueError("_key debe ser un entero positivo")
                if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in coordinates):
                    raise ValueError("position debe incluir x, y, z numéricos")
                x, y, z = (float(value) for value in coordinates)
                if not all(math.isfinite(value) for value in (x, y, z)):
                    raise ValueError("position incluye coordenadas no finitas")
                systems.append((system_id, x, y, z))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"mapSolarSystems, registro ordinal={ordinal}: {exc}") from exc
    if not systems:
        raise ValueError("mapSolarSystems.db no contiene sistemas")
    return systems


def spatial_candidates(
    systems: list[tuple[int, float, float, float]],
) -> Iterator[tuple[int, int, float]]:
    """Yield each different-system pair once; a 3D grid skips distant pairs safely."""
    cells: dict[tuple[int, int, int], list[tuple[int, float, float, float]]] = defaultdict(list)
    for system in systems:
        cell = tuple(math.floor(coordinate / MAX_DISTANCE_M) for coordinate in system[1:])
        cells[cell].append(system)

    for system_id, x, y, z in systems:
        cell_x, cell_y, cell_z = (
            math.floor(x / MAX_DISTANCE_M),
            math.floor(y / MAX_DISTANCE_M),
            math.floor(z / MAX_DISTANCE_M),
        )
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    for other_id, other_x, other_y, other_z in cells.get(
                        (cell_x + dx, cell_y + dy, cell_z + dz), ()
                    ):
                        if other_id <= system_id:
                            continue
                        distance_squared = (
                            (x - other_x) ** 2
                            + (y - other_y) ** 2
                            + (z - other_z) ** 2
                        )
                        if distance_squared < MAX_DISTANCE_SQUARED:
                            yield system_id, other_id, math.sqrt(distance_squared)


def create_database(
    path: Path,
    systems: list[tuple[int, float, float, float]],
    source_hash: str,
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
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            ("mapSolarSystem_sha256", source_hash),
        )
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        batch: list[tuple[int, int, int]] = []
        pair_count = 0
        for origin_id, destination_id, distance in spatial_candidates(systems):
            # Compare the unrounded distance first; truncate only for storage in whole metres.
            distance_m = int(distance)
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
        stored_pair_count = int(
            connection.execute("SELECT COUNT(*) FROM distances").fetchone()[0]
        )
        if stored_pair_count != pair_count:
            raise RuntimeError(
                f"Se calcularon {pair_count} parejas, pero SQLite contiene {stored_pair_count}"
            )
        return pair_count, stored_pair_count
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
    if not SOURCE.is_file():
        raise FileNotFoundError(f"No existe la fuente SDE: {SOURCE}")
    source_hash = sha256_file(SOURCE)
    stored_metadata = read_stored_metadata(OUTPUT)
    if not force and (
        stored_metadata.get("mapSolarSystem_sha256") == source_hash
        and stored_metadata.get("schema_version") == SCHEMA_VERSION
    ):
        print(
            f"Sin cambios: mapSolarSystems_sha256={source_hash}; "
            "no se recalculan los saltos. Usa --force para reconstruirlos.",
            flush=True,
        )
        return 0
    if force:
        print("Modo force activo: se reconstruye jumpRoutes.db aunque el SHA coincida.", flush=True)

    systems = load_systems(SOURCE)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="jump-routes-build-", dir=OUTPUT.parent) as temp_dir:
        plain_database = Path(temp_dir) / "jumpRoutes.db"
        compressed_database = Path(temp_dir) / "jumpRoutes.db.zlib"
        pair_count, destination_count = create_database(plain_database, systems, source_hash)
        compress_zlib(plain_database, compressed_database)
        os.replace(compressed_database, OUTPUT)

    print(
        f"SQLite creada: {len(systems):,} sistemas; {pair_count:,} parejas únicas "
        f"almacenadas sin invertir ni incluir el propio sistema; {destination_count:,} filas; "
        f"{OUTPUT.stat().st_size:,} bytes zlib; mapSolarSystem_sha256={source_hash}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="reconstruye la base aunque el SHA de la fuente coincida",
    )
    try:
        raise SystemExit(main(force=parser.parse_args().force))
    except (OSError, sqlite3.Error, ValueError, RuntimeError, zlib.error) as exc:
        print(f"Error construyendo jumpRoutes.db: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
