"""Download CCP's SDE JSONL ZIP and create SQLite databases with four workers."""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import json
import os
import re
import sqlite3
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PurePosixPath

import zstandard


LATEST_URL = "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip"
USER_AGENT = "EVE-Route-Intel-SDE-Snapshot/1.0 (+https://github.com/landu-eou/eou)"
REQUEST_TIMEOUT_SECONDS = 120
GITHUB_FILE_LIMIT_BYTES = 100_000_000
INSERT_BATCH_SIZE = 2_000
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "sde"
PRIORITY_FILES = {
    "mapConstellations.jsonl",
    "mapRegions.jsonl",
    "mapSolarSystems.jsonl",
    "mapStargates.jsonl",
    "npcCorporations.jsonl",
    "npcStations.jsonl",
    "types.jsonl",
}
ENGLISH_ONLY_FILES = {"mapPlanets.jsonl", "missions.jsonl"}
LANGUAGE_CODES = {"de", "en", "es", "fr", "ja", "ko", "ru", "zh"}


class CaptureError(RuntimeError):
    """The SDE snapshot cannot be safely published."""


def request(url: str, method: str):
    req = urllib.request.Request(
        url,
        method=method,
        headers={"Accept": "application/zip, application/json;q=0.9, */*;q=0.8", "User-Agent": USER_AGENT},
    )
    try:
        return urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise CaptureError(f"Error HTTP {method} {url}: {exc}") from exc


def read_response_metadata(headers) -> dict[str, object]:
    last_modified = headers.get("Last-Modified")
    etag = headers.get("ETag")
    cache_control = headers.get("Cache-Control", "")
    max_age_match = re.search(r"(?:^|,)\s*max-age\s*=\s*(\d+)", cache_control, re.IGNORECASE)
    if not last_modified or not etag or not max_age_match:
        raise CaptureError("La respuesta SDE debe incluir Last-Modified, ETag y Cache-Control con max-age")
    try:
        modified = email.utils.parsedate_to_datetime(last_modified)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CaptureError(f"Last-Modified no válido: {last_modified!r}") from exc
    if modified.tzinfo is None:
        raise CaptureError("Last-Modified no especifica zona horaria")
    modified = modified.astimezone(dt.timezone.utc)
    return {
        "last-modified": modified.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "max-age": int(max_age_match.group(1)),
        "etag": etag,
    }


def existing_etag(metadata_path: Path) -> str | None:
    try:
        with metadata_path.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
        value = data.get("etag") if isinstance(data, dict) else None
        return value if isinstance(value, str) else None
    except (OSError, json.JSONDecodeError):
        return None


def download_zip(url: str, target: Path, expected_etag: str) -> int:
    written = 0
    with request(url, "GET") as response, target.open("wb") as output:
        if response.headers.get("ETag") != expected_etag:
            raise CaptureError("El ETag cambió entre HEAD y GET; no se publicará esta descarga")
        expected_size = response.headers.get("Content-Length")
        while block := response.read(1024 * 1024):
            output.write(block)
            written += len(block)
        if expected_size is not None and written != int(expected_size):
            raise CaptureError(f"Descarga incompleta: {written} de {expected_size} bytes")
    if not written:
        raise CaptureError("El ZIP SDE descargado está vacío")
    return written


def database_filename(member_name: str) -> str:
    path = PurePosixPath(member_name)
    if path.is_absolute() or ".." in path.parts:
        raise CaptureError(f"Ruta insegura dentro del ZIP: {member_name!r}")
    if not path.name.lower().endswith(".jsonl"):
        raise CaptureError(f"Miembro no JSONL: {member_name!r}")
    return f"{path.name[:-6]}.db"


def keep_english_localizations(value):
    """Keep only English entries in nested SDE localization maps."""
    if isinstance(value, dict):
        language_keys = {key for key in value if key in LANGUAGE_CODES}
        localized = "en" in language_keys and len(language_keys) > 1
        result = {}
        changed = False
        for key, item in value.items():
            if localized and key in LANGUAGE_CODES and key != "en":
                changed = True
                continue
            converted, nested_change = keep_english_localizations(item)
            result[key] = converted
            changed = changed or nested_change
        return result, changed
    if isinstance(value, list):
        converted_items = [keep_english_localizations(item) for item in value]
        return [item for item, _changed in converted_items], any(changed for _item, changed in converted_items)
    return value, False


def import_jsonl(
    connection: sqlite3.Connection,
    archive: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    english_only: bool,
) -> int:
    connection.execute(
        "CREATE TABLE records (ordinal INTEGER PRIMARY KEY, document JSON NOT NULL)"
    )
    count = 0
    batch: list[tuple[int, str]] = []
    with archive.open(member, "r") as source:
        for line_number, raw_line in enumerate(source, start=1):
            raw_line = raw_line.rstrip(b"\r\n")
            if not raw_line:
                continue
            try:
                document = raw_line.decode("utf-8", errors="strict")
                parsed = json.loads(document)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CaptureError(f"JSONL inválido en {member.filename}, línea {line_number}: {exc}") from exc
            if english_only:
                parsed, changed = keep_english_localizations(parsed)
                if changed:
                    document = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
            count += 1
            batch.append((count, document))
            if len(batch) >= INSERT_BATCH_SIZE:
                connection.executemany("INSERT INTO records(ordinal, document) VALUES (?, ?)", batch)
                batch.clear()
    if batch:
        connection.executemany("INSERT INTO records(ordinal, document) VALUES (?, ?)", batch)
    if not count:
        raise CaptureError(f"El dataset {member.filename} está vacío")
    return count


def build_one_database(archive_path: Path, member_name: str, target: Path, english_only: bool) -> int:
    with zipfile.ZipFile(archive_path, "r") as archive:
        member = archive.getinfo(member_name)
        connection = sqlite3.connect(target)
        try:
            connection.execute("PRAGMA page_size = 4096")
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("PRAGMA application_id = 1163281747")
            connection.execute("PRAGMA user_version = 1")
            count = import_jsonl(connection, archive, member, english_only)
            connection.commit()
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise CaptureError(f"Falló integrity_check para {member.filename}: {result}")
            actual_count = int(connection.execute("SELECT COUNT(*) FROM records").fetchone()[0])
            if actual_count != count:
                raise CaptureError(f"Recuento incorrecto en {member.filename}: {actual_count} frente a {count}")
            connection.execute("VACUUM")
            return count
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def compress_database(database_path: Path, compressed_path: Path) -> int:
    compressor = zstandard.ZstdCompressor(level=22, write_checksum=True, write_content_size=False)
    with database_path.open("rb") as source, compressed_path.open("wb") as output:
        compressor.copy_stream(source, output)
    return compressed_path.stat().st_size


def convert_member(archive_path: Path, member_name: str, staged: Path) -> tuple[str, int, int]:
    filename = database_filename(member_name)
    target = staged / filename
    english_only = PurePosixPath(member_name).name in ENGLISH_ONLY_FILES
    if english_only:
        print(f"{PurePosixPath(member_name).name}: se conservará únicamente inglés.", flush=True)
    count = build_one_database(archive_path, member_name, target, english_only)
    size = target.stat().st_size
    if size >= GITHUB_FILE_LIMIT_BYTES:
        compressed_name = f"{filename}.zst"
        compressed_target = staged / compressed_name
        compressed_size = compress_database(target, compressed_target)
        if compressed_size >= GITHUB_FILE_LIMIT_BYTES:
            raise CaptureError(
                f"{filename} ocupa {size:,} bytes y comprimida {compressed_size:,}; "
                "ambas superan el límite de 100 MB por archivo de GitHub"
            )
        target.unlink()
        filename = compressed_name
        size = compressed_size
    print(f"Preparada {filename}: {count:,} registros, {size:,} bytes", flush=True)
    return filename, count, size


def write_metadata(path: Path, metadata: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(metadata, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def publish_group(staged: Path, output_dir: Path, outputs: list[tuple[str, int, int]], label: str) -> None:
    for filename, _count, _size in outputs:
        os.replace(staged / filename, output_dir / filename)
    print(f"Lote {label} publicado: {len(outputs)} bases", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convierte el último SDE JSONL de CCP a bases SQLite")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--force", action="store_true", help="Descarga aunque el ETag coincida con el snapshot local")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.json"

    try:
        with request(LATEST_URL, "HEAD") as response:
            download_url = response.geturl()
            metadata = read_response_metadata(response.headers)
            expected_size = response.headers.get("Content-Length")

        current_etag = existing_etag(metadata_path)
        if current_etag == metadata["etag"] and not args.force:
            print(
                f"ETag sin cambios ({metadata['etag']}); data/sde ya tiene esta versión. "
                "Se omiten descarga y conversión. Usa force para reconstruirla.",
                flush=True,
            )
            return 0
        if args.force and current_etag == metadata["etag"]:
            print("Forzado activado: se reconstruirá el snapshot aunque el ETag coincida.", flush=True)

        with tempfile.TemporaryDirectory(prefix=".sde-build-", dir=output_dir) as temporary_dir:
            temp_dir = Path(temporary_dir)
            archive_path = temp_dir / "sde-jsonl.zip"
            downloaded = download_zip(download_url, archive_path, str(metadata["etag"]))
            if expected_size is not None and downloaded != int(expected_size):
                raise CaptureError(f"HEAD/GET difieren: {expected_size} frente a {downloaded} bytes")
            print(f"ZIP descargado: {downloaded:,} bytes", flush=True)

            with zipfile.ZipFile(archive_path, "r") as archive:
                members = sorted(
                    (item.filename for item in archive.infolist()
                     if not item.is_dir() and item.filename.lower().endswith(".jsonl")),
                    key=str.casefold,
                )
            if not members:
                raise CaptureError("El ZIP no contiene archivos JSONL")
            member_by_basename = {PurePosixPath(name).name: name for name in members}
            if len(member_by_basename) != len(members):
                raise CaptureError("Hay nombres JSONL duplicados en distintas rutas del ZIP")
            missing_priority = PRIORITY_FILES - set(member_by_basename)
            if missing_priority:
                raise CaptureError(f"Faltan archivos prioritarios en el ZIP: {', '.join(sorted(missing_priority))}")

            priority_members = [member_by_basename[name] for name in sorted(PRIORITY_FILES)]
            secondary_members = [name for name in members if PurePosixPath(name).name not in PRIORITY_FILES]
            staged = temp_dir / "databases"
            staged.mkdir()

            # One dedicated priority worker plus three workers for the remaining datasets.
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="sde-priority") as priority_pool, \
                    ThreadPoolExecutor(max_workers=3, thread_name_prefix="sde-secondary") as secondary_pool:
                priority_future = priority_pool.submit(
                    lambda: [convert_member(archive_path, name, staged) for name in priority_members]
                )
                secondary_futures = [
                    secondary_pool.submit(convert_member, archive_path, name, staged)
                    for name in secondary_members
                ]

                priority_outputs = priority_future.result()
                publish_group(staged, output_dir, priority_outputs, "prioritario")

                secondary_outputs = [future.result() for future in as_completed(secondary_futures)]
                if secondary_outputs:
                    publish_group(staged, output_dir, secondary_outputs, "secundario")

            all_outputs = priority_outputs + secondary_outputs
            output_names = {filename for filename, _count, _size in all_outputs}
            if len(output_names) != len(all_outputs):
                raise CaptureError("Dos JSONL generan el mismo nombre de base de datos")
            if sum(count for _filename, count, _size in all_outputs) == 0:
                raise CaptureError("Los archivos JSONL no contienen registros")

            # Remove every stale file at data/sde root, keeping metadata until the snapshot is complete.
            for existing in output_dir.iterdir():
                if existing.is_file() and existing.name != metadata_path.name and existing.name not in output_names:
                    existing.unlink()

            metadata_temp = temp_dir / "metadata.json"
            write_metadata(metadata_temp, metadata)
            os.replace(metadata_temp, metadata_path)
    except (CaptureError, OSError, sqlite3.Error, zstandard.ZstdError, zipfile.BadZipFile) as exc:
        print(f"Captura SDE fallida: {exc}", file=sys.stderr)
        return 1

    print(f"Snapshot publicado localmente en {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
