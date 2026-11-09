"""Download CCP's latest JSONL SDE and create one SQLite database per JSONL."""

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
from pathlib import Path, PurePosixPath


LATEST_URL = "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip"
USER_AGENT = "EVE-Route-Intel-SDE-Snapshot/1.0 (+https://github.com/landu-eou/eou)"
REQUEST_TIMEOUT_SECONDS = 120
GITHUB_FILE_LIMIT_BYTES = 100_000_000
INSERT_BATCH_SIZE = 2_000
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "sde"


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


def import_jsonl(connection: sqlite3.Connection, archive: zipfile.ZipFile, member: zipfile.ZipInfo) -> int:
    connection.executescript(
        """
        CREATE TABLE records (
            ordinal INTEGER PRIMARY KEY,
            document JSON NOT NULL
        );
        """
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
                json.loads(document)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CaptureError(f"JSONL inválido en {member.filename}, línea {line_number}: {exc}") from exc
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


def build_one_database(archive: zipfile.ZipFile, member: zipfile.ZipInfo, target: Path) -> int:
    connection = sqlite3.connect(target)
    try:
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA application_id = 1163281747")
        connection.execute("PRAGMA user_version = 1")
        count = import_jsonl(connection, archive, member)
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


def write_metadata(path: Path, metadata: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(metadata, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convierte el último SDE JSONL de CCP a bases SQLite")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
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

        existing_databases = list(output_dir.glob("*.db"))
        if existing_etag(metadata_path) == metadata["etag"] and existing_databases:
            print("El ETag SDE no ha cambiado; se conserva el snapshot existente.", flush=True)
            return 0

        with tempfile.TemporaryDirectory(prefix=".sde-build-", dir=output_dir) as temporary_dir:
            temp_dir = Path(temporary_dir)
            archive_path = temp_dir / "sde-jsonl.zip"
            downloaded = download_zip(download_url, archive_path, str(metadata["etag"]))
            if expected_size is not None and downloaded != int(expected_size):
                raise CaptureError(f"HEAD/GET difieren: {expected_size} frente a {downloaded} bytes")
            print(f"ZIP descargado: {downloaded:,} bytes", flush=True)

            staged = temp_dir / "databases"
            staged.mkdir()
            try:
                with zipfile.ZipFile(archive_path, "r") as archive:
                    members = sorted(
                        (item for item in archive.infolist()
                         if not item.is_dir() and item.filename.lower().endswith(".jsonl")),
                        key=lambda item: item.filename,
                    )
                    if not members:
                        raise CaptureError("El ZIP no contiene archivos JSONL")
                    output_names: set[str] = set()
                    total_records = 0
                    for member in members:
                        filename = database_filename(member.filename)
                        if filename in output_names:
                            raise CaptureError(f"Dos JSONL generan el mismo nombre DB: {filename}")
                        output_names.add(filename)
                        target = staged / filename
                        count = build_one_database(archive, member, target)
                        size = target.stat().st_size
                        if size >= GITHUB_FILE_LIMIT_BYTES:
                            raise CaptureError(
                                f"{filename} ocupa {size:,} bytes y supera el límite de 100 MB por archivo de GitHub"
                            )
                        total_records += count
                        print(f"{filename}: {count:,} registros, {size:,} bytes", flush=True)

                if total_records == 0:
                    raise CaptureError("Los archivos JSONL no contienen registros")

                metadata_temp = temp_dir / "metadata.json"
                write_metadata(metadata_temp, metadata)

                # Preserve the old snapshot unless every database has been built and checked.
                for old_db in output_dir.glob("*.db"):
                    if old_db.name not in output_names:
                        old_db.unlink()
                for filename in sorted(output_names):
                    os.replace(staged / filename, output_dir / filename)
                os.replace(metadata_temp, metadata_path)
            except (zipfile.BadZipFile, RuntimeError, OSError) as exc:
                if isinstance(exc, CaptureError):
                    raise
                raise CaptureError(f"No se pudo procesar el ZIP SDE: {exc}") from exc
    except (CaptureError, OSError, sqlite3.Error, zipfile.BadZipFile) as exc:
        print(f"Captura SDE fallida: {exc}", file=sys.stderr)
        return 1

    print(f"Snapshot publicado localmente en {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
