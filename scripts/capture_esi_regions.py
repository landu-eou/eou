"""Capture ESI region metadata into data/esi/uni/regions.db."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import os
import random
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ESI = "https://esi.evetech.net/latest"
USER_AGENT = "EVE-Route-Intel/0.3 (+https://github.com/landu-eou/eou)"
COMPATIBILITY_DATE = "2026-11-08"
MAX_RETRIES = 5
WORKERS = 6
ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "esi" / "uni" / "regions.db"


class CaptureError(RuntimeError):
    """ESI data was unavailable or invalid; do not replace the previous DB."""


def retry_delay(headers: Any, attempt: int, status: int | None = None) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = dt.datetime.strptime(retry_after, "%a, %d %b %Y %H:%M:%S GMT")
                return max(0.0, (retry_at.replace(tzinfo=dt.timezone.utc) - dt.datetime.now(dt.timezone.utc)).total_seconds())
            except ValueError:
                pass
    if headers and status == 420:
        try:
            return max(1.0, float(headers.get("X-ESI-Error-Limit-Reset", "1")))
        except ValueError:
            pass
    return min(30.0, (2**attempt) + random.random())


def fetch_json(url: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Language": "en",
            "User-Agent": USER_AGENT,
            "X-Compatibility-Date": COMPATIBILITY_DATE,
        },
    )
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"ESI respondió HTTP {exc.code} para {url}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            print(f"ESI HTTP {exc.code}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Error de red al consultar ESI: {exc}") from exc
            delay = min(30.0, (2**attempt) + random.random())
            print(f"Error de red; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"ESI devolvió JSON inválido para {url}") from exc
            time.sleep(min(30.0, (2**attempt) + random.random()))
    raise AssertionError("unreachable")


def fetch_region_ids() -> list[int]:
    payload = fetch_json(f"{ESI}/universe/regions/")
    if not isinstance(payload, list) or not payload:
        raise CaptureError("ESI no devolvió una lista de regiones válida")
    if any(isinstance(region_id, bool) or not isinstance(region_id, int) or region_id <= 0 for region_id in payload):
        raise CaptureError("La lista de regiones contiene un ID no válido")
    return sorted(set(payload))


def fetch_region(region_id: int) -> tuple[int, str, str | None, list[int]]:
    payload = fetch_json(f"{ESI}/universe/regions/{region_id}/")
    if not isinstance(payload, dict):
        raise CaptureError(f"Región {region_id}: se esperaba un objeto JSON")
    returned_id = payload.get("region_id")
    name = payload.get("name")
    description = payload.get("description")
    constellations = payload.get("constellations")
    if returned_id != region_id:
        raise CaptureError(f"Región {region_id}: ESI devolvió region_id={returned_id}")
    if not isinstance(name, str) or not name:
        raise CaptureError(f"Región {region_id}: nombre ausente o no válido")
    if description is not None and not isinstance(description, str):
        raise CaptureError(f"Región {region_id}: descripción no válida")
    if not isinstance(constellations, list) or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in constellations
    ):
        raise CaptureError(f"Región {region_id}: lista de constelaciones no válida")
    if len(set(constellations)) != len(constellations):
        raise CaptureError(f"Región {region_id}: constelaciones duplicadas en ESI")
    return region_id, name, description, sorted(constellations)


def fetch_all_regions() -> list[tuple[int, str, str | None, list[int]]]:
    region_ids = fetch_region_ids()
    regions: list[tuple[int, str, str | None, list[int]]] = []
    failures: list[tuple[int, Exception]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="esi-region") as pool:
        futures = {pool.submit(fetch_region, region_id): region_id for region_id in region_ids}
        for completed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            region_id = futures[future]
            try:
                regions.append(future.result())
            except Exception as exc:
                failures.append((region_id, exc))
            print(f"Regiones recibidas: {completed}/{len(region_ids)}", flush=True)
    if failures:
        details = "; ".join(f"{region_id}: {error}" for region_id, error in failures[:8])
        raise CaptureError(f"Fallaron {len(failures)} regiones; no se actualizará la base. {details}")
    regions.sort(key=lambda region: region[0])
    if len(regions) != len(region_ids):
        raise CaptureError("No se recibieron todos los detalles regionales")
    return regions


def write_database(regions: list[tuple[int, str, str | None, list[int]]]) -> Path:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".regions.", suffix=".tmp", dir=OUTPUT.parent, delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        connection = sqlite3.connect(temp_path)
        try:
            connection.execute("PRAGMA page_size=4096")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA user_version=1")
            connection.execute(
                """CREATE TABLE regions (
                    region_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT
                )"""
            )
            connection.execute(
                """CREATE TABLE region_constellations (
                    region_id INTEGER NOT NULL REFERENCES regions(region_id),
                    constellation_id INTEGER NOT NULL,
                    PRIMARY KEY (region_id, constellation_id)
                ) WITHOUT ROWID"""
            )
            connection.executemany(
                "INSERT INTO regions(region_id, name, description) VALUES (?, ?, ?)",
                ((region_id, name, description) for region_id, name, description, _ in regions),
            )
            connection.executemany(
                "INSERT INTO region_constellations(region_id, constellation_id) VALUES (?, ?)",
                (
                    (region_id, constellation_id)
                    for region_id, _, _, constellation_ids in regions
                    for constellation_id in constellation_ids
                ),
            )
            connection.commit()
            connection.execute("VACUUM")
        finally:
            connection.close()
        os.replace(temp_path, OUTPUT)
        return OUTPUT
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def main() -> int:
    try:
        regions = fetch_all_regions()
        path = write_database(regions)
    except Exception as exc:
        print(f"Captura cancelada; se conserva la base anterior: {exc}", file=sys.stderr)
        return 1
    constellation_count = sum(len(region[3]) for region in regions)
    print(
        f"Guardadas {len(regions)} regiones y {constellation_count} relaciones región-constelación "
        f"en {path} ({path.stat().st_size:,} bytes)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
