"""Capture hourly ESI system jumps and kills into per-metric SQLite snapshots."""

from __future__ import annotations

import datetime as dt
import email.utils
import json
import random
import re
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ESI = "https://esi.evetech.net/latest"
USER_AGENT = "EVE-Route-Intel/0.3 (+https://github.com/landu-eou/eou)"
COMPATIBILITY_DATE = "2026-11-08"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
FALLBACK_CACHE_SECONDS = 3600
ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "esi" / "uni"


class CaptureError(RuntimeError):
    """The ESI activity snapshot could not be safely captured."""


@dataclass(frozen=True)
class Dataset:
    filename: str
    metric: str

    @property
    def path(self) -> Path:
        return OUTPUT_DIR / self.filename


@dataclass(frozen=True)
class Endpoint:
    name: str
    path: str
    fields: tuple[str, ...]
    datasets: tuple[Dataset, ...]


SHIP_JUMPS = Dataset("shipJumps.db", "ship_jumps")
NPC_KILLS = Dataset("npcKills.db", "npc_kills")
POD_KILLS = Dataset("podKills.db", "pod_kills")
SHIP_KILLS = Dataset("shipKills.db", "ship_kills")
ENDPOINTS = (
    Endpoint("ship jumps", "/universe/system_jumps/", ("ship_jumps",), (SHIP_JUMPS,)),
    Endpoint("system kills", "/universe/system_kills/", ("npc_kills", "pod_kills", "ship_kills"),
             (NPC_KILLS, POD_KILLS, SHIP_KILLS)),
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_http_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).replace(microsecond=0)
    except (TypeError, ValueError, OverflowError):
        return None


def next_request_at(headers: dict[str, str], received_at: dt.datetime) -> dt.datetime:
    """Honor both cache headers conservatively; the endpoint TTL is at most one hour."""
    deadlines: list[dt.datetime] = []
    expires_at = parse_http_date(headers.get("expires"))
    if expires_at is not None:
        deadlines.append(expires_at)

    cache_control = headers.get("cache-control", "")
    max_age_match = re.search(r"(?:^|,)\s*max-age\s*=\s*(\d+)\s*(?:,|$)", cache_control, re.IGNORECASE)
    if max_age_match:
        server_date = parse_http_date(headers.get("date")) or received_at
        deadlines.append(server_date + dt.timedelta(seconds=int(max_age_match.group(1))))

    if deadlines:
        return max(deadlines)
    return received_at + dt.timedelta(seconds=FALLBACK_CACHE_SECONDS)


def retry_delay(headers: Any, attempt: int, status: int | None = None) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            retry_at = parse_http_date(retry_after)
            if retry_at is not None:
                return max(0.0, (retry_at - utc_now()).total_seconds())
    if status == 420 and headers:
        try:
            return max(1.0, float(headers.get("X-ESI-Error-Limit-Reset", "1")))
        except (TypeError, ValueError):
            pass
    return min(30.0, 2**attempt + random.random())


def _lower_headers(headers: Any) -> dict[str, str]:
    return {key.lower(): value for key, value in headers.items()}


def fetch_endpoint(
    endpoint: Endpoint, etag: str | None
) -> tuple[list[dict[str, Any]] | None, dict[str, str], bool, dt.datetime]:
    request_headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "X-Compatibility-Date": COMPATIBILITY_DATE,
    }
    if etag:
        request_headers["If-None-Match"] = etag
    request = urllib.request.Request(f"{ESI}{endpoint.path}", headers=request_headers)

    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                received_at = utc_now()
                headers = _lower_headers(response.headers)
                try:
                    payload = json.loads(response.read())
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise CaptureError(f"ESI devolvió JSON inválido para {endpoint.name}") from exc
                if not isinstance(payload, list):
                    raise CaptureError(f"ESI devolvió una respuesta que no es una lista para {endpoint.name}")
                return payload, headers, False, received_at
        except urllib.error.HTTPError as exc:
            received_at = utc_now()
            headers = _lower_headers(exc.headers)
            if exc.code == 304:
                return None, headers, True, received_at
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"ESI respondió HTTP {exc.code} para {endpoint.name}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            print(f"ESI {endpoint.name}: HTTP {exc.code}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Error de red en ESI {endpoint.name}: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            print(f"ESI {endpoint.name}: error de red; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def validate_payload(payload: list[dict[str, Any]], fields: tuple[str, ...]) -> dict[int, dict[str, int]]:
    systems: dict[int, dict[str, int]] = {}
    for index, record in enumerate(payload, start=1):
        if not isinstance(record, dict):
            raise CaptureError(f"Registro {index}: ESI devolvió un valor que no es objeto")
        system_id = record.get("system_id")
        if isinstance(system_id, bool) or not isinstance(system_id, int) or system_id <= 0:
            raise CaptureError(f"Registro {index}: system_id inválido")
        if system_id in systems:
            raise CaptureError(f"ESI repitió system_id={system_id}")
        values = {}
        for field in fields:
            value = record.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise CaptureError(f"system_id={system_id}: {field} inválido")
            values[field] = value
        systems[system_id] = values
    return systems


def load_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'metadata'"
            ).fetchone()
            if not table:
                return {}
            return dict(connection.execute("SELECT key, value FROM metadata"))
    except sqlite3.Error as exc:
        raise CaptureError(f"No se pudo leer el estado de caché en {path}: {exc}") from exc


def shared_state(endpoint: Endpoint) -> dict[str, str]:
    states = [load_metadata(dataset.path) for dataset in endpoint.datasets]
    if not states or any(not state for state in states):
        return {}
    shared_keys = ("etag", "next_request_at_utc")
    if any(any(state.get(key) != states[0].get(key) for key in shared_keys) for state in states[1:]):
        return {}
    state = states[0]
    if not state.get("etag") or not state.get("next_request_at_utc"):
        return {}
    try:
        expires_at = dt.datetime.fromisoformat(state["next_request_at_utc"].replace("Z", "+00:00"))
    except ValueError:
        return {}
    if expires_at.tzinfo is None:
        return {}
    for dataset in endpoint.datasets:
        if not has_capture(dataset.path, state["etag"]):
            return {}
    return state


def has_capture(path: Path, etag: str) -> bool:
    if not path.is_file():
        return False
    try:
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'captures'"
            ).fetchone()
            if not table:
                return False
            return connection.execute("SELECT 1 FROM captures WHERE etag = ? LIMIT 1", (etag,)).fetchone() is not None
    except sqlite3.Error as exc:
        raise CaptureError(f"No se pudieron comprobar snapshots en {path}: {exc}") from exc


def _schema(connection: sqlite3.Connection, dataset: Dataset) -> None:
    metric_table = dataset.metric
    connection.execute(
        """CREATE TABLE IF NOT EXISTS captures (
            date TEXT PRIMARY KEY,
            captured_at_utc TEXT NOT NULL,
            etag TEXT NOT NULL,
            last_modified TEXT
        )"""
    )
    connection.execute(
        f"""CREATE TABLE IF NOT EXISTS {metric_table} (
            date TEXT NOT NULL REFERENCES captures(date) ON DELETE CASCADE,
            system_id INTEGER NOT NULL CHECK (system_id > 0),
            {dataset.metric} INTEGER NOT NULL CHECK ({dataset.metric} >= 0),
            PRIMARY KEY (date, system_id)
        ) WITHOUT ROWID"""
    )
    connection.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
    connection.execute(
        f"""CREATE VIEW IF NOT EXISTS observations AS
            SELECT captures.date, captures.captured_at_utc, {metric_table}.system_id, {metric_table}.{dataset.metric}
            FROM captures JOIN {metric_table} USING (date)"""
    )


def _copy_to_temp(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.unlink(missing_ok=True)
    if path.is_file():
        shutil.copy2(path, temporary)
    return temporary


def _prepare_database(
    dataset: Dataset,
    state: dict[str, str],
    systems: dict[int, dict[str, int]] | None = None,
    snapshot: tuple[str, str, str, str | None] | None = None,
) -> tuple[Path, bool]:
    target = dataset.path
    temporary = _copy_to_temp(target)
    captured = False
    connection = sqlite3.connect(temporary)
    try:
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA foreign_keys = ON")
        _schema(connection, dataset)
        if snapshot is not None and systems is not None:
            date_key, captured_at, etag, last_modified = snapshot
            # At most one snapshot is allowed for each UTC hour. A newer ETag in
            # the same hour replaces that hour's data instead of duplicating it.
            connection.execute("DELETE FROM captures WHERE date = ?", (date_key,))
            connection.execute(
                "INSERT INTO captures(date, captured_at_utc, etag, last_modified) VALUES (?, ?, ?, ?)",
                snapshot,
            )
            connection.executemany(
                f"INSERT INTO {dataset.metric}(date, system_id, {dataset.metric}) VALUES (?, ?, ?)",
                ((date_key, system_id, values[dataset.metric]) for system_id, values in sorted(systems.items())),
            )
            captured = True
        connection.execute("DELETE FROM metadata")
        connection.executemany("INSERT INTO metadata(key, value) VALUES (?, ?)", sorted(state.items()))
        connection.commit()
        connection.execute("VACUUM")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise CaptureError(f"Falló integrity_check en {target}: {integrity}")
    except Exception:
        connection.close()
        temporary.unlink(missing_ok=True)
        raise
    connection.close()
    return temporary, captured


def persist_endpoint(
    endpoint: Endpoint,
    headers: dict[str, str],
    checked_at: dt.datetime,
    etag: str,
    last_modified: str | None,
    systems: dict[int, dict[str, int]] | None,
) -> int:
    cache_until = next_request_at(headers, checked_at)
    state = {
        "etag": etag,
        "last_modified": last_modified or "",
        "cache_control": headers.get("cache-control", ""),
        "expires": headers.get("expires", ""),
        "next_request_at_utc": iso_utc(cache_until),
        "checked_at_utc": iso_utc(checked_at),
    }
    snapshot = None
    if systems is not None:
        hour = checked_at.replace(minute=0, second=0, microsecond=0)
        snapshot = (iso_utc(hour), iso_utc(checked_at), etag, last_modified)

    prepared: list[tuple[Path, Path, bool]] = []
    try:
        for dataset in endpoint.datasets:
            temporary, captured = _prepare_database(dataset, state, systems, snapshot)
            prepared.append((temporary, dataset.path, captured))
        for temporary, target, _captured in prepared:
            temporary.replace(target)
    except Exception:
        for temporary, _target, _captured in prepared:
            temporary.unlink(missing_ok=True)
        raise
    return sum(1 for _temporary, _target, captured in prepared if captured)


def process_endpoint(endpoint: Endpoint, now: dt.datetime | None = None) -> bool:
    current_time = now or utc_now()
    state = shared_state(endpoint)
    if state:
        next_at = dt.datetime.fromisoformat(state["next_request_at_utc"].replace("Z", "+00:00"))
        if current_time < next_at:
            print(f"ESI {endpoint.name}: caché fresca hasta {state['next_request_at_utc']}; sin petición.", flush=True)
            return False

    conditional_etag = state.get("etag") if state else None
    print(f"Consultando ESI {endpoint.name}" + (" con If-None-Match." if conditional_etag else " (sin ETag local)."), flush=True)
    payload, headers, not_modified, checked_at = fetch_endpoint(endpoint, conditional_etag)
    response_etag = headers.get("etag") or conditional_etag
    if not response_etag:
        raise CaptureError(f"ESI no devolvió ETag para {endpoint.name}; se conserva el estado anterior")

    if not_modified:
        if not state:
            raise CaptureError(f"ESI respondió 304 para {endpoint.name} sin snapshot local")
        captures_written = persist_endpoint(
            endpoint,
            headers,
            checked_at,
            response_etag,
            headers.get("last-modified") or state.get("last_modified"),
            systems=None,
        )
        print(
            f"ESI {endpoint.name}: ETag sin cambios; 0 snapshots añadidos, caché renovada "
            f"hasta {iso_utc(next_request_at(headers, checked_at))}.",
            flush=True,
        )
        return captures_written > 0

    assert payload is not None
    systems = validate_payload(payload, endpoint.fields)
    if not headers.get("etag"):
        raise CaptureError(f"ESI no devolvió ETag en la respuesta de {endpoint.name}")
    changed_etag = response_etag != state.get("etag")
    captures_written = persist_endpoint(
        endpoint,
        headers,
        checked_at,
        response_etag,
        headers.get("last-modified"),
        systems if changed_etag else None,
    )
    if captures_written:
        print(
            f"ESI {endpoint.name}: snapshot {iso_utc(checked_at.replace(minute=0, second=0, microsecond=0))}; "
            f"{len(systems):,} sistemas; guardado en {captures_written} DB.",
            flush=True,
        )
    elif changed_etag:
        print(f"ESI {endpoint.name}: no se añadió snapshot.", flush=True)
    else:
        print(f"ESI {endpoint.name}: ETag sin cambios; 0 snapshots añadidos.", flush=True)
    return True


def main() -> int:
    failures = []
    for endpoint in ENDPOINTS:
        try:
            process_endpoint(endpoint)
        except (CaptureError, OSError, sqlite3.Error, ValueError) as exc:
            failures.append(f"{endpoint.name}: {exc}")
            print(f"Captura fallida ({endpoint.name}): {exc}", file=sys.stderr, flush=True)
    if failures:
        print("Fallaron endpoints: " + " | ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
