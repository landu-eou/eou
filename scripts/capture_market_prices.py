"""Capture ESI's public market price list as a compact SQLite snapshot."""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import json
import math
import os
import random
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


URL = "https://esi.evetech.net/latest/markets/prices/"
USER_AGENT = "EVE-Route-Intel/0.3 (+https://github.com/landu-eou/eou)"
COMPATIBILITY_DATE = "2026-11-08"
REQUEST_TIMEOUT_SECONDS = 90
MAX_RETRIES = 5
OUTPUT = Path(__file__).resolve().parents[1] / "data" / "esi" / "market" / "marketPrices.db"


class CaptureError(RuntimeError):
    """The ESI market prices snapshot could not be safely captured."""


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def load_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            return dict(connection.execute("SELECT key, value FROM metadata"))
    except sqlite3.Error as exc:
        raise CaptureError(f"No se pudo leer {path}: {exc}") from exc


def retry_delay(headers: Any, attempt: int, status: int | None = None) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = email.utils.parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=dt.timezone.utc)
                return max(0.0, (retry_at - utc_now()).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    if status == 420 and headers:
        try:
            return max(1.0, float(headers.get("X-ESI-Error-Limit-Reset", "1")))
        except ValueError:
            pass
    return min(30.0, 2**attempt + random.random())


def fetch_prices(etag: str | None) -> tuple[list[dict[str, Any]] | None, dict[str, str], bool]:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "X-Compatibility-Date": COMPATIBILITY_DATE,
    }
    if etag:
        headers["If-None-Match"] = etag

    for attempt in range(MAX_RETRIES + 1):
        request = urllib.request.Request(URL, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read())
                response_headers = {key.lower(): value for key, value in response.headers.items()}
                if not isinstance(payload, list) or not payload:
                    raise CaptureError("ESI no devolvió una lista de precios válida")
                return payload, response_headers, False
        except urllib.error.HTTPError as exc:
            if exc.code == 304:
                return None, {key.lower(): value for key, value in exc.headers.items()}, True
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"ESI respondió HTTP {exc.code}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            print(f"ESI HTTP {exc.code}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Error de red al consultar ESI: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            print(f"Error de red; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError("ESI devolvió JSON inválido") from exc
            delay = min(30.0, 2**attempt + random.random())
            time.sleep(delay)
    raise AssertionError("unreachable")


def validate_prices(payload: list[dict[str, Any]]) -> list[tuple[int, float | None, float | None]]:
    rows = []
    seen = set()
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise CaptureError(f"Registro {index}: se esperaba un objeto JSON")
        type_id = item.get("type_id")
        if isinstance(type_id, bool) or not isinstance(type_id, int) or type_id <= 0:
            raise CaptureError(f"Registro {index}: type_id no válido")
        if type_id in seen:
            raise CaptureError(f"ESI repitió type_id={type_id}")
        seen.add(type_id)
        values = []
        for field in ("adjusted_price", "average_price"):
            value = item.get(field)
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise CaptureError(f"type_id={type_id}: {field} no es numérico")
                if not math.isfinite(value) or value < 0:
                    raise CaptureError(f"type_id={type_id}: {field} no es finito o es negativo")
                value = float(value)
            values.append(value)
        rows.append((type_id, values[0], values[1]))
    return rows


def write_snapshot(
    target: Path,
    rows: list[tuple[int, float | None, float | None]],
    metadata: dict[str, str],
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        with sqlite3.connect(temporary) as connection:
            connection.execute("PRAGMA page_size = 4096")
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("CREATE TABLE prices (type_id INTEGER PRIMARY KEY, adjusted_price REAL, average_price REAL)")
            connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            connection.executemany(
                "INSERT INTO prices(type_id, adjusted_price, average_price) VALUES (?, ?, ?)", rows
            )
            connection.executemany("INSERT INTO metadata(key, value) VALUES (?, ?)", sorted(metadata.items()))
            connection.commit()
            connection.execute("VACUUM")
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise CaptureError(f"Falló integrity_check de SQLite: {integrity}")
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Captura ESI /markets/prices/ en SQLite")
    parser.add_argument("--force", action="store_true", help="Ignora el intervalo y solicita el cuerpo completo")
    args = parser.parse_args()
    try:
        previous = load_metadata(OUTPUT)
        etag = None if args.force else previous.get("etag")
        payload, headers, not_modified = fetch_prices(etag)
        if not_modified:
            if not OUTPUT.is_file():
                raise CaptureError("ESI respondió 304 pero no existe una base local para conservar")
            print(f"ESI respondió 304 Not Modified; snapshot conservado en {OUTPUT}", flush=True)
            return 0

        assert payload is not None
        rows = validate_prices(payload)
        captured_at = utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")
        new_metadata = {
            "captured_at_utc": captured_at,
            "etag": headers.get("etag", ""),
            "last_modified": headers.get("last-modified", ""),
            "cache_control": headers.get("cache-control", ""),
            "expires": headers.get("expires", ""),
        }
        write_snapshot(OUTPUT, rows, new_metadata)
        print(f"Guardados {len(rows):,} precios de {len(rows):,} tipos en {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)", flush=True)
        return 0
    except (CaptureError, OSError, sqlite3.Error) as exc:
        print(f"Captura de precios fallida: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
