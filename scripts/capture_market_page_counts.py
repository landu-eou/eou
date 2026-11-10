"""Fetch only X-Pages from page 1 of each regional market-order route."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import random
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any


ESI = "https://esi.evetech.net/latest"
USER_AGENT = "EVE-Route-Intel/0.3 (+https://github.com/landu-eou/eou)"
COMPATIBILITY_DATE = "2026-11-08"
MAX_RETRIES = 5
MAX_WORKERS = 6
FALLBACK_CACHE_SECONDS = 300
ROOT = Path(__file__).resolve().parents[1]
REGIONS_SDE_DB = ROOT / "data" / "sde" / "mapRegions.db"
OUTPUT = ROOT / "data" / "esi" / "mkt" / "pages.yml"


class CaptureError(RuntimeError):
    """Page counts could not be collected as a complete set."""


def _delay(headers: Any, attempt: int, status: int | None = None) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=dt.timezone.utc)
                return max(0.0, (retry_at - dt.datetime.now(dt.timezone.utc)).total_seconds())
            except (TypeError, ValueError):
                pass
    if status == 420 and headers:
        try:
            return max(1.0, float(headers.get("X-ESI-Error-Limit-Reset", "1")))
        except ValueError:
            pass
    return min(30.0, 2**attempt + random.random())


def _expires_at(value: str | None) -> tuple[str, float]:
    if value:
        try:
            expires = parsedate_to_datetime(value)
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=dt.timezone.utc)
            expires = expires.astimezone(dt.timezone.utc)
            return expires.isoformat(timespec="seconds").replace("+00:00", "Z"), expires.timestamp()
        except (TypeError, ValueError, OverflowError):
            pass
    expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=FALLBACK_CACHE_SECONDS)
    return expires.isoformat(timespec="seconds").replace("+00:00", "Z"), expires.timestamp()


def fetch_page_count(region_id: int) -> tuple[int, int, str]:
    query = urllib.parse.urlencode({"order_type": "all", "page": 1})
    url = f"{ESI}/markets/{region_id}/orders/?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "X-Compatibility-Date": COMPATIBILITY_DATE,
        },
    )

    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw_pages = response.headers.get("X-Pages")
                try:
                    pages = int(raw_pages) if raw_pages is not None else 0
                except ValueError as exc:
                    raise CaptureError(f"Región {region_id}: encabezado X-Pages inválido: {raw_pages!r}") from exc
                if pages < 1:
                    raise CaptureError(f"Región {region_id}: ESI no devolvió un X-Pages válido")
                expires_at, _ = _expires_at(response.headers.get("Expires"))

                # ESI always returns page 1's orders in the body. Drain and discard them;
                # only the pagination header is needed for this capture.
                while response.read(64 * 1024):
                    pass
                return region_id, pages, expires_at
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"Región {region_id}: ESI respondió HTTP {exc.code}: {detail}") from exc
            delay = _delay(exc.headers, attempt, exc.code)
            print(f"Región {region_id}: HTTP {exc.code}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Región {region_id}: error de red: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            print(f"Región {region_id}: error de red; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)

    raise AssertionError("unreachable")


def load_region_ids() -> list[int]:
    if not REGIONS_SDE_DB.is_file():
        raise CaptureError(f"No existe la base de regiones de la SDE: {REGIONS_SDE_DB}")
    try:
        with sqlite3.connect(f"{REGIONS_SDE_DB.resolve().as_uri()}?mode=ro", uri=True) as connection:
            rows = connection.execute("SELECT document FROM records ORDER BY ordinal").fetchall()
        region_ids = [json.loads(row[0])["_key"] for row in rows]
    except (sqlite3.Error, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise CaptureError(f"No se pudieron leer las regiones de {REGIONS_SDE_DB}: {exc}") from exc
    if not region_ids or any(type(region_id) is not int or region_id <= 0 for region_id in region_ids):
        raise CaptureError("La SDE no contiene IDs de región válidos")
    return region_ids


def load_previous() -> dict[int, tuple[int, str, float]]:
    if not OUTPUT.is_file():
        return {}
    lines = OUTPUT.read_text(encoding="utf-8").splitlines()
    previous: dict[int, tuple[int, str, float]] = {}
    index = 0
    while index < len(lines):
        region_match = re.fullmatch(r"  (\d+):", lines[index])
        if not region_match:
            index += 1
            continue
        if index + 2 >= len(lines):
            raise CaptureError(f"{OUTPUT} está truncado; no se puede aplicar la caché")
        try:
            region_id = int(region_match.group(1))
            pages_match = re.fullmatch(r"    pages: (\d+)", lines[index + 1])
            expiry_match = re.fullmatch(r"    expires_at_utc: (.+)", lines[index + 2])
            if not pages_match or not expiry_match:
                raise ValueError("bloque YAML no reconocido")
            expiry_text = json.loads(expiry_match.group(1))
            expiry = dt.datetime.fromisoformat(expiry_text.replace("Z", "+00:00"))
            if expiry.tzinfo is None:
                raise ValueError("expires_at_utc no incluye zona horaria")
            previous[region_id] = (int(pages_match.group(1)), expiry_text, expiry.timestamp())
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CaptureError(f"{OUTPUT}, línea {index + 1}: bloque YAML no válido: {exc}") from exc
        index += 3
    return previous


def write_yaml(results: dict[int, tuple[int, str]]) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Generated from ESI /markets/{region_id}/orders/?order_type=all&page=1.",
        "# Only X-Pages is recorded; the page-1 response body is discarded.",
        "regions:",
    ]
    for region_id in sorted(results):
        pages, expires_at = results[region_id]
        lines.extend((f"  {region_id}:", f"    pages: {pages}", f"    expires_at_utc: {json.dumps(expires_at)}"))
    content = "\n".join(lines) + "\n"
    temp_path = OUTPUT.with_suffix(".yml.tmp")
    try:
        temp_path.write_text(content, encoding="utf-8", newline="\n")
        temp_path.replace(OUTPUT)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def main() -> int:
    try:
        region_ids = load_region_ids()
        previous = load_previous()
    except CaptureError as exc:
        print(f"Captura cancelada: {exc}", file=sys.stderr)
        return 1

    now = time.time()
    results = {
        region_id: (pages, expires_at)
        for region_id, (pages, expires_at, expiry_epoch) in previous.items()
        if region_id in region_ids and expiry_epoch > now
    }
    due = [region_id for region_id in region_ids if region_id not in results]
    for region_id in region_ids:
        if region_id in results:
            print(f"Región {region_id}: caché ESI vigente; se conserva {results[region_id][0]} páginas", flush=True)

    if due:
        print(
            f"Consultando página 1 de {len(due)} región(es), máximo {MAX_WORKERS} peticiones simultáneas; "
            "el cuerpo se descarta y solo se conserva X-Pages.",
            flush=True,
        )
        failures: list[tuple[int, Exception]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="esi-market-pages") as pool:
            futures = {pool.submit(fetch_page_count, region_id): region_id for region_id in due}
            for completed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                region_id = futures[future]
                try:
                    _, pages, expires_at = future.result()
                    results[region_id] = (pages, expires_at)
                except Exception as exc:
                    failures.append((region_id, exc))
                print(f"Regiones consultadas: {completed}/{len(due)}", flush=True)
        if failures:
            details = "; ".join(f"{region_id}: {error}" for region_id, error in failures[:8])
            print(f"Captura incompleta ({len(failures)} fallidas); el YAML anterior queda intacto. {details}", file=sys.stderr)
            return 1

    missing = set(region_ids) - set(results)
    if missing:
        print(f"Faltan regiones sin recuento: {sorted(missing)}", file=sys.stderr)
        return 1
    try:
        write_yaml(results)
    except OSError as exc:
        print(f"No se pudo guardar {OUTPUT}: {exc}", file=sys.stderr)
        return 1

    total_pages = sum(value[0] for value in results.values())
    print(f"Guardadas páginas de {len(results)} regiones ({total_pages:,} páginas en total) en {OUTPUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
