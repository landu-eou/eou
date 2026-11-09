"""Capture public ESI market orders into one compact SQLite file per region.

Run manually from the repository root:
    python scripts/capture_market_orders.py
    python scripts/capture_market_orders.py --region 10000002 --workers 4
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
import random
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ESI = "https://esi.evetech.net/latest"
USER_AGENT = "EVE-Route-Intel/0.3 (+https://github.com/landu-eou/eou)"
COMPATIBILITY_DATE = "2026-11-09"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
MAX_WORKERS = 16
DEFAULT_WORKERS = 6
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "esi" / "mkt"


class CaptureError(RuntimeError):
    """A region cannot be saved as a complete, consistent capture."""


class PageRequestError(CaptureError):
    """A page failed after retrying transient errors."""


class SnapshotError(CaptureError):
    """Page metadata or duplicated order values show snapshot drift."""


def _retry_delay(headers: Any, attempt: int, status: int | None = None) -> float:
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


def fetch_json(url: str) -> tuple[Any, dict[str, str]]:
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
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read())
                return payload, {key.lower(): value for key, value in response.headers.items()}
        except urllib.error.HTTPError as exc:
            status = exc.code
            headers = exc.headers
            retryable = status in (420, 429) or 500 <= status <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise PageRequestError(f"ESI respondió HTTP {status} para {url}: {detail}") from exc
            delay = _retry_delay(headers, attempt, status)
            print(f"  ESI HTTP {status}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise PageRequestError(f"Error de red para {url}: {exc}") from exc
            delay = min(30.0, (2**attempt) + random.random())
            print(f"  Error de red; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            if attempt >= MAX_RETRIES:
                raise PageRequestError(f"ESI devolvió JSON inválido para {url}") from exc
            delay = min(30.0, (2**attempt) + random.random())
            time.sleep(delay)

    raise AssertionError("unreachable")


def fetch_region_ids() -> list[int]:
    payload, _ = fetch_json(f"{ESI}/universe/regions/")
    if not isinstance(payload, list) or not payload:
        raise CaptureError("ESI no devolvió una lista de regiones válida")
    try:
        regions = sorted({int(region_id) for region_id in payload})
    except (TypeError, ValueError) as exc:
        raise CaptureError("La lista de regiones contiene un ID inválido") from exc
    if any(region_id <= 0 for region_id in regions):
        raise CaptureError("La lista de regiones contiene un ID no positivo")
    return regions


def _orders_url(region_id: int, page: int) -> str:
    query = urllib.parse.urlencode({"order_type": "all", "page": page})
    return f"{ESI}/markets/{region_id}/orders/?{query}"


def fetch_page(region_id: int, page: int) -> tuple[list[dict[str, Any]], dict[str, str]]:
    payload, headers = fetch_json(_orders_url(region_id, page))
    if not isinstance(payload, list):
        raise PageRequestError(f"Región {region_id}, página {page}: se esperaba una lista")
    try:
        page_count = int(headers.get("x-pages", "1"))
    except ValueError as exc:
        raise PageRequestError(f"Región {region_id}, página {page}: encabezado X-Pages inválido") from exc
    if page_count < 1:
        raise PageRequestError(f"Región {region_id}, página {page}: X-Pages={page_count} no es válido")
    if page > page_count:
        raise SnapshotError(f"Región {region_id}: X-Pages cambió durante la captura")
    headers["parsed-x-pages"] = str(page_count)
    return payload, headers


def _integer(record: dict[str, Any], key: str, order_id: int) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CaptureError(f"La orden {order_id} tiene {key} inválido")
    return value


def normalize_order(record: dict[str, Any], region_id: int) -> tuple[Any, ...]:
    try:
        order_id = _integer(record, "order_id", 0)
        type_id = _integer(record, "type_id", order_id)
        location_id = _integer(record, "location_id", order_id)
        system_id = _integer(record, "system_id", order_id)
        volume_remain = _integer(record, "volume_remain", order_id)
        volume_total = _integer(record, "volume_total", order_id)
        min_volume = _integer(record, "min_volume", order_id)
        duration = _integer(record, "duration", order_id)
        is_buy_order = record.get("is_buy_order")
        if not isinstance(is_buy_order, bool):
            raise CaptureError(f"La orden {order_id} tiene is_buy_order inválido")
        price = record.get("price")
        if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price):
            raise CaptureError(f"La orden {order_id} tiene price inválido")
        issued_text = record.get("issued")
        if not isinstance(issued_text, str):
            raise CaptureError(f"La orden {order_id} tiene issued inválido")
        issued = dt.datetime.fromisoformat(issued_text.replace("Z", "+00:00"))
        if issued.tzinfo is None:
            issued = issued.replace(tzinfo=dt.timezone.utc)
        issued_epoch = int(issued.timestamp())
        raw_range = record.get("range")
        if not isinstance(raw_range, str):
            raise CaptureError(f"La orden {order_id} tiene range inválido")
        order_range = encode_range(raw_range, order_id)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CaptureError(f"Región {region_id}: orden con campos inválidos: {exc}") from exc

    if order_id <= 0 or min(type_id, location_id, system_id, volume_remain, volume_total, min_volume, duration) < 0:
        raise CaptureError(f"La orden {order_id} tiene un valor numérico fuera de rango")

    return (
        order_id,
        type_id,
        location_id,
        system_id,
        float(price),
        volume_remain,
        volume_total,
        min_volume,
        int(is_buy_order),
        issued_epoch,
        duration,
        order_range,
    )


def encode_range(value: str, order_id: int) -> int:
    """Compact ESI range strings: 0=station, 1=solarsystem, 2=region, 3+n=distance n."""
    named_ranges = {"station": 0, "solarsystem": 1, "region": 2}
    if value in named_ranges:
        return named_ranges[value]
    try:
        distance = int(value)
    except ValueError as exc:
        raise CaptureError(f"La orden {order_id} tiene range desconocido") from exc
    if str(distance) != value or not 0 <= distance <= 32764:
        raise CaptureError(f"La orden {order_id} tiene range fuera de rango")
    return distance + 3


def collect_region(region_id: int, workers: int) -> tuple[dict[int, tuple[Any, ...]], dict[str, Any]]:
    first_orders, first_headers = fetch_page(region_id, 1)
    page_count = int(first_headers["parsed-x-pages"])
    last_modified = first_headers.get("last-modified")
    expires = first_headers.get("expires")

    if expires:
        try:
            expiry = dt.datetime.strptime(expires, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=dt.timezone.utc)
            remaining = (expiry - dt.datetime.now(dt.timezone.utc)).total_seconds()
            if remaining <= 5:
                delay = max(0.0, remaining) + 1.0
                print(f"  Página 1 cerca de caducar; esperando {delay:.1f} s y reiniciando región", flush=True)
                time.sleep(delay)
                raise SnapshotError(f"Región {region_id}: la caché estaba a punto de caducar al iniciar")
        except ValueError:
            print(f"  Región {region_id}: encabezado Expires no interpretable; continúo validando Last-Modified", file=sys.stderr)

    pages: dict[int, list[dict[str, Any]]] = {1: first_orders}
    page_headers: dict[int, dict[str, str]] = {1: first_headers}
    failures: list[tuple[int, BaseException]] = []
    if page_count > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"esi-{region_id}") as pool:
            futures = {pool.submit(fetch_page, region_id, page): page for page in range(2, page_count + 1)}
            completed = 1
            for future in concurrent.futures.as_completed(futures):
                page = futures[future]
                try:
                    payload, headers = future.result()
                    pages[page] = payload
                    page_headers[page] = headers
                except Exception as exc:
                    failures.append((page, exc))
                completed += 1
                print(f"  Región {region_id}: páginas {completed}/{page_count} finalizadas", flush=True)

    if failures:
        details = "; ".join(f"página {page}: {error}" for page, error in failures[:5])
        raise CaptureError(f"Región {region_id}: fallaron {len(failures)} página(s); {details}")

    for page in range(1, page_count + 1):
        headers = page_headers[page]
        if int(headers["parsed-x-pages"]) != page_count:
            raise SnapshotError(f"Región {region_id}: X-Pages cambió durante la captura")
        current_modified = headers.get("last-modified")
        if last_modified is not None and current_modified != last_modified:
            raise SnapshotError(f"Región {region_id}: Last-Modified cambió en la página {page}")

    unique_orders: dict[int, tuple[Any, ...]] = {}
    for page in range(1, page_count + 1):
        for item in pages[page]:
            if not isinstance(item, dict):
                raise CaptureError(f"Región {region_id}, página {page}: registro no válido")
            row = normalize_order(item, region_id)
            order_id = row[0]
            previous = unique_orders.get(order_id)
            if previous is not None and previous != row:
                raise SnapshotError(f"Región {region_id}: orden {order_id} aparece con valores distintos entre páginas")
            unique_orders[order_id] = row

    if last_modified is None and page_count > 1:
        print(f"  Región {region_id}: ESI no devolvió Last-Modified; la captura no puede verificar versión entre páginas", file=sys.stderr)
    return unique_orders, {
        "page_count": page_count,
        "last_modified": last_modified,
        "expires": expires,
    }


def write_region_database(region_id: int, records: dict[int, tuple[Any, ...]], metadata: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"{region_id}.db"
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{region_id}.", suffix=".tmp", dir=output_dir, delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)

        connection = sqlite3.connect(temp_path)
        try:
            connection.execute("PRAGMA page_size=4096")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """CREATE TABLE orders (
                    order_id INTEGER PRIMARY KEY,
                    type_id INTEGER NOT NULL,
                    location_id INTEGER NOT NULL,
                    system_id INTEGER NOT NULL,
                    price REAL NOT NULL,
                    volume_remain INTEGER NOT NULL,
                    volume_total INTEGER NOT NULL,
                    min_volume INTEGER NOT NULL,
                    is_buy_order INTEGER NOT NULL,
                    issued INTEGER NOT NULL,
                    duration INTEGER NOT NULL,
                    range_code INTEGER NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE capture (
                    region_id INTEGER NOT NULL,
                    captured_at INTEGER NOT NULL,
                    page_count INTEGER NOT NULL,
                    order_count INTEGER NOT NULL,
                    last_modified TEXT,
                    expires TEXT
                )"""
            )
            connection.executemany(
                """INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                records.values(),
            )
            connection.execute(
                "CREATE INDEX orders_by_type_side_price ON orders(type_id, is_buy_order, price)"
            )
            connection.execute(
                "INSERT INTO capture VALUES (?, ?, ?, ?, ?, ?)",
                (
                    region_id,
                    int(time.time()),
                    metadata["page_count"],
                    len(records),
                    metadata["last_modified"],
                    metadata["expires"],
                ),
            )
            connection.commit()
            connection.execute("VACUUM")
        finally:
            connection.close()

        os.replace(temp_path, final_path)
        return final_path
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def capture_one_region(region_id: int, workers: int, output_dir: Path) -> tuple[int, int, Path]:
    for attempt in range(1, 4):
        try:
            print(f"Región {region_id}: descargando páginas (intento {attempt}/3)", flush=True)
            records, metadata = collect_region(region_id, workers)
            path = write_region_database(region_id, records, metadata, output_dir)
            return metadata["page_count"], len(records), path
        except SnapshotError:
            if attempt >= 3:
                raise
            delay = 2 + random.random() * 2
            print(f"  Paginación inconsistente; descarto la ronda y reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def cache_expiry(path: Path) -> float | None:
    if not path.is_file():
        return None
    try:
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            row = connection.execute("SELECT expires FROM capture LIMIT 1").fetchone()
        if not row or not row[0]:
            return None
        expiry = dt.datetime.strptime(row[0], "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=dt.timezone.utc)
        return expiry.timestamp()
    except (OSError, sqlite3.Error, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Descarga manualmente órdenes públicas ESI a una base SQLite por región.")
    parser.add_argument(
        "--region",
        type=int,
        action="append",
        help="ID regional a capturar; se puede repetir. Si se omite, captura todas las regiones de ESI.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Páginas concurrentes por región (1-{MAX_WORKERS}; predeterminado: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Directorio de salida (predeterminado: {DEFAULT_OUTPUT}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        print(f"--workers debe estar entre 1 y {MAX_WORKERS}", file=sys.stderr)
        return 2
    output_dir = args.output.expanduser().resolve()

    try:
        region_ids = sorted(set(args.region)) if args.region else fetch_region_ids()
    except CaptureError as exc:
        print(f"No se pudo obtener la lista de regiones: {exc}", file=sys.stderr)
        return 1
    if not region_ids or any(region_id <= 0 for region_id in region_ids):
        print("La lista de regiones está vacía o contiene IDs no válidos", file=sys.stderr)
        return 2

    print(f"Captura manual: {len(region_ids)} región(es), concurrencia {args.workers}; destino {output_dir}", flush=True)
    failures: list[tuple[int, str]] = []
    total_orders = 0
    total_pages = 0
    for index, region_id in enumerate(region_ids, start=1):
        print(f"[{index}/{len(region_ids)}]", flush=True)
        final_path = output_dir / f"{region_id}.db"
        expires_at = cache_expiry(final_path)
        if expires_at is not None and expires_at > time.time():
            remaining = int(expires_at - time.time())
            print(f"  Omitida {region_id}.db: ESI indica que la caché sigue vigente ({remaining} s)", flush=True)
            continue
        try:
            page_count, order_count, path = capture_one_region(region_id, args.workers, output_dir)
            total_pages += page_count
            total_orders += order_count
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  Guardada {path.name}: {order_count:,} órdenes, {page_count} páginas, {size_mb:.1f} MiB", flush=True)
        except Exception as exc:
            failures.append((region_id, str(exc)))
            print(f"  ERROR en región {region_id}: {exc}", file=sys.stderr, flush=True)

    print(f"Resumen: {len(region_ids) - len(failures)}/{len(region_ids)} regiones, {total_pages:,} páginas, {total_orders:,} órdenes", flush=True)
    if failures:
        print("Regiones fallidas (reintenta pasando --region ID):", file=sys.stderr)
        for region_id, error in failures:
            print(f"  {region_id}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
