"""Capture Jita 4-4 market orders and calculate buy/sell reference prices."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import random
import sqlite3
import sys
import time
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any, Iterable

from capture_market_orders import (
    DEFAULT_WORKERS,
    MAX_WORKERS,
    CaptureError,
    SnapshotError,
    fetch_page,
    normalize_order,
)


REGION_ID = 10000002
STATION_ID = 60003760
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "test"
BUY_JSONL = "jita4-4_buy_orders.jsonl"
SELL_JSONL = "jita4-4_sell_orders.jsonl"
PRICES_YAML = "jita4-4_market_prices.yml"


def _valid_side_orders(orders: Iterable[dict[str, Any]], buy_side: bool) -> tuple[int, list[dict[str, Any]]]:
    materialized = list(orders)
    valid = []
    for order in materialized:
        if not isinstance(order, dict):
            continue
        volume = order.get("volume_remain")
        price = order.get("price")
        if order.get("location_id") != STATION_ID or order.get("is_buy_order") is not buy_side:
            continue
        if isinstance(volume, bool) or not isinstance(volume, int) or volume <= 0:
            continue
        if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
            continue
        valid.append(order)

    type_ids = {order.get("type_id") for order in valid}
    if len(type_ids) > 1:
        raise ValueError("El cálculo debe recibir órdenes de un solo type_id")
    return len(materialized), valid


def _quantile(sorted_prices: list[Decimal], probability: Decimal) -> Decimal:
    position = Decimal(len(sorted_prices) - 1) * probability
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_prices) - 1)
    fraction = position - lower_index
    return sorted_prices[lower_index] + (sorted_prices[upper_index] - sorted_prices[lower_index]) * fraction


def _calculate_reference(orders: Iterable[dict[str, Any]], buy_side: bool) -> dict[str, Any]:
    total_orders, valid = _valid_side_orders(orders, buy_side)
    result: dict[str, Any] = {
        "initial_mean": None,
        "final_mean": None,
        "q1": None,
        "q3": None,
        "iqr": None,
        "lower_fence": None,
        "upper_fence": None,
        "total_orders": total_orders,
        "valid_orders": len(valid),
        "excluded_orders": 0,
        "excluded_order_ids": [],
        "retained_orders": 0,
    }
    if not valid:
        return result

    with localcontext() as context:
        context.prec = 50
        prices = [Decimal(str(order["price"])) for order in valid]
        sorted_prices = sorted(prices)
        initial_mean = sum(prices, Decimal(0)) / len(prices)
        q1 = _quantile(sorted_prices, Decimal("0.25"))
        q3 = _quantile(sorted_prices, Decimal("0.75"))
        iqr = q3 - q1
        fence = q1 - Decimal("1.5") * iqr if buy_side else q3 + Decimal("1.5") * iqr
        if buy_side:
            excluded = [
                order for order in valid
                if Decimal(str(order["price"])) < initial_mean
                and Decimal(str(order["price"])) < fence
            ]
        else:
            excluded = [
                order for order in valid
                if Decimal(str(order["price"])) > initial_mean
                and Decimal(str(order["price"])) > fence
            ]

        excluded_ids = {order.get("order_id") for order in excluded}
        retained_prices = [
            Decimal(str(order["price"])) for order in valid if order.get("order_id") not in excluded_ids
        ]
        result.update(
            initial_mean=initial_mean,
            final_mean=sum(retained_prices, Decimal(0)) / len(retained_prices) if retained_prices else None,
            q1=q1,
            q3=q3,
            iqr=iqr,
            lower_fence=fence if buy_side else None,
            upper_fence=fence if not buy_side else None,
            excluded_orders=len(excluded),
            excluded_order_ids=sorted(item for item in excluded_ids if item is not None),
            retained_orders=len(retained_prices),
        )
    return result


def calculateBuyReferencePrice(orders: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Calculate the unweighted BUY mean after removing lower Tukey outliers."""
    return _calculate_reference(orders, buy_side=True)


def calculateSellReferencePrice(orders: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Calculate the unweighted SELL mean after removing upper Tukey outliers."""
    return _calculate_reference(orders, buy_side=False)


def _collect_once(workers: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    first_orders, first_headers = fetch_page(REGION_ID, 1)
    page_count = int(first_headers["parsed-x-pages"])
    last_modified = first_headers.get("last-modified")
    expires = first_headers.get("expires")
    if expires:
        try:
            expiry = dt.datetime.strptime(expires, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=dt.timezone.utc)
            if (expiry - dt.datetime.now(dt.timezone.utc)).total_seconds() <= 5:
                raise SnapshotError(f"La caché de ESI caduca al iniciar la captura ({expires})")
        except ValueError:
            print("ESI devolvió un encabezado Expires no interpretable; validaré Last-Modified.", file=sys.stderr)

    unique: dict[int, tuple[tuple[Any, ...], dict[str, Any]]] = {}

    def retain_station_orders(page_orders: list[dict[str, Any]], page: int) -> None:
        for record in page_orders:
            if not isinstance(record, dict):
                raise CaptureError(f"Página {page}: ESI devolvió un registro que no es objeto")
            if record.get("location_id") != STATION_ID:
                continue
            normalized = normalize_order(record, REGION_ID)
            order_id = normalized[0]
            previous = unique.get(order_id)
            if previous is not None and (previous[0] != normalized or previous[1] != record):
                raise SnapshotError(f"La orden {order_id} aparece con datos distintos en varias páginas")
            unique[order_id] = (normalized, record)

    retain_station_orders(first_orders, 1)
    headers_by_page = {1: first_headers}
    failures: list[tuple[int, BaseException]] = []
    if page_count > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="jita-market-pages") as pool:
            futures = {pool.submit(fetch_page, REGION_ID, page): page for page in range(2, page_count + 1)}
            for completed, future in enumerate(concurrent.futures.as_completed(futures), start=2):
                page = futures[future]
                try:
                    page_orders, headers = future.result()
                    headers_by_page[page] = headers
                    retain_station_orders(page_orders, page)
                except Exception as exc:
                    failures.append((page, exc))
                if completed % 25 == 0 or completed == page_count:
                    print(f"Páginas regionales completadas: {completed}/{page_count}", flush=True)
    if failures:
        details = "; ".join(f"página {page}: {error}" for page, error in failures[:6])
        if any(isinstance(error, SnapshotError) for _page, error in failures):
            raise SnapshotError(f"La paginación cambió durante la captura: {details}")
        raise CaptureError(f"Fallaron {len(failures)} páginas de ESI: {details}")

    for page in range(1, page_count + 1):
        headers = headers_by_page[page]
        if int(headers["parsed-x-pages"]) != page_count:
            raise SnapshotError(f"X-Pages cambió mientras se leía la página {page}")
        if last_modified is not None and headers.get("last-modified") != last_modified:
            raise SnapshotError(f"Last-Modified cambió mientras se leía la página {page}")
    if last_modified is None and page_count > 1:
        print("ESI no devolvió Last-Modified; no se pudo verificar la versión entre páginas.", file=sys.stderr)

    buy = []
    sell = []
    for _normalized, order in sorted(unique.values(), key=lambda item: item[0][0]):
        (buy if order["is_buy_order"] else sell).append(order)
    return buy, sell, page_count


def collect_station_orders(workers: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    for attempt in range(1, 4):
        try:
            print(f"Capturando órdenes de {REGION_ID}, estación {STATION_ID} (intento {attempt}/3)", flush=True)
            return _collect_once(workers)
        except SnapshotError:
            if attempt == 3:
                raise
            delay = 1.0 + random.random() * 2
            print(f"Snapshot regional inconsistente; se reintenta en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def build_prices(buy_orders: list[dict[str, Any]], sell_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buy_by_type: dict[int, list[dict[str, Any]]] = {}
    sell_by_type: dict[int, list[dict[str, Any]]] = {}
    for order in buy_orders:
        buy_by_type.setdefault(order["type_id"], []).append(order)
    for order in sell_orders:
        sell_by_type.setdefault(order["type_id"], []).append(order)

    rows = []
    for type_id in sorted(buy_by_type.keys() | sell_by_type.keys()):
        buy_result = calculateBuyReferencePrice(buy_by_type.get(type_id, []))
        sell_result = calculateSellReferencePrice(sell_by_type.get(type_id, []))
        rows.append({
            "type_id": type_id,
            "buy": buy_result["final_mean"],
            "sell": sell_result["final_mean"],
        })
    return rows


def _yaml_decimal(value: Decimal | None) -> str:
    if value is None:
        return "null"
    formatted = format(value, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


def write_outputs(
    buy_orders: list[dict[str, Any]],
    sell_orders: list[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = (
        output_dir / PRICES_YAML,
        output_dir / BUY_JSONL,
        output_dir / SELL_JSONL,
    )
    temporary_paths = tuple(path.with_name(f".{path.name}.tmp") for path in outputs)
    try:
        price_rows = build_prices(buy_orders, sell_orders)
        with temporary_paths[0].open("w", encoding="utf-8", newline="\n") as stream:
            for row in price_rows:
                stream.write(f"- type_id: {row['type_id']}\n")
                stream.write(f"  buy: {_yaml_decimal(row['buy'])}\n")
                stream.write(f"  sell: {_yaml_decimal(row['sell'])}\n")
        if not price_rows:
            temporary_paths[0].write_text("[]\n", encoding="utf-8", newline="\n")

        for path, orders in ((temporary_paths[1], buy_orders), (temporary_paths[2], sell_orders)):
            with path.open("w", encoding="utf-8", newline="\n") as stream:
                for order in orders:
                    stream.write(json.dumps(order, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
                    stream.write("\n")

        for temporary, final in zip(temporary_paths, outputs, strict=True):
            temporary.replace(final)
        return outputs
    except Exception:
        for path in temporary_paths:
            path.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Captura órdenes de Jita 4-4 y calcula precios de referencia")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Páginas en paralelo (1-{MAX_WORKERS})")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        print(f"--workers debe estar entre 1 y {MAX_WORKERS}", file=sys.stderr)
        return 2
    try:
        buy_orders, sell_orders, page_count = collect_station_orders(args.workers)
        outputs = write_outputs(buy_orders, sell_orders, args.output_dir.expanduser().resolve())
    except (CaptureError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"Extracción fallida: {exc}", file=sys.stderr)
        return 1

    print(
        f"Leídas {page_count} páginas; guardadas {len(buy_orders):,} órdenes BUY y "
        f"{len(sell_orders):,} SELL; {len(build_prices(buy_orders, sell_orders)):,} type_id.",
        flush=True,
    )
    for path in outputs:
        print(f"Creado: {path} ({path.stat().st_size:,} bytes)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
