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
from decimal import Decimal, ROUND_HALF_UP, localcontext
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
REFERENCE_QUANTUM = Decimal("0.01")
EXECUTION_QUANTUM = Decimal("0.01")
METRIC_QUANTUM_2 = Decimal("0.01")
METRIC_QUANTUM_4 = Decimal("0.0001")
EXECUTION_PERCENTAGES = (1, 5, 10, 20, 30, 100)
DEPTH_PERCENTAGES = (1, 2, 5)


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


def filter_reference_orders(
    orders: Iterable[dict[str, Any]], buy_side: bool
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate orders and apply the existing one-pass Tukey filter once."""
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
        return [], result

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
            excluded_indexes = {
                index for index, price in enumerate(prices)
                if price < initial_mean and price < fence
            }
        else:
            excluded_indexes = {
                index for index, price in enumerate(prices)
                if price > initial_mean and price > fence
            }
        excluded = [order for index, order in enumerate(valid) if index in excluded_indexes]
        retained = [order for index, order in enumerate(valid) if index not in excluded_indexes]
        retained_prices = [Decimal(str(order["price"])) for order in retained]
        excluded_ids = {order.get("order_id") for order in excluded}
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
    return retained, result


def _calculate_reference(orders: Iterable[dict[str, Any]], buy_side: bool) -> dict[str, Any]:
    return filter_reference_orders(orders, buy_side)[1]


def calculateBuyReferencePrice(orders: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Calculate the unweighted BUY mean after removing lower Tukey outliers."""
    return _calculate_reference(orders, buy_side=True)


def calculateSellReferencePrice(orders: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Calculate the unweighted SELL mean after removing upper Tukey outliers."""
    return _calculate_reference(orders, buy_side=False)


def calculate_execution_levels(
    orders: Iterable[dict[str, Any]], buy_side: bool
) -> dict[int, dict[str, int | Decimal | None]] | None:
    """Simulate gross depth fills over valid orders."""
    valid, _metrics = filter_reference_orders(orders, buy_side)
    return _execution_levels_from_valid(valid, buy_side)


def _execution_levels_from_valid(
    valid: list[dict[str, Any]], buy_side: bool
) -> dict[int, dict[str, int | Decimal | None]] | None:
    if not valid:
        return None

    total_volume = sum(order["volume_remain"] for order in valid)
    targets = {percentage: total_volume * percentage // 100 for percentage in EXECUTION_PERCENTAGES}
    sorted_orders = sorted(
        valid,
        key=lambda order: (Decimal(str(order["price"])), order["order_id"]),
        reverse=buy_side,
    )

    results: dict[int, dict[str, int | Decimal | None]] = {}
    positive_targets = sorted((target, percentage) for percentage, target in targets.items() if target > 0)
    for percentage, target in targets.items():
        if target == 0:
            results[percentage] = {"volume": 0, "price": None}

    order_index = 0
    order_remaining = sorted_orders[0]["volume_remain"]
    executed_volume = 0
    total_isk = Decimal(0)
    with localcontext() as context:
        context.prec = 50
        for target, percentage in positive_targets:
            while executed_volume < target:
                if order_remaining == 0:
                    order_index += 1
                    order_remaining = sorted_orders[order_index]["volume_remain"]
                order = sorted_orders[order_index]
                quantity = min(order_remaining, target - executed_volume)
                total_isk += Decimal(str(order["price"])) * quantity
                executed_volume += quantity
                order_remaining -= quantity
            average_price = (total_isk / target).quantize(EXECUTION_QUANTUM, rounding=ROUND_HALF_UP)
            results[percentage] = {"volume": target, "price": average_price}

    return {percentage: results[percentage] for percentage in EXECUTION_PERCENTAGES}


def calculate_reference_depth(
    orders: Iterable[dict[str, Any]], buy_side: bool, reference_price: Decimal | None
) -> dict[str, int | Decimal | None] | None:
    """Return eligible station depth and VWAP at or better than a reference price."""
    valid, _metrics = filter_reference_orders(orders, buy_side)
    return _reference_depth_from_valid(valid, buy_side, reference_price)


def _reference_depth_from_valid(
    valid: list[dict[str, Any]], buy_side: bool, reference_price: Decimal | None
) -> dict[str, int | Decimal | None] | None:
    if not valid:
        return None
    if reference_price is None:
        return {"volume": 0, "price": None}

    cutoff = Decimal(str(reference_price))
    eligible = [
        order
        for order in valid
        if (Decimal(str(order["price"])) >= cutoff if buy_side else Decimal(str(order["price"])) <= cutoff)
    ]
    volume = sum(order["volume_remain"] for order in eligible)
    if volume == 0:
        return {"volume": 0, "price": None}
    with localcontext() as context:
        context.prec = 50
        gross_isk = sum(
            (Decimal(str(order["price"])) * order["volume_remain"] for order in eligible),
            Decimal(0),
        )
        price = (gross_isk / volume).quantize(EXECUTION_QUANTUM, rounding=ROUND_HALF_UP)
    return {"volume": volume, "price": price}


def _quantize_metric(value: Decimal | None, quantum: Decimal) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = max(50, len(value.as_tuple().digits) + 8)
        return value.quantize(quantum, rounding=ROUND_HALF_UP)


def _market_depth(
    orders: list[dict[str, Any]], best_price: Decimal | None, buy_side: bool
) -> dict[int, dict[str, int | None]] | None:
    if not orders or best_price is None:
        return None
    results = {}
    for percentage in DEPTH_PERCENTAGES:
        fraction = Decimal(percentage) / Decimal(100)
        cutoff = best_price * (Decimal(1) - fraction if buy_side else Decimal(1) + fraction)
        eligible = [
            order for order in orders
            if (Decimal(str(order["price"])) >= cutoff if buy_side else Decimal(str(order["price"])) <= cutoff)
        ]
        results[percentage] = {
            "volume": sum(order["volume_remain"] for order in eligible),
            "orders": len(eligible),
        }
    return results


def _market_liquidity(orders: list[dict[str, Any]]) -> dict[str, int | Decimal]:
    volume = sum(order["volume_remain"] for order in orders)
    with localcontext() as context:
        context.prec = 50
        value_isk = sum(
            (Decimal(str(order["price"])) * order["volume_remain"] for order in orders),
            Decimal(0),
        )
    return {
        "orders": len(orders),
        "volume": volume,
        "value_isk": _quantize_metric(value_isk, METRIC_QUANTUM_2) or Decimal("0.00"),
    }


def _market_distribution(orders: list[dict[str, Any]]) -> dict[str, Decimal | None]:
    if not orders:
        return {"median": None, "p10": None, "p90": None, "top_5_volume_pct": None}
    prices = sorted(Decimal(str(order["price"])) for order in orders)
    total_volume = sum(order["volume_remain"] for order in orders)
    top_volume = sum(sorted((order["volume_remain"] for order in orders), reverse=True)[:5])
    concentration = Decimal(top_volume) * Decimal(100) / Decimal(total_volume)
    return {
        "median": _quantize_metric(_quantile(prices, Decimal("0.5")), METRIC_QUANTUM_2),
        "p10": _quantize_metric(_quantile(prices, Decimal("0.1")), METRIC_QUANTUM_2),
        "p90": _quantize_metric(_quantile(prices, Decimal("0.9")), METRIC_QUANTUM_2),
        "top_5_volume_pct": _quantize_metric(concentration, METRIC_QUANTUM_4),
    }


def calculate_market_indicators(
    filtered_buy: list[dict[str, Any]], filtered_sell: list[dict[str, Any]]
) -> dict[str, Any]:
    """Calculate all secondary indicators from already-filtered order lists."""
    buy_prices = [Decimal(str(order["price"])) for order in filtered_buy]
    sell_prices = [Decimal(str(order["price"])) for order in filtered_sell]
    best_bid = max(buy_prices) if buy_prices else None
    best_ask = min(sell_prices) if sell_prices else None

    if best_bid is None or best_ask is None:
        spread = {"absolute": None, "percentage": None}
    else:
        absolute = best_ask - best_bid
        mid_price = (best_ask + best_bid) / Decimal(2)
        spread = {
            "absolute": _quantize_metric(absolute, METRIC_QUANTUM_4),
            "percentage": _quantize_metric(absolute / mid_price * Decimal(100), METRIC_QUANTUM_4),
        }

    buy_depth = _market_depth(filtered_buy, best_bid, buy_side=True)
    sell_depth = _market_depth(filtered_sell, best_ask, buy_side=False)
    if buy_depth is None or sell_depth is None:
        imbalance = None
    else:
        buy_volume = buy_depth[5]["volume"]
        sell_volume = sell_depth[5]["volume"]
        denominator = buy_volume + sell_volume
        imbalance = (
            _quantize_metric(Decimal(buy_volume - sell_volume) / Decimal(denominator), METRIC_QUANTUM_4)
            if denominator else None
        )

    return {
        "best_bid": _quantize_metric(best_bid, METRIC_QUANTUM_2),
        "best_ask": _quantize_metric(best_ask, METRIC_QUANTUM_2),
        "spread": spread,
        "buy_depth": buy_depth,
        "sell_depth": sell_depth,
        "buy_liquidity": _market_liquidity(filtered_buy),
        "sell_liquidity": _market_liquidity(filtered_sell),
        "buy_distribution": _market_distribution(filtered_buy),
        "sell_distribution": _market_distribution(filtered_sell),
        "market_balance": {"imbalance_5pct": imbalance},
    }


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
        filtered_buy, buy_result = filter_reference_orders(buy_by_type.get(type_id, []), buy_side=True)
        filtered_sell, sell_result = filter_reference_orders(sell_by_type.get(type_id, []), buy_side=False)
        buy_cutoff = buy_result["final_mean"]
        sell_cutoff = sell_result["final_mean"]
        indicators = calculate_market_indicators(filtered_buy, filtered_sell)
        rows.append({
            "type_id": type_id,
            "buy": buy_result["final_mean"],
            "sell": sell_result["final_mean"],
            "best_bid": indicators["best_bid"],
            "best_ask": indicators["best_ask"],
            "spread": indicators["spread"],
            "buy_execution": _execution_levels_from_valid(filtered_buy, buy_side=True),
            "sell_execution": _execution_levels_from_valid(filtered_sell, buy_side=False),
            "buy_at_reference": _reference_depth_from_valid(
                filtered_buy, buy_side=True, reference_price=buy_cutoff
            ),
            "sell_at_reference": _reference_depth_from_valid(
                filtered_sell, buy_side=False, reference_price=sell_cutoff
            ),
            "buy_depth": indicators["buy_depth"],
            "sell_depth": indicators["sell_depth"],
            "buy_liquidity": indicators["buy_liquidity"],
            "sell_liquidity": indicators["sell_liquidity"],
            "buy_distribution": indicators["buy_distribution"],
            "sell_distribution": indicators["sell_distribution"],
            "market_balance": indicators["market_balance"],
        })
    return rows


def round_reference_price(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = max(50, len(value.as_tuple().digits) + 8)
        return value.quantize(REFERENCE_QUANTUM, rounding=ROUND_HALF_UP)


def _yaml_decimal(value: Decimal | None) -> str:
    rounded = round_reference_price(value)
    return "null" if rounded is None else format(rounded, ".2f")


def _yaml_execution_price(value: Decimal | None) -> str:
    return "null" if value is None else format(value, ".2f")


def _yaml_metric(value: Decimal | int | None, places: int) -> str:
    if value is None:
        return "null"
    return format(value, f".{places}f") if isinstance(value, Decimal) else str(value)


def write_price_yaml(price_rows: list[dict[str, Any]], path: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            for row in price_rows:
                stream.write(f"- type_id: {row['type_id']}\n")
                stream.write(f"  buy: {_yaml_decimal(row['buy'])}\n")
                stream.write(f"  sell: {_yaml_decimal(row['sell'])}\n")
                stream.write(f"  best_bid: {_yaml_metric(row['best_bid'], 2)}\n")
                stream.write(f"  best_ask: {_yaml_metric(row['best_ask'], 2)}\n")
                stream.write("  spread:\n")
                stream.write(f"    absolute: {_yaml_metric(row['spread']['absolute'], 4)}\n")
                stream.write(f"    percentage: {_yaml_metric(row['spread']['percentage'], 4)}\n")
                for field in ("buy_execution", "sell_execution"):
                    levels = row[field]
                    if levels is None:
                        stream.write(f"  {field}: null\n")
                        continue
                    stream.write(f"  {field}:\n")
                    for percentage in EXECUTION_PERCENTAGES:
                        level = levels[percentage]
                        stream.write(f"    {percentage}:\n")
                        stream.write(f"      volume: {level['volume']}\n")
                        stream.write(f"      price: {_yaml_execution_price(level['price'])}\n")
                for field in ("buy_at_reference", "sell_at_reference"):
                    level = row[field]
                    if level is None:
                        stream.write(f"  {field}: null\n")
                    else:
                        stream.write(f"  {field}:\n")
                        stream.write(f"    volume: {level['volume']}\n")
                        stream.write(f"    price: {_yaml_execution_price(level['price'])}\n")
                for field in ("buy_depth", "sell_depth"):
                    depth = row[field]
                    if depth is None:
                        stream.write(f"  {field}: null\n")
                        continue
                    stream.write(f"  {field}:\n")
                    for percentage in DEPTH_PERCENTAGES:
                        level = depth[percentage]
                        stream.write(f"    {percentage}:\n")
                        stream.write(f"      volume: {_yaml_metric(level['volume'], 0)}\n")
                        stream.write(f"      orders: {_yaml_metric(level['orders'], 0)}\n")
                for field in ("buy_liquidity", "sell_liquidity"):
                    liquidity = row[field]
                    stream.write(f"  {field}:\n")
                    stream.write(f"    orders: {_yaml_metric(liquidity['orders'], 0)}\n")
                    stream.write(f"    volume: {_yaml_metric(liquidity['volume'], 0)}\n")
                    stream.write(f"    value_isk: {_yaml_metric(liquidity['value_isk'], 2)}\n")
                for field in ("buy_distribution", "sell_distribution"):
                    distribution = row[field]
                    stream.write(f"  {field}:\n")
                    for key in ("median", "p10", "p90"):
                        stream.write(f"    {key}: {_yaml_metric(distribution[key], 2)}\n")
                    stream.write(f"    top_5_volume_pct: {_yaml_metric(distribution['top_5_volume_pct'], 4)}\n")
                stream.write("  market_balance:\n")
                stream.write(
                    f"    imbalance_5pct: {_yaml_metric(row['market_balance']['imbalance_5pct'], 4)}\n"
                )
            if not price_rows:
                stream.write("[]\n")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def load_order_jsonl(path: Path) -> list[dict[str, Any]]:
    orders = []
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                order = json.loads(line)
                if not isinstance(order, dict):
                    raise CaptureError(f"{path}, línea {line_number}: se esperaba un objeto JSON")
                orders.append(order)
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError(f"No se pudo leer {path}: {exc}") from exc
    return orders


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
        write_price_yaml(price_rows, temporary_paths[0])

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
    parser.add_argument("--from-jsonl", action="store_true", help="Recalcula el YAML usando los JSONL existentes, sin consultar ESI")
    args = parser.parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    if args.from_jsonl:
        try:
            buy_orders = load_order_jsonl(output_dir / BUY_JSONL)
            sell_orders = load_order_jsonl(output_dir / SELL_JSONL)
            price_rows = build_prices(buy_orders, sell_orders)
            write_price_yaml(price_rows, output_dir / PRICES_YAML)
            print(f"Recalculado {PRICES_YAML} desde los JSONL existentes; no se consultó ESI.", flush=True)
            return 0
        except (CaptureError, OSError, ValueError) as exc:
            print(f"Recalculo fallido: {exc}", file=sys.stderr)
            return 1
    if not 1 <= args.workers <= MAX_WORKERS:
        print(f"--workers debe estar entre 1 y {MAX_WORKERS}", file=sys.stderr)
        return 2
    try:
        buy_orders, sell_orders, page_count = collect_station_orders(args.workers)
        outputs = write_outputs(buy_orders, sell_orders, output_dir)
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
