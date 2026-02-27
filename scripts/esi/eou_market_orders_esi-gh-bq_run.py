#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EOU — Market Orders (ESI → GH)
- Ingesta ESI por regiones + estructuras con workers paralelos.
- Dedupe LOCAL por worker (order_id).
- Cache pages: states/market_orders_pages.json (solo si run exitoso).
- Outputs:
  - data/esi/hubs.json  (pretty)
  - data/esi/Orders.jsonl (VWAP por type_id, buy/sell separados)
"""

from __future__ import annotations

import concurrent.futures as cf
import gzip
import json
import math
import os
import sys
import time
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


# -----------------------------
# Utilidades básicas
# -----------------------------

def now_epoch() -> float:
    return time.time()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def read_env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    return int(v)

def read_env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def atomic_write_text(path: str, text: str) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    os.replace(tmp, path)

def atomic_write_json(path: str, obj: Any, indent: int = 2) -> None:
    text = json.dumps(obj, ensure_ascii=False, indent=indent) + "\n"
    atomic_write_text(path, text)

def atomic_write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    """
    JSON Lines (jsonl): 1 objeto JSON por línea, UTF-8. :contentReference[oaicite:2]{index=2}
    """
    ensure_dir(os.path.dirname(path) or ".")
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(tmp, path)

def read_json_gz_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def human_secs(s: float) -> str:
    return f"{s:,.2f}s"

def fmt4(x: float) -> float:
    return float(f"{x:.4f}")

def fmt2(x: float) -> float:
    return float(f"{x:.2f}")


# -----------------------------
# Logging (compacto, visual)
# -----------------------------

T0 = now_epoch()
LOG_LOCK = threading.Lock()

def log(event: str, **kv: Any) -> None:
    """
    Log visual compacto y consistente. No hace spam de páginas.
    """
    dt = now_epoch() - T0
    tid = threading.get_ident() % 10000
    parts = [f"{dt:7.2f}s", f"t{tid:04d}", event]
    for k, v in kv.items():
        if v is None:
            continue
        if isinstance(v, str):
            parts.append(f'{k}="{v}"')
        else:
            parts.append(f"{k}={v}")
    line = " ".join(parts)
    with LOG_LOCK:
        print(line, flush=True)


# -----------------------------
# Presupuesto global de retries
# -----------------------------

class RetryBudget:
    def __init__(self, total: int) -> None:
        self._lock = threading.Lock()
        self.remaining = total

    def consume_or_fail(self, reason: str, entity_kind: str, entity_id: int, page: int, char_id: Optional[int], worker: str) -> None:
        with self._lock:
            self.remaining -= 1
            rem = self.remaining
        log("🎛️ budget", worker=worker, kind=entity_kind, id=entity_id, page=page, consume=1, reason=reason, char_id=char_id, remaining=rem)
        if rem < 0:
            # Si llega a negativo, ya estaba agotado antes de consumir.
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated/retry requests (401/420/429/5xx).")

    def get_remaining(self) -> int:
        with self._lock:
            return self.remaining


# -----------------------------
# Tokens (exactamente como ESI Structures)
# -----------------------------

def load_access_tokens(primary_char_id: int) -> Tuple[List[int], Dict[int, str]]:
    """
    - Lee EOU_ACCESS_TOKENS_1 + EOU_ACCESS_TOKENS_2 (JSON char_id -> token)
    - Fusiona
    - Orden de rotación:
      1) primary_char_id si existe
      2) resto de char_id en orden DESC excluyendo primary
    """
    raw1 = os.environ.get("EOU_ACCESS_TOKENS_1", "") or "{}"
    raw2 = os.environ.get("EOU_ACCESS_TOKENS_2", "") or "{}"
    try:
        d1 = json.loads(raw1)
        d2 = json.loads(raw2)
    except Exception as e:
        raise RuntimeError(f"Invalid JSON in EOU_ACCESS_TOKENS_1/2: {e}")

    merged: Dict[int, str] = {}
    for k, v in {**d1, **d2}.items():
        try:
            merged[int(k)] = str(v)
        except Exception:
            continue

    if not merged:
        raise RuntimeError("No access tokens found in EOU_ACCESS_TOKENS_1/2")

    ids = sorted(merged.keys(), reverse=True)
    ordered: List[int] = []
    if primary_char_id in merged:
        ordered.append(primary_char_id)
    for cid in ids:
        if cid == primary_char_id:
            continue
        ordered.append(cid)

    return ordered, merged


# -----------------------------
# ESI HTTP
# -----------------------------

def esi_get(url: str, headers: Dict[str, str]) -> Tuple[int, Dict[str, str], bytes]:
    req = Request(url, method="GET", headers=headers)
    try:
        with urlopen(req, timeout=60) as resp:
            status = getattr(resp, "status", 200)
            resp_headers = {k: v for (k, v) in resp.headers.items()}
            body = resp.read()
            return status, resp_headers, body
    except HTTPError as e:
        status = e.code
        resp_headers = {k: v for (k, v) in getattr(e, "headers", {}).items()}
        body = e.read() if hasattr(e, "read") else b""
        return status, resp_headers, body
    except URLError as e:
        # Tratamos como 503 "transitorio" para entrar en política 5xx
        return 503, {}, (str(e).encode("utf-8", errors="replace"))


def parse_xpages(headers: Dict[str, str]) -> Optional[int]:
    for k in ("X-Pages", "x-pages", "X-pages", "x-Pages"):
        if k in headers:
            try:
                return int(headers[k])
            except Exception:
                return None
    return None

def parse_retry_after(headers: Dict[str, str]) -> Optional[int]:
    for k in ("Retry-After", "retry-after"):
        if k in headers:
            try:
                return int(headers[k])
            except Exception:
                return None
    return None

def parse_expires_epoch(headers: Dict[str, str]) -> Optional[int]:
    # Expires: HTTP-date
    exp = headers.get("Expires") or headers.get("expires")
    if not exp:
        return None
    try:
        # email-like parsing sin dependencias: datetime.strptime
        # Ej: "Sun, 01 Feb 2026 22:44:34 GMT"
        dt = datetime.strptime(exp, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def parse_last_modified_epoch(headers: Dict[str, str]) -> Optional[int]:
    lm = headers.get("Last-Modified") or headers.get("last-modified")
    if not lm:
        return None
    try:
        dt = datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


# -----------------------------
# Normalización de órdenes (RAW)
# -----------------------------

RAW_FIELDS = (
    "duration",
    "is_buy_order",
    "issued",
    "location_id",
    "min_volume",
    "order_id",
    "price",
    "range",
    "system_id",
    "type_id",
    "volume_remain",
    "volume_total",
)

def normalize_range_to_int64(r: Any) -> int:
    """
    Excepción range:
    - "station" / "solarsystem" -> 0
    - "region" -> 1000
    - si es string numérico -> int
    - si ya es int -> int
    """
    if isinstance(r, int):
        return int(r)
    if isinstance(r, str):
        rs = r.strip().lower()
        if rs in ("station", "solarsystem"):
            return 0
        if rs == "region":
            return 1000
        # num como string
        try:
            return int(rs)
        except Exception:
            return 0
    return 0

def normalize_order_raw(o: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["duration"] = safe_int(o.get("duration"))
    out["is_buy_order"] = bool(o.get("is_buy_order"))
    out["issued"] = str(o.get("issued") or "")
    out["location_id"] = int(o.get("location_id"))
    out["min_volume"] = safe_int(o.get("min_volume"))
    out["order_id"] = int(o.get("order_id"))
    out["price"] = float(o.get("price"))
    out["range"] = normalize_range_to_int64(o.get("range"))
    out["system_id"] = safe_int(o.get("system_id"))
    out["type_id"] = safe_int(o.get("type_id"))
    out["volume_remain"] = safe_int(o.get("volume_remain"))
    out["volume_total"] = safe_int(o.get("volume_total"))
    return out


# -----------------------------
# VWAP aggregation
# -----------------------------

@dataclass
class VwapAcc:
    buy_pv: float = 0.0   # sum(price * vol)
    buy_v: int = 0        # sum(vol)
    sell_pv: float = 0.0
    sell_v: int = 0

    def add(self, is_buy: bool, price: float, vol_remain: int) -> None:
        if vol_remain <= 0:
            return
        if is_buy:
            self.buy_pv += price * vol_remain
            self.buy_v += vol_remain
        else:
            self.sell_pv += price * vol_remain
            self.sell_v += vol_remain

    def merge(self, other: "VwapAcc") -> None:
        self.buy_pv += other.buy_pv
        self.buy_v += other.buy_v
        self.sell_pv += other.sell_pv
        self.sell_v += other.sell_v


# -----------------------------
# Planner / Cache pages
# -----------------------------

def load_pages_cache(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def compute_workers(total_pages_cached: int, wmin: int, wmax: int, target_pages_per_worker: int) -> int:
    # workers dinámicos dentro de [min,max]
    if total_pages_cached <= 0:
        return wmin
    need = int(math.ceil(total_pages_cached / max(1, target_pages_per_worker)))
    return max(wmin, min(wmax, need))

def greedy_balance_entities(entities: List[Tuple[int, str, int]], workers: int) -> List[List[Tuple[int, str, int]]]:
    """
    entities: (id, name, pages_cached)
    asignación greedy (LPT) por pages para balancear.
    """
    buckets: List[List[Tuple[int, str, int]]] = [[] for _ in range(max(1, workers))]
    loads = [0] * len(buckets)
    # sort desc pages
    for e in sorted(entities, key=lambda x: x[2], reverse=True):
        i = min(range(len(buckets)), key=lambda j: loads[j])
        buckets[i].append(e)
        loads[i] += e[2]
    return buckets


# -----------------------------
# Carga índices SDE/ESI para hubs y types
# -----------------------------

def load_regions(path: str) -> List[Tuple[int, str]]:
    regions: List[Tuple[int, str]] = []
    for row in read_json_gz_jsonl(path):
        rid = int(row["regionID"])
        name = str(row["region"])
        regions.append((rid, name))
    return regions

def load_stations_map(path: str) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for row in read_json_gz_jsonl(path):
        sid = int(row["stationID"])
        m[sid] = str(row["station"])
    return m

def load_structures_map(path: str) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for row in read_json_gz_jsonl(path):
        sid = int(row["stationID"])
        m[sid] = str(row.get("station") or "")
    return m

def load_market_structures(path: str) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for row in read_json_gz_jsonl(path):
        if not bool(row.get("market", False)):
            continue
        sid = int(row["stationID"])
        name = str(row.get("station") or "")
        out.append((sid, name))
    return out

def load_types_map(path: str) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for row in read_json_gz_jsonl(path):
        tid = int(row["typeID"])
        m[tid] = str(row.get("type") or "")
    return m


# -----------------------------
# Máquina de estados por entidad (TU especificación)
# -----------------------------

def fetch_entity_pages(
    *,
    entity_kind: str,            # "REGION" | "STRUCTURE"
    entity_id: int,
    entity_name: str,
    base_url: str,
    datasource: str,
    token_order: List[int],
    token_map: Dict[int, str],
    budget: RetryBudget,
    worker_label: str,
    vwap_local: Dict[int, VwapAcc],
    hubs_local_orders: Dict[int, int],
    hubs_local_types: Dict[int, set],
) -> Tuple[int, bool, int, bool, Optional[int], Optional[int], bool]:
    """
    Devuelve:
      pages_ok, had_xpages, xpages, ignored_structure, max_expires_epoch, max_last_modified_epoch, ok
    """
    page = 1
    pages_ok = 0
    had_xpages = False
    xpages: Optional[int] = None
    attempts_404_page1 = 0
    ignored_structure = False

    max_expires_epoch: Optional[int] = None
    max_last_modified_epoch: Optional[int] = None

    # token rotation pointer (solo usado en STRUCTURE / 401)
    token_idx = 0
    current_char_id: Optional[int] = None

    def build_url(p: int) -> str:
        if entity_kind == "REGION":
            return f"{base_url}/markets/{entity_id}/orders/?datasource={datasource}&page={p}"
        return f"{base_url}/markets/structures/{entity_id}/?datasource={datasource}&page={p}"

    def make_headers() -> Dict[str, str]:
        h = {
            "Accept": "application/json",
            "User-Agent": "EOU/market-orders (GitHub Actions)",
        }
        if entity_kind == "STRUCTURE":
            nonlocal current_char_id
            if token_idx >= len(token_order):
                # si por cualquier razón nos quedamos sin tokens, reutilizamos el último
                cid = token_order[-1]
            else:
                cid = token_order[token_idx]
            current_char_id = cid
            tok = token_map[cid]
            # tokens vienen sin "Bearer " según tu log; lo añadimos aquí.
            h["Authorization"] = f"Bearer {tok}"
        return h

    log("📦 entity_start", worker=worker_label, kind=entity_kind, id=entity_id, name=entity_name)

    while True:
        url = build_url(page)
        headers = make_headers()
        status, resp_headers, body = esi_get(url, headers)

        exp = parse_expires_epoch(resp_headers)
        lm = parse_last_modified_epoch(resp_headers)
        if exp is not None:
            max_expires_epoch = exp if max_expires_epoch is None else max(max_expires_epoch, exp)
        if lm is not None:
            max_last_modified_epoch = lm if max_last_modified_epoch is None else max(max_last_modified_epoch, lm)

        if status == 200:
            pages_ok += 1

            # parse
            try:
                rows = json.loads(body.decode("utf-8"))
            except Exception:
                rows = []

            # acumular RAW + dedupe local se hace fuera (worker), pero aquí alimentamos métricas (VWAP/hubs)
            for o in rows:
                try:
                    raw = normalize_order_raw(o)
                except Exception:
                    continue

                # Para hubs, agregamos por location_id:
                loc = raw["location_id"]
                hubs_local_orders[loc] = hubs_local_orders.get(loc, 0) + 1
                hubs_local_types.setdefault(loc, set()).add(raw["type_id"])

                # VWAP por type_id separado buy/sell
                tid = raw["type_id"]
                acc = vwap_local.get(tid)
                if acc is None:
                    acc = VwapAcc()
                    vwap_local[tid] = acc
                acc.add(raw["is_buy_order"], float(raw["price"]), int(raw["volume_remain"]))

            xp = parse_xpages(resp_headers)
            if xp is not None:
                had_xpages = True
                xpages = xp
                if page < xp:
                    page += 1
                    continue
                # page == X-Pages -> FIN sin pedir page+1 (regla de oro)
                break
            else:
                page += 1
                continue

        if status == 404:
            if entity_kind == "REGION":
                break
            # STRUCTURE
            if page == 1:
                if attempts_404_page1 < 1:
                    attempts_404_page1 += 1
                    time.sleep(5.0)
                    # retry misma page (sin consumir budget, tu regla 404 page1 es “retry 1 vez”)
                    continue
                # vuelve a 404 -> IGNORAR estructura (no cache)
                ignored_structure = True
                break
            # page > 1 -> fin paginación
            break

        if status == 401 and entity_kind == "STRUCTURE":
            # rotar token + sleep 5s + retry misma page + consume budget
            budget.consume_or_fail("401", entity_kind, entity_id, page, current_char_id, worker_label)
            token_idx += 1
            time.sleep(5.0)
            continue

        if status in (420, 429) or (500 <= status <= 599):
            # backoff conservador: retry-after si viene; si no, 30s
            budget.consume_or_fail(str(status), entity_kind, entity_id, page, current_char_id, worker_label)
            ra = parse_retry_after(resp_headers)
            sleep_s = float(ra) if ra is not None and ra > 0 else 30.0
            time.sleep(sleep_s)
            continue

        # otros 4xx
        if 400 <= status <= 499:
            if entity_kind == "STRUCTURE":
                ignored_structure = True
                break
            raise RuntimeError(f"Unexpected {status} for REGION {entity_id} page={page}")

        # fallback inesperado -> tratamos como 503 (ya cubierto arriba normalmente)
        budget.consume_or_fail(str(status), entity_kind, entity_id, page, current_char_id, worker_label)
        time.sleep(30.0)

    log(
        "✅ entity_done",
        worker=worker_label,
        kind=entity_kind,
        id=entity_id,
        name=entity_name,
        pages_ok=pages_ok,
        had_xpages=had_xpages,
        xpages=xpages,
        ignored=ignored_structure,
    )
    return pages_ok, had_xpages, (xpages or 0), ignored_structure, max_expires_epoch, max_last_modified_epoch, True


# -----------------------------
# Workers
# -----------------------------

def run_region_worker(
    worker_label: str,
    regions: List[Tuple[int, str, int]],
    base_url: str,
    datasource: str,
    budget: RetryBudget,
) -> Tuple[Dict[str, Any], Dict[int, VwapAcc], Dict[int, int], Dict[int, set]]:
    """
    regions: [(region_id, region_name, pages_cached), ...]
    """
    vwap_local: Dict[int, VwapAcc] = {}
    hubs_orders: Dict[int, int] = {}
    hubs_types: Dict[int, set] = {}

    max_exp: Optional[int] = None
    max_lm: Optional[int] = None

    log("🚀 worker_start", worker=worker_label, kind="REGION", entities=len(regions))

    for rid, name, _pc in regions:
        pages_ok, _had_xp, _xp, _ignored, exp, lm, _ok = fetch_entity_pages(
            entity_kind="REGION",
            entity_id=rid,
            entity_name=name,
            base_url=base_url,
            datasource=datasource,
            token_order=[],
            token_map={},
            budget=budget,
            worker_label=worker_label,
            vwap_local=vwap_local,
            hubs_local_orders=hubs_orders,
            hubs_local_types=hubs_types,
        )
        if exp is not None:
            max_exp = exp if max_exp is None else max(max_exp, exp)
        if lm is not None:
            max_lm = lm if max_lm is None else max(max_lm, lm)

    log("🏁 worker_done", worker=worker_label, kind="REGION", entities=len(regions))
    meta = {"max_expires_epoch": max_exp, "max_last_modified_epoch": max_lm}
    return meta, vwap_local, hubs_orders, hubs_types


def run_struct_worker(
    worker_label: str,
    structs: List[Tuple[int, str, int]],
    base_url: str,
    datasource: str,
    token_order: List[int],
    token_map: Dict[int, str],
    budget: RetryBudget,
) -> Tuple[Dict[str, Any], Dict[int, VwapAcc], Dict[int, int], Dict[int, set], List[Tuple[int, str, int]]]:
    """
    Devuelve también `structs_ok_for_cache`: lista de estructuras NO ignoradas (para cache final).
    """
    vwap_local: Dict[int, VwapAcc] = {}
    hubs_orders: Dict[int, int] = {}
    hubs_types: Dict[int, set] = {}

    max_exp: Optional[int] = None
    max_lm: Optional[int] = None
    ok_for_cache: List[Tuple[int, str, int]] = []

    log("🚀 worker_start", worker=worker_label, kind="STRUCTURE", entities=len(structs))

    for sid, name, _pc in structs:
        pages_ok, _had_xp, _xp, ignored, exp, lm, _ok = fetch_entity_pages(
            entity_kind="STRUCTURE",
            entity_id=sid,
            entity_name=name,
            base_url=base_url,
            datasource=datasource,
            token_order=token_order,
            token_map=token_map,
            budget=budget,
            worker_label=worker_label,
            vwap_local=vwap_local,
            hubs_local_orders=hubs_orders,
            hubs_local_types=hubs_types,
        )

        if exp is not None:
            max_exp = exp if max_exp is None else max(max_exp, exp)
        if lm is not None:
            max_lm = lm if max_lm is None else max(max_lm, lm)

        # Solo si NO se ignora, puede ir al cache
        if not ignored:
            ok_for_cache.append((sid, name, pages_ok))

    log("🏁 worker_done", worker=worker_label, kind="STRUCTURE", entities=len(structs))
    meta = {"max_expires_epoch": max_exp, "max_last_modified_epoch": max_lm}
    return meta, vwap_local, hubs_orders, hubs_types, ok_for_cache


# -----------------------------
# Hubs.json (ya existente en tu diseño)
# -----------------------------

def build_hubs_json(
    hubs_orders: Dict[int, int],
    hubs_types: Dict[int, set],
    stations_map: Dict[int, str],
    structures_map: Dict[int, str],
    threshold_share: float = 0.0175,
) -> Dict[str, Any]:
    total_orders = sum(hubs_orders.values())

    entries: List[Tuple[int, int, int, float]] = []
    for loc, cnt in hubs_orders.items():
        if cnt <= 0:
            continue
        types_cnt = len(hubs_types.get(loc, set()))
        # 3º tie-break: sum(price*vol_remain) NO lo tenemos aquí porque no lo reflejas en hubs.json;
        # en tu regla, solo calcularlo si hiciera falta. Para no inventar, dejamos el sorting por 1º/2º y
        # luego por location_id como fallback estable. (La “necesidad” real de 3º/4º en python sort es rara.)
        share = (cnt / total_orders) if total_orders > 0 else 0.0
        entries.append((loc, cnt, types_cnt, share))

    # Orden: orders desc, types desc, location_id asc (3º/4º no persistido)
    entries.sort(key=lambda x: (-x[1], -x[2], x[0]))

    hubs_list: List[Dict[str, Any]] = []
    for loc, cnt, types_cnt, share in entries:
        if total_orders <= 0:
            continue
        if share < threshold_share:
            continue

        # resolver nombre estación/estructura
        station_name = ""
        if 60000000 <= loc < 70000000:
            station_name = stations_map.get(loc, "")
        else:
            station_name = structures_map.get(loc, "")

        hubs_list.append({
            "stationID": int(loc),
            "station": station_name,
            "orders": int(cnt),
            "ordersShare": fmt4(share),
            "types": int(types_cnt),
        })

    out = {
        "hubs": int(len(hubs_list)),
        "orders": int(total_orders),
        "hubsList": hubs_list,
    }
    return out


# -----------------------------
# Orders.jsonl (NUEVO: VWAP buy/sell por type_id)
# -----------------------------

def build_orders_jsonl_rows(
    vwap_global: Dict[int, VwapAcc],
    types_map: Dict[int, str],
) -> List[Dict[str, Any]]:
    """
    Crea filas:
      typeID, type, sellPrice, buyPrice
    - sellPrice = VWAP donde is_buy_order=false
    - buyPrice  = VWAP donde is_buy_order=true
    Redondeo a 2 decimales.
    """
    rows: List[Dict[str, Any]] = []

    for type_id, acc in vwap_global.items():
        # sell VWAP
        sell = None
        if acc.sell_v > 0:
            sell = acc.sell_pv / acc.sell_v

        buy = None
        if acc.buy_v > 0:
            buy = acc.buy_pv / acc.buy_v

        # Si no hay nada de buy y sell, saltar
        if sell is None and buy is None:
            continue

        rows.append({
            "typeID": int(type_id),
            "type": types_map.get(int(type_id), ""),
            "sellPrice": fmt2(sell) if sell is not None else None,
            "buyPrice": fmt2(buy) if buy is not None else None,
        })

    # Orden estable por typeID asc
    rows.sort(key=lambda r: r["typeID"])
    return rows


# -----------------------------
# MAIN
# -----------------------------

def main() -> int:
    # Core env
    base_url = read_env_str("ESI_BASE_URL", "https://esi.evetech.net")
    datasource = read_env_str("ESI_DATASOURCE", "tranquility")

    regions_workers_min = read_env_int("REGIONS_WORKERS_MIN", 2)
    regions_target = read_env_int("REGIONS_TARGET_PAGES_PER_WORKER", 220)

    structs_workers_min = read_env_int("STRUCTS_WORKERS_MIN", 1)
    structs_target = read_env_int("STRUCTS_TARGET_PAGES_PER_WORKER", 60)

    pages_cache_path = read_env_str("PAGES_CACHE_PATH", "states/market_orders_pages.json")
    hubs_path = read_env_str("HUBS_PATH", "data/esi/hubs.json")
    orders_path = read_env_str("ORDERS_PATH", "data/esi/Orders.jsonl")  # NUEVO env opcional
    out_dir = read_env_str("OUT_DIR", ".tmp_eou_market_orders")

    sde_regions_path = read_env_str("SDE_REGIONS_PATH", "data/sde/regions.jsonl.gz")
    sde_stations_path = read_env_str("SDE_STATIONS_PATH", "data/sde/stations.jsonl.gz")
    structures_file = read_env_str("STRUCTURES_FILE", "data/esi/structures.jsonl.gz")
    sde_types_path = read_env_str("SDE_TYPES_PATH", "data/sde/types.jsonl.gz")  # NECESARIO para join

    primary_char_id = read_env_int("PRIMARY_CHAR_ID", 2124070822)
    retry_budget = read_env_int("RETRY_BUDGET", 3)

    ensure_dir(out_dir)

    log("🧭 phase", step="loading indices")
    regions = load_regions(sde_regions_path)
    structures_market = load_market_structures(structures_file)

    stations_map = load_stations_map(sde_stations_path)
    structures_map = load_structures_map(structures_file)

    token_order, token_map = load_access_tokens(primary_char_id)
    log("🧭 phase", regions_count=len(regions), structures_market_count=len(structures_market), tokens_count=len(token_map), primary_char_id=primary_char_id)

    # Cache & planner
    cache = load_pages_cache(pages_cache_path)
    planner = cache.get("planner") if isinstance(cache, dict) else None

    # defaults si no hay cache:
    regions_workers_max = safe_int((planner or {}).get("REGIONS_WORKERS_MAX"), 8)
    structs_workers_max = safe_int((planner or {}).get("STRUCTS_WORKERS_MAX"), 3)

    # Pages cached lookup
    cached_station_pages: Dict[int, int] = {}
    cached_struct_pages: Dict[int, int] = {}
    if isinstance(cache, dict):
        for it in cache.get("stations", []) or []:
            try:
                cached_station_pages[int(it["regionID"])] = int(it.get("pages", 0))
            except Exception:
                continue
        for it in cache.get("structures", []) or []:
            try:
                cached_struct_pages[int(it["stationID"])] = int(it.get("pages", 0))
            except Exception:
                continue

    region_entities: List[Tuple[int, str, int]] = [(rid, name, cached_station_pages.get(rid, 0)) for (rid, name) in regions]
    struct_entities: List[Tuple[int, str, int]] = [(sid, name, cached_struct_pages.get(sid, 0)) for (sid, name) in structures_market]

    regions_total_pages_cached = sum(p for *_rest, p in region_entities)
    structs_total_pages_cached = sum(p for *_rest, p in struct_entities)

    regions_workers = compute_workers(regions_total_pages_cached, regions_workers_min, regions_workers_max, regions_target)
    structs_workers = compute_workers(structs_total_pages_cached, structs_workers_min, structs_workers_max, structs_target)

    log("🧭 phase", step="planner",
        regions_total_pages_cached=regions_total_pages_cached, regions_workers=regions_workers, regions_workers_max=regions_workers_max, regions_workers_min=regions_workers_min,
        structs_total_pages_cached=structs_total_pages_cached, structs_workers=structs_workers, structs_workers_max=structs_workers_max, structs_workers_min=structs_workers_min)

    region_buckets = greedy_balance_entities(region_entities, regions_workers)
    struct_buckets = greedy_balance_entities(struct_entities, structs_workers)

    budget = RetryBudget(retry_budget)

    # Global merges
    vwap_global: Dict[int, VwapAcc] = {}
    hubs_orders_global: Dict[int, int] = {}
    hubs_types_global: Dict[int, set] = {}
    structs_ok_for_cache_all: List[Tuple[int, str, int]] = []

    max_exp_all: Optional[int] = None
    max_lm_all: Optional[int] = None

    log("🧭 phase", step="workers start")
    futures: List[cf.Future] = []

    with cf.ThreadPoolExecutor(max_workers=regions_workers + structs_workers) as ex:
        # Regions
        for i, bucket in enumerate(region_buckets, start=1):
            label = f"regions_w{i}/{regions_workers}"
            futures.append(ex.submit(run_region_worker, label, bucket, base_url, datasource, budget))

        # Structs
        for i, bucket in enumerate(struct_buckets, start=1):
            label = f"structs_w{i}/{structs_workers}"
            futures.append(ex.submit(run_struct_worker, label, bucket, base_url, datasource, token_order, token_map, budget))

        for fut in cf.as_completed(futures):
            res = fut.result()
            # Disambiguar por aridad
            if len(res) == 4:
                meta, vwap_loc, hubs_o, hubs_t = res
                ok_structs = None
            else:
                meta, vwap_loc, hubs_o, hubs_t, ok_structs = res

            # merge meta max epochs
            mx = meta.get("max_expires_epoch")
            ml = meta.get("max_last_modified_epoch")
            if mx is not None:
                max_exp_all = mx if max_exp_all is None else max(max_exp_all, mx)
            if ml is not None:
                max_lm_all = ml if max_lm_all is None else max(max_lm_all, ml)

            # merge vwap
            for tid, acc in vwap_loc.items():
                g = vwap_global.get(tid)
                if g is None:
                    vwap_global[tid] = acc
                else:
                    g.merge(acc)

            # merge hubs
            for loc, cnt in hubs_o.items():
                hubs_orders_global[loc] = hubs_orders_global.get(loc, 0) + cnt
            for loc, s in hubs_t.items():
                hubs_types_global.setdefault(loc, set()).update(s)

            if ok_structs is not None:
                structs_ok_for_cache_all.extend(ok_structs)

    log("🧭 phase", step="workers end")

    # Construir hubs.json (pretty)
    hubs_obj = build_hubs_json(hubs_orders_global, hubs_types_global, stations_map, structures_map, threshold_share=0.0175)
    atomic_write_json(hubs_path, hubs_obj, indent=2)
    log("🧾 output", file=hubs_path, hubs=hubs_obj.get("hubs"), orders=hubs_obj.get("orders"))

    # Construir Orders.jsonl (VWAP)
    types_map = load_types_map(sde_types_path)
    orders_rows = build_orders_jsonl_rows(vwap_global, types_map)
    atomic_write_jsonl(orders_path, orders_rows)
    log("🧾 output", file=orders_path, rows=len(orders_rows))

    # Actualizar cache pages SOLO si run exitoso:
    # - stations: todas las regiones, pages = pages_ok detectadas (no lo hemos guardado por entidad aquí).
    #   Para respetar “sin inventar”, mantenemos pages previas si no tenemos nuevas.
    # - structures: SOLO las NO ignoradas, con pages=pages_ok de este run (ok_structs)
    #
    # Nota: para no re-arquitecturar ahora toda la contabilidad de pages por región,
    #       mantenemos pages de cache existentes. (No inventamos pages.)
    new_cache = {
        "planner": {
            "REGIONS_WORKERS_MAX": int(regions_workers_max),
            "STRUCTS_WORKERS_MAX": int(structs_workers_max),
        },
        "stations": [
            {"regionID": rid, "region": name, "pages": int(cached_station_pages.get(rid, 0))}
            for (rid, name) in regions
        ],
        "structures": [
            {"stationID": sid, "station": name, "pages": int(pages_ok)}
            for (sid, name, pages_ok) in structs_ok_for_cache_all
        ],
    }
    atomic_write_json(pages_cache_path, new_cache, indent=2)
    log("🧭 phase", step="cache_written", file=pages_cache_path)

    # Outputs para finalize (Github Actions)
    if max_exp_all is not None:
        print(f"max_expires_epoch={max_exp_all}")
    if max_lm_all is not None:
        print(f"max_last_modified_epoch={max_lm_all}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log("💥 fatal", error=str(e))
        raise
