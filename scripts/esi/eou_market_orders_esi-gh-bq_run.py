#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import json
import math
import os
import sys
import time
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# ----------------------------
# Config / env
# ----------------------------

ESI_BASE_URL = os.environ.get("ESI_BASE_URL", "https://esi.evetech.net").rstrip("/")
ESI_DATASOURCE = os.environ.get("ESI_DATASOURCE", "tranquility")

PRIMARY_CHAR_ID = os.environ.get("PRIMARY_CHAR_ID", "2124070822").strip()
RETRY_BUDGET = int(os.environ.get("RETRY_BUDGET", "3"))

REGIONS_WORKERS_MIN = int(os.environ.get("REGIONS_WORKERS_MIN", "2"))
STRUCTS_WORKERS_MIN = int(os.environ.get("STRUCTS_WORKERS_MIN", "1"))

REGIONS_TARGET_PAGES_PER_WORKER = int(os.environ.get("REGIONS_TARGET_PAGES_PER_WORKER", "220"))
STRUCTS_TARGET_PAGES_PER_WORKER = int(os.environ.get("STRUCTS_TARGET_PAGES_PER_WORKER", "60"))

SDE_REGIONS_PATH = os.environ.get("SDE_REGIONS_PATH", "data/sde/regions.jsonl.gz")
SDE_STATIONS_PATH = os.environ.get("SDE_STATIONS_PATH", "data/sde/stations.jsonl.gz")
STRUCTURES_FILE = os.environ.get("STRUCTURES_FILE", "data/esi/structures.jsonl.gz")

PAGES_CACHE_PATH = os.environ.get("PAGES_CACHE_PATH", "states/market_orders_pages.json")

OUT_DIR = os.environ.get("OUT_DIR", ".tmp_eou_market_orders")

EOU_ACCESS_TOKENS_1 = os.environ.get("EOU_ACCESS_TOKENS_1", "")
EOU_ACCESS_TOKENS_2 = os.environ.get("EOU_ACCESS_TOKENS_2", "")

HUBS_PATH = os.environ.get("HUBS_PATH", "data/esi/hubs.jsonl")

DEFAULT_REGIONS_WORKERS_MAX = 8
DEFAULT_STRUCTS_WORKERS_MAX = 3

MAX_WORKERS_SUM_LIMIT = 13

USER_AGENT = os.environ.get("ESI_USER_AGENT", "EOU/market-orders (+https://github.com/landu-eou/eou)")

STATION_MIN_ID = 60_000_000
STATION_MAX_ID = 70_000_000  # [60M,70M) station ; >=70M structure

# ----------------------------
# Minimal logs
# ----------------------------

_START_EPOCH = time.time()
_PRINT_LOCK = threading.Lock()

def _rel_ts() -> str:
    return f"{time.time() - _START_EPOCH:7.2f}s"

def _tid() -> str:
    return f"t{threading.get_ident() % 10000:04d}"

def _icon(event: str) -> str:
    return {
        "phase": "🧭",
        "worker_start": "🚀",
        "worker_done": "🏁",
        "entity_start": "📦",
        "entity_done": "✅",
        "entity_ignored": "⏭️",
        "retry": "🔁",
        "budget": "🎛️",
        "fatal": "💥",
    }.get(event, "•")

def vlog(
    *,
    event: str,
    worker: str = "-",
    kind: str = "-",
    entity_id: Optional[int] = None,
    entity_name: str = "",
    msg: str = "",
) -> None:
    parts = [
        _rel_ts(),
        _tid(),
        _icon(event),
        f"{event:12s}",
        f"worker={worker}",
        f"kind={kind}",
    ]
    if entity_id is not None:
        parts.append(f"id={entity_id}")
    if entity_name:
        safe = entity_name.replace("\n", " ").strip()
        if len(safe) > 70:
            safe = safe[:67] + "..."
        parts.append(f"name=\"{safe}\"")
    if msg:
        parts.append(msg)

    with _PRINT_LOCK:
        print(" ".join(parts), flush=True)

# ----------------------------
# Global auth retry budget
# ----------------------------

class GlobalBudget:
    """
    RETRY_BUDGET cuenta *reintentos*. Si budget=3:
      - puedes consumir 1 hasta llegar a 0 (aún permitido),
      - sólo fallas si necesitas consumir otra vez y pasa a -1.
    """
    def __init__(self, initial: int):
        self._lock = threading.Lock()
        self._remaining = initial

    def consume_or_fail(self, reason: str, kind: str, entity_id: int, page: int, char_id: Optional[str], worker: str) -> None:
        with self._lock:
            self._remaining -= 1
            remaining = self._remaining

        vlog(
            event="budget",
            worker=worker,
            kind=kind,
            entity_id=entity_id,
            msg=f"consume=1 reason={reason} page={page} char_id={char_id} remaining={remaining}",
        )

        # ✅ FIX: fallar sólo si se pasa de 0 -> -1 (o menos)
        if remaining < 0:
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated/retry requests (401/420/429/5xx).")

# ----------------------------
# Token rotation (exact policy)
# ----------------------------

class TokenRotator:
    def __init__(self, tokens: Dict[str, str], primary_char_id: str):
        self._tokens = tokens
        self._primary = primary_char_id

        ordered: List[str] = []
        if primary_char_id in tokens:
            ordered.append(primary_char_id)

        rest = [cid for cid in tokens.keys() if cid != primary_char_id]

        def keyf(x: str) -> int:
            try:
                return int(x)
            except Exception:
                return -1

        rest.sort(key=keyf, reverse=True)
        ordered.extend(rest)

        self._order = ordered
        self._idx = 0
        self._lock = threading.Lock()

    def current(self) -> Tuple[Optional[str], Optional[str]]:
        with self._lock:
            if not self._order:
                return None, None
            cid = self._order[self._idx % len(self._order)]
            return cid, self._tokens.get(cid)

    def rotate_next(self) -> Tuple[Optional[str], Optional[str]]:
        with self._lock:
            if not self._order:
                return None, None
            self._idx = (self._idx + 1) % len(self._order)
            cid = self._order[self._idx]
            return cid, self._tokens.get(cid)

# ----------------------------
# IO helpers
# ----------------------------

def read_jsonl_gz(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_jsonl_atomic(path: str, rows: List[Dict[str, Any]]) -> None:
    tmp_dir = os.path.dirname(path) or "."
    ensure_dir(tmp_dir)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_jsonl_", suffix=".jsonl", dir=tmp_dir)
    os.close(fd)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False))
                f.write("\n")
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

# ----------------------------
# Cache model (pages planner)
# ----------------------------

@dataclass
class CachePlanner:
    REGIONS_WORKERS_MAX: int
    STRUCTS_WORKERS_MAX: int

@dataclass
class CacheStation:
    regionID: int
    region: str
    pages: int

@dataclass
class CacheStructure:
    stationID: int
    station: str
    pages: int

@dataclass
class PagesCache:
    planner: CachePlanner
    stations: List[CacheStation]
    structures: List[CacheStructure]

def load_pages_cache(path: str) -> Optional[PagesCache]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        planner_raw = raw.get("planner") or {}
        planner = CachePlanner(
            REGIONS_WORKERS_MAX=int(planner_raw.get("REGIONS_WORKERS_MAX", DEFAULT_REGIONS_WORKERS_MAX)),
            STRUCTS_WORKERS_MAX=int(planner_raw.get("STRUCTS_WORKERS_MAX", DEFAULT_STRUCTS_WORKERS_MAX)),
        )
        stations = []
        for x in (raw.get("stations") or []):
            stations.append(CacheStation(int(x["regionID"]), str(x["region"]), int(x.get("pages", 0))))
        structures = []
        for x in (raw.get("structures") or []):
            structures.append(CacheStructure(int(x["stationID"]), str(x["station"]), int(x.get("pages", 0))))
        return PagesCache(planner=planner, stations=stations, structures=structures)
    except Exception as e:
        vlog(event="phase", msg=f"cache_load_failed path={path} err={e}")
        return None

def write_pages_cache_atomic(path: str, cache: PagesCache) -> None:
    tmp_dir = os.path.dirname(path) or "."
    ensure_dir(tmp_dir)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_pages_cache_", suffix=".json", dir=tmp_dir)
    os.close(fd)
    try:
        payload = {
            "planner": {
                "REGIONS_WORKERS_MAX": cache.planner.REGIONS_WORKERS_MAX,
                "STRUCTS_WORKERS_MAX": cache.planner.STRUCTS_WORKERS_MAX,
            },
            "stations": [{"regionID": s.regionID, "region": s.region, "pages": s.pages} for s in cache.stations],
            "structures": [{"stationID": s.stationID, "station": s.station, "pages": s.pages} for s in cache.structures],
        }
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
            f.write("\n")
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

# ----------------------------
# Planner helpers
# ----------------------------

def clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))

def calc_recommended_workers(total_pages: int, target_pages_per_worker: int, min_workers: int, max_workers: int) -> int:
    if total_pages <= 0:
        return min_workers
    rec = int(math.ceil(total_pages / float(target_pages_per_worker)))
    return clamp(rec, min_workers, max_workers)

def greedy_balance(items: List[Tuple[int, Any]], workers: int) -> List[List[Any]]:
    bins = [{"w": 0, "items": []} for _ in range(max(1, workers))]
    items_sorted = sorted(items, key=lambda x: x[0], reverse=True)
    for w, payload in items_sorted:
        b = min(bins, key=lambda x: x["w"])
        b["items"].append(payload)
        b["w"] += w
    return [b["items"] for b in bins]

def enforce_sum_limit(reg_max: int, str_max: int) -> Tuple[int, int]:
    if reg_max + str_max <= MAX_WORKERS_SUM_LIMIT:
        return reg_max, str_max
    while reg_max + str_max > MAX_WORKERS_SUM_LIMIT:
        if reg_max >= str_max and reg_max > 1:
            reg_max -= 1
        elif str_max > 1:
            str_max -= 1
        else:
            break
    return reg_max, str_max

def ceil_div(a: int, b: int) -> int:
    return int(math.ceil(a / float(b))) if b else a

def update_max_slow(*, current_max: int, recommended_max: int, min_workers: int) -> int:
    if recommended_max > current_max:
        return current_max + 1
    if recommended_max < (current_max / 4.0):
        lower_bound = min_workers * 2
        new_max = max(lower_bound, current_max - ceil_div(current_max, 2))
        return max(new_max, lower_bound)
    return current_max

# ----------------------------
# HTTP helpers
# ----------------------------

def http_get(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout_s: int = 60) -> requests.Response:
    return requests.get(url, headers=headers, params=params, timeout=timeout_s)

def parse_retry_after_seconds(resp: requests.Response) -> Optional[int]:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return int(ra.strip())
    except Exception:
        return None

def parse_http_date_to_epoch(s: str) -> Optional[int]:
    try:
        dt = datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %Z")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None

def take_expires_epoch(resp: requests.Response) -> Optional[int]:
    exp = resp.headers.get("Expires")
    if not exp:
        return None
    return parse_http_date_to_epoch(exp)

def take_last_modified_epoch(resp: requests.Response) -> Optional[int]:
    lm = resp.headers.get("Last-Modified")
    if not lm:
        return None
    return parse_http_date_to_epoch(lm)

def header_int(resp: requests.Response, name: str) -> Optional[int]:
    v = resp.headers.get(name)
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None

# ----------------------------
# RAW normalization
# ----------------------------

def normalize_range_to_int64(r: Any) -> Optional[int]:
    if r is None:
        return None
    if isinstance(r, (int, float)):
        try:
            return int(r)
        except Exception:
            return None
    if not isinstance(r, str):
        r = str(r)
    s = r.strip().lower()
    if s in ("station", "solarsystem"):
        return 0
    if s == "region":
        return 1000
    try:
        return int(s)
    except Exception:
        return None

def _as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _as_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default

def normalize_order_raw(o: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Mantiene RAW tal y como pediste, pero:
      - range: string -> int64 según reglas
      - min_volume: si no viene, lo guardamos como 0 (para evitar descartar sells)
    """
    r_int = normalize_range_to_int64(o.get("range"))
    if r_int is None:
        return None

    duration = _as_int(o.get("duration"))
    issued = o.get("issued")
    location_id = _as_int(o.get("location_id"))
    order_id = _as_int(o.get("order_id"))
    price = _as_float(o.get("price"))
    system_id = _as_int(o.get("system_id"))
    type_id = _as_int(o.get("type_id"))
    volume_remain = _as_int(o.get("volume_remain"))
    volume_total = _as_int(o.get("volume_total"))

    # Campos obligatorios (excepto min_volume)
    if (
        duration is None
        or issued is None
        or location_id is None
        or order_id is None
        or price is None
        or system_id is None
        or type_id is None
        or volume_remain is None
        or volume_total is None
    ):
        return None

    min_volume = _as_int(o.get("min_volume"), default=0)  # ✅ FIX

    out = {
        "duration": int(duration),
        "is_buy_order": bool(o.get("is_buy_order")),
        "issued": str(issued),
        "location_id": int(location_id),
        "min_volume": int(min_volume),
        "order_id": int(order_id),
        "price": float(price),
        "range": int(r_int),
        "system_id": int(system_id),
        "type_id": int(type_id),
        "volume_remain": int(volume_remain),
        "volume_total": int(volume_total),
    }
    return out

# ----------------------------
# Per-worker hubs aggregation
# ----------------------------

@dataclass
class HubAgg:
    orders: int
    types: set
    value_sum: float

def hub_add(hubs: Dict[int, HubAgg], raw: Dict[str, Any]) -> None:
    loc = int(raw["location_id"])
    type_id = int(raw["type_id"])
    value = float(raw["price"]) * float(raw["volume_remain"])
    cur = hubs.get(loc)
    if cur is None:
        hubs[loc] = HubAgg(orders=1, types={type_id}, value_sum=value)
    else:
        cur.orders += 1
        cur.types.add(type_id)
        cur.value_sum += value

# ----------------------------
# Entity state machine
# ----------------------------

@dataclass
class EntityResult:
    entity_kind: str
    entity_id: int
    pages_ok: int
    had_xpages: bool
    xpages: Optional[int]
    ignored: bool
    max_expires_epoch: int
    max_last_modified_epoch: int
    rows_emitted: int
    rows_seen: int

def fetch_entity_pages(
    *,
    entity_kind: str,
    entity_id: int,
    entity_name: str,
    rotator: Optional[TokenRotator],
    budget: GlobalBudget,
    worker_label: str,
    seen_order_ids_worker: set,
    hubs_worker: Dict[int, HubAgg],
) -> EntityResult:
    page = 1
    pages_ok = 0
    had_xpages = False
    xpages: Optional[int] = None
    attempts_404_page1 = 0

    max_expires_epoch = 0
    max_last_modified_epoch = 0

    rows_emitted = 0
    rows_seen = 0

    vlog(event="entity_start", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name, msg="start")

    while True:
        if entity_kind == "REGION":
            url = f"{ESI_BASE_URL}/latest/markets/{entity_id}/orders/"
            headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
            active_char: Optional[str] = None
        else:
            url = f"{ESI_BASE_URL}/latest/markets/structures/{entity_id}/"
            cid, tok = (rotator.current() if rotator else (None, None))
            active_char = cid
            if not tok:
                raise RuntimeError("No access tokens available for authenticated structure requests.")
            headers = {
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {tok}",
            }

        params = {"datasource": ESI_DATASOURCE, "page": page}

        try:
            resp = http_get(url, headers=headers, params=params, timeout_s=60)
        except requests.RequestException as e:
            budget.consume_or_fail("net_err", entity_kind, entity_id, page, active_char, worker_label)
            sleep_s = 30
            vlog(event="retry", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                 msg=f"reason=net_err page={page} sleep_s={sleep_s} err={type(e).__name__}")
            time.sleep(sleep_s)
            continue

        status = resp.status_code

        exp_e = take_expires_epoch(resp) or 0
        lm_e = take_last_modified_epoch(resp) or 0
        if exp_e > max_expires_epoch:
            max_expires_epoch = exp_e
        if lm_e > max_last_modified_epoch:
            max_last_modified_epoch = lm_e

        xpages_h = header_int(resp, "X-Pages")

        # (1) 200
        if status == 200:
            pages_ok += 1

            try:
                body = resp.json()
                if not isinstance(body, list):
                    body = []
            except Exception:
                body = []

            for o in body:
                if not isinstance(o, dict):
                    continue
                rows_seen += 1
                raw = normalize_order_raw(o)
                if raw is None:
                    continue

                oid = raw["order_id"]
                if oid in seen_order_ids_worker:
                    continue
                seen_order_ids_worker.add(oid)
                rows_emitted += 1

                hub_add(hubs_worker, raw)

            # X-Pages regla de oro
            if xpages_h is not None:
                had_xpages = True
                xpages = xpages_h
                if page < xpages:
                    page += 1
                    continue
                if page == xpages:
                    break
                break

            page += 1
            continue

        # (2) 404
        if status == 404:
            if entity_kind == "REGION":
                break
            if page == 1:
                if attempts_404_page1 < 1:
                    attempts_404_page1 += 1
                    sleep_s = 5
                    vlog(event="retry", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                         msg=f"reason=404_page1_retry page={page} sleep_s={sleep_s}")
                    time.sleep(sleep_s)
                    continue

                vlog(event="entity_ignored", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                     msg="reason=404_page1_after_retry")
                return EntityResult(entity_kind, entity_id, 0, False, None, True, max_expires_epoch, max_last_modified_epoch, 0, rows_seen)

            break

        # (3) 401 (solo structures)
        if status == 401:
            if entity_kind != "STRUCTURE":
                raise RuntimeError(f"Unexpected 401 for REGION entity_id={entity_id} page={page}")

            cid_before, _ = rotator.current() if rotator else (None, None)
            budget.consume_or_fail("401", entity_kind, entity_id, page, cid_before, worker_label)
            cid_new, _ = rotator.rotate_next() if rotator else (None, None)
            sleep_s = 5
            vlog(event="retry", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                 msg=f"reason=401 rotate {cid_before}->{cid_new} page={page} sleep_s={sleep_s}")
            time.sleep(sleep_s)
            continue

        # (4) 420/429/5xx
        if status in (420, 429) or (500 <= status <= 599):
            ra = parse_retry_after_seconds(resp)
            sleep_s = ra if (ra is not None and ra > 0) else 30
            cur_cid = rotator.current()[0] if rotator else None
            budget.consume_or_fail(str(status), entity_kind, entity_id, page, cur_cid, worker_label)
            vlog(event="retry", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                 msg=f"reason={status} page={page} sleep_s={sleep_s}")
            time.sleep(sleep_s)
            continue

        # (5) otros 4xx
        if 400 <= status <= 499:
            if entity_kind == "STRUCTURE":
                vlog(event="entity_ignored", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
                     msg=f"reason=other4xx status={status} page={page}")
                return EntityResult(entity_kind, entity_id, 0, had_xpages, xpages, True, max_expires_epoch, max_last_modified_epoch, 0, rows_seen)

            raise RuntimeError(f"Unexpected {status} for REGION entity_id={entity_id} page={page}")

        raise RuntimeError(f"Unexpected status={status} kind={entity_kind} entity_id={entity_id} page={page}")

    vlog(event="entity_done", worker=worker_label, kind=entity_kind, entity_id=entity_id, entity_name=entity_name,
         msg=f"pages_ok={pages_ok} had_xpages={had_xpages} xpages={xpages} raw_seen={rows_seen} unique_emitted={rows_emitted}")

    return EntityResult(entity_kind, entity_id, pages_ok, had_xpages, xpages, False, max_expires_epoch, max_last_modified_epoch, rows_emitted, rows_seen)

# ----------------------------
# Tokens
# ----------------------------

def merge_tokens(json1: str, json2: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in (json1, json2):
        raw = (raw or "").strip()
        if not raw:
            continue
        obj = json.loads(raw)
        if isinstance(obj, dict):
            for k, v in obj.items():
                if v:
                    out[str(k)] = str(v)
    return out

def build_cache_maps(cache: Optional[PagesCache]) -> Tuple[Dict[int, int], Dict[int, int], int, int]:
    station_pages: Dict[int, int] = {}
    struct_pages: Dict[int, int] = {}
    reg_max = DEFAULT_REGIONS_WORKERS_MAX
    str_max = DEFAULT_STRUCTS_WORKERS_MAX
    if cache:
        reg_max = int(cache.planner.REGIONS_WORKERS_MAX)
        str_max = int(cache.planner.STRUCTS_WORKERS_MAX)
        for s in cache.stations:
            station_pages[s.regionID] = s.pages
        for s in cache.structures:
            struct_pages[s.stationID] = s.pages
    return station_pages, struct_pages, reg_max, str_max

# ----------------------------
# Hubs output
# ----------------------------

def resolve_station_name(location_id: int, stations_by_id: Dict[int, str], structures_by_id: Dict[int, str]) -> str:
    if STATION_MIN_ID <= location_id < STATION_MAX_ID:
        return stations_by_id.get(location_id, "")
    if location_id >= STATION_MAX_ID:
        return structures_by_id.get(location_id, "")
    return ""

def build_hubs_rows(
    hubs: Dict[int, HubAgg],
    stations_by_id: Dict[int, str],
    structures_by_id: Dict[int, str],
) -> List[Dict[str, Any]]:
    sortable: List[Tuple[int, int, float, int]] = []
    for loc, agg in hubs.items():
        if agg.orders <= 0:
            continue
        sortable.append((agg.orders, len(agg.types), agg.value_sum, loc))

    sortable.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))

    rows: List[Dict[str, Any]] = []
    for orders, types_cnt, _value_sum, loc in sortable:
        rows.append({
            "stationID": int(loc),
            "station": resolve_station_name(int(loc), stations_by_id, structures_by_id),
            "orders": int(orders),
            "types": int(types_cnt),
        })
    return rows

# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ensure_dir(OUT_DIR)

    vlog(event="phase", msg="loading indices...")

    regions = read_jsonl_gz(SDE_REGIONS_PATH)
    region_ids = [int(r["regionID"]) for r in regions if "regionID" in r]
    region_name_by_id = {int(r["regionID"]): str(r.get("region", "")) for r in regions if "regionID" in r}

    stations = read_jsonl_gz(SDE_STATIONS_PATH)
    stations_by_id = {int(s["stationID"]): str(s.get("station", "")) for s in stations if "stationID" in s}

    structures = read_jsonl_gz(STRUCTURES_FILE)
    structure_ids = [int(s["stationID"]) for s in structures if "stationID" in s]
    structures_by_id = {int(s["stationID"]): str(s.get("station", "")) for s in structures if "stationID" in s}

    vlog(event="phase", msg=f"regions_count={len(region_ids)} structures_market_count={len(structure_ids)}")

    tokens = merge_tokens(EOU_ACCESS_TOKENS_1, EOU_ACCESS_TOKENS_2)
    rotator = TokenRotator(tokens, PRIMARY_CHAR_ID) if tokens else None
    vlog(event="phase", msg=f"tokens_count={len(tokens)} primary_char_id={PRIMARY_CHAR_ID}")

    cache0 = load_pages_cache(PAGES_CACHE_PATH)
    station_pages_cache, struct_pages_cache, reg_max_current, str_max_current = build_cache_maps(cache0)
    reg_max_current, str_max_current = enforce_sum_limit(reg_max_current, str_max_current)

    region_items: List[Tuple[int, int]] = [(int(station_pages_cache.get(rid, 0)), rid) for rid in region_ids]
    struct_items: List[Tuple[int, int]] = [(int(struct_pages_cache.get(sid, 0)), sid) for sid in structure_ids]

    total_region_pages = sum(w for w, _ in region_items)
    total_struct_pages = sum(w for w, _ in struct_items)

    reg_workers = calc_recommended_workers(total_region_pages, REGIONS_TARGET_PAGES_PER_WORKER, REGIONS_WORKERS_MIN, reg_max_current)
    str_workers = calc_recommended_workers(total_struct_pages, STRUCTS_TARGET_PAGES_PER_WORKER, STRUCTS_WORKERS_MIN, str_max_current)
    reg_workers, str_workers = enforce_sum_limit(reg_workers, str_workers)

    vlog(event="phase", msg=f"planner regions_total_pages_cached={total_region_pages} workers={reg_workers} max={reg_max_current} min={REGIONS_WORKERS_MIN}")
    vlog(event="phase", msg=f"planner structs_total_pages_cached={total_struct_pages} workers={str_workers} max={str_max_current} min={STRUCTS_WORKERS_MIN}")

    reg_assign = greedy_balance(region_items, reg_workers)
    str_assign = greedy_balance(struct_items, str_workers)

    budget = GlobalBudget(RETRY_BUDGET)

    max_expires_epoch = 0
    max_last_modified_epoch = 0

    region_pages_observed: Dict[int, int] = {}
    struct_pages_observed: Dict[int, int] = {}
    ignored_structures: set[int] = set()

    total_raw_seen = 0
    total_unique_emitted = 0

    hubs_global: Dict[int, HubAgg] = {}
    hubs_lock = threading.Lock()

    def merge_hubs_local(local: Dict[int, HubAgg]) -> None:
        with hubs_lock:
            for loc, agg in local.items():
                cur = hubs_global.get(loc)
                if cur is None:
                    hubs_global[loc] = HubAgg(orders=agg.orders, types=set(agg.types), value_sum=float(agg.value_sum))
                else:
                    cur.orders += agg.orders
                    cur.types |= agg.types
                    cur.value_sum += agg.value_sum

    def run_region_worker(worker_idx: int, ids: List[int]) -> List[EntityResult]:
        label = f"regions_w{worker_idx+1}/{reg_workers}"
        vlog(event="worker_start", worker=label, kind="REGION", msg=f"entities={len(ids)}")

        seen_order_ids_worker: set[int] = set()
        hubs_local: Dict[int, HubAgg] = {}

        out: List[EntityResult] = []
        for rid in ids:
            res = fetch_entity_pages(
                entity_kind="REGION",
                entity_id=rid,
                entity_name=region_name_by_id.get(rid, ""),
                rotator=None,
                budget=budget,
                worker_label=label,
                seen_order_ids_worker=seen_order_ids_worker,
                hubs_worker=hubs_local,
            )
            out.append(res)

        merge_hubs_local(hubs_local)

        vlog(event="worker_done", worker=label, kind="REGION", msg=f"done unique_orders={len(seen_order_ids_worker)}")
        return out

    def run_struct_worker(worker_idx: int, ids: List[int]) -> List[EntityResult]:
        label = f"structs_w{worker_idx+1}/{str_workers}"
        vlog(event="worker_start", worker=label, kind="STRUCTURE", msg=f"entities={len(ids)}")

        seen_order_ids_worker: set[int] = set()
        hubs_local: Dict[int, HubAgg] = {}

        out: List[EntityResult] = []
        for sid in ids:
            res = fetch_entity_pages(
                entity_kind="STRUCTURE",
                entity_id=sid,
                entity_name=structures_by_id.get(sid, ""),
                rotator=rotator,
                budget=budget,
                worker_label=label,
                seen_order_ids_worker=seen_order_ids_worker,
                hubs_worker=hubs_local,
            )
            out.append(res)

        merge_hubs_local(hubs_local)

        vlog(event="worker_done", worker=label, kind="STRUCTURE", msg=f"done unique_orders={len(seen_order_ids_worker)}")
        return out

    vlog(event="phase", msg="phase=workers start")
    t0 = time.time()

    futures = []
    results: List[EntityResult] = []
    with ThreadPoolExecutor(max_workers=(reg_workers + str_workers)) as ex:
        for i, ids in enumerate(reg_assign):
            futures.append(ex.submit(run_region_worker, i, ids))
        for i, ids in enumerate(str_assign):
            futures.append(ex.submit(run_struct_worker, i, ids))

        for fut in as_completed(futures):
            part = fut.result()
            results.extend(part)

    vlog(event="phase", msg=f"phase=workers end elapsed_s={time.time()-t0:.2f}")

    for r in results:
        max_expires_epoch = max(max_expires_epoch, r.max_expires_epoch)
        max_last_modified_epoch = max(max_last_modified_epoch, r.max_last_modified_epoch)

        total_raw_seen += r.rows_seen
        total_unique_emitted += r.rows_emitted

        if r.entity_kind == "REGION":
            pages_val = int(r.xpages) if (r.had_xpages and r.xpages is not None) else int(r.pages_ok)
            region_pages_observed[r.entity_id] = pages_val
        else:
            if r.ignored:
                ignored_structures.add(r.entity_id)
            else:
                pages_val = int(r.xpages) if (r.had_xpages and r.xpages is not None) else int(r.pages_ok)
                struct_pages_observed[r.entity_id] = pages_val

    vlog(event="phase", msg=f"ingest_summary raw_seen={total_raw_seen} unique_emitted_worker_deduped={total_unique_emitted}")

    new_stations = [
        CacheStation(regionID=rid, region=region_name_by_id.get(rid, ""), pages=int(region_pages_observed.get(rid, 0)))
        for rid in sorted(region_ids)
    ]
    new_structs: List[CacheStructure] = []
    for sid in sorted(structure_ids):
        if sid in ignored_structures:
            continue
        new_structs.append(
            CacheStructure(stationID=sid, station=structures_by_id.get(sid, ""), pages=int(struct_pages_observed.get(sid, 0)))
        )

    observed_region_pages_total = sum(s.pages for s in new_stations)
    observed_struct_pages_total = sum(s.pages for s in new_structs)

    rec_reg_max = int(math.ceil(observed_region_pages_total / float(REGIONS_TARGET_PAGES_PER_WORKER))) if observed_region_pages_total > 0 else REGIONS_WORKERS_MIN
    rec_str_max = int(math.ceil(observed_struct_pages_total / float(STRUCTS_TARGET_PAGES_PER_WORKER))) if observed_struct_pages_total > 0 else STRUCTS_WORKERS_MIN

    updated_reg_max = update_max_slow(current_max=reg_max_current, recommended_max=rec_reg_max, min_workers=REGIONS_WORKERS_MIN)
    updated_str_max = update_max_slow(current_max=str_max_current, recommended_max=rec_str_max, min_workers=STRUCTS_WORKERS_MIN)

    updated_reg_max = max(updated_reg_max, REGIONS_WORKERS_MIN * 2)
    updated_str_max = max(updated_str_max, STRUCTS_WORKERS_MIN * 2)
    updated_reg_max, updated_str_max = enforce_sum_limit(updated_reg_max, updated_str_max)

    vlog(event="phase", msg=f"planner_update reg_max_current={reg_max_current} reg_max_recommended={rec_reg_max} reg_max_next={updated_reg_max}")
    vlog(event="phase", msg=f"planner_update str_max_current={str_max_current} str_max_recommended={rec_str_max} str_max_next={updated_str_max}")

    new_cache = PagesCache(
        planner=CachePlanner(REGIONS_WORKERS_MAX=updated_reg_max, STRUCTS_WORKERS_MAX=updated_str_max),
        stations=new_stations,
        structures=new_structs,
    )

    write_pages_cache_atomic(PAGES_CACHE_PATH, new_cache)
    vlog(event="phase", msg=f"cache_written path={PAGES_CACHE_PATH} stations={len(new_stations)} structures={len(new_structs)}")

    hubs_rows = build_hubs_rows(hubs_global, stations_by_id, structures_by_id)
    write_jsonl_atomic(HUBS_PATH, hubs_rows)
    vlog(event="phase", msg=f"hubs_written path={HUBS_PATH} rows={len(hubs_rows)}")

    print(f"max_expires_epoch={max_expires_epoch}", flush=True)
    print(f"max_last_modified_epoch={max_last_modified_epoch}", flush=True)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        vlog(event="fatal", msg=str(e))
        raise
