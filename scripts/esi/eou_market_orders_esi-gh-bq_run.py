#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EOU · Market Orders (ESI → GH Actions) — Runner

Cambios solicitados:
- Ingesta "raw": por cada orden se normalizan y conservan SOLO estos campos:
  * duration (int64)
  * is_buy_order (bool)
  * issued (string date-time)
  * location_id (int64)
  * min_volume (int64)
  * order_id (int64)
  * price (double)
  * range (int64)  <-- conversión especial desde string
  * system_id (int64)
  * type_id (int64)
  * volume_remain (int64)
  * volume_total (int64)

- Conversión range:
  * "station" o "solarsystem" -> 0
  * "region" -> 1000
  * otro -> int(string)

- Dedupe "order_id" por worker (streaming): seen_order_ids global dentro del worker
- No se comitea ningún resultado de órdenes (solo el cache pages del planner)
- Logs mínimos (worker/entity start/end), sin logs por page
- Mantiene EXACTAMENTE la máquina de estados A/B y la política de budget global
"""

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
STRUCTURES_FILE = os.environ.get("STRUCTURES_FILE", "data/esi/structures.jsonl.gz")
PAGES_CACHE_PATH = os.environ.get("PAGES_CACHE_PATH", "states/market_orders_pages.json")

OUT_DIR = os.environ.get("OUT_DIR", ".tmp_eou_market_orders")

EOU_ACCESS_TOKENS_1 = os.environ.get("EOU_ACCESS_TOKENS_1", "")
EOU_ACCESS_TOKENS_2 = os.environ.get("EOU_ACCESS_TOKENS_2", "")

# Defaults if no cache exists (E)
DEFAULT_REGIONS_WORKERS_MAX = 8
DEFAULT_STRUCTS_WORKERS_MAX = 3

# Restriction global (E)
MAX_WORKERS_SUM_LIMIT = 13

USER_AGENT = os.environ.get("ESI_USER_AGENT", "EOU/market-orders (+https://github.com/landu-eou/eou)")

# ----------------------------
# Minimal visual logs (worker/entity only)
# ----------------------------

_START_EPOCH = time.time()
_PRINT_LOCK = threading.Lock()

def _rel_ts() -> str:
    return f"{time.time() - _START_EPOCH:7.2f}s"

def _tid() -> str:
    return f"t{threading.get_ident() % 10000:04d}"

def _icon(event: str) -> str:
    if event == "phase":
        return "🧭"
    if event == "worker_start":
        return "🚀"
    if event == "worker_done":
        return "🏁"
    if event == "entity_start":
        return "📦"
    if event == "entity_done":
        return "✅"
    if event == "entity_ignored":
        return "⏭️"
    if event == "retry":
        return "🔁"
    if event == "budget":
        return "🎛️"
    if event == "fatal":
        return "💥"
    return "•"

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
# Global auth retry budget (shared between workers)
# ----------------------------

class GlobalBudget:
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

        if remaining < 0:
            raise RuntimeError("RETRY_BUDGET internal underflow")
        if remaining == 0:
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated/retry requests (401/420/429/5xx).")

    def remaining(self) -> int:
        with self._lock:
            return self._remaining

# ----------------------------
# Token rotation (exact policy)
# ----------------------------

class TokenRotator:
    """
    Order:
      1) PRIMARY_CHAR_ID first if exists
      2) rest char_ids sorted DESC (numeric) excluding primary
    """
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
# IO: read jsonl.gz
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

# ----------------------------
# Cache model (D/E)
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
# Planning helpers
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

# ----------------------------
# HTTP helpers (A/B)
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
    # ESI uses RFC1123-like, e.g. "Sun, 01 Feb 2026 22:44:34 GMT"
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
    # ESI typically returns range as string enum: "station", "solarsystem", "region", or numeric strings.
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

def normalize_order_raw(o: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Devuelve un dict con SOLO los campos raw requeridos y con tipos normalizados.
    Si falta algún campo crítico o la conversión falla, devuelve None (se descarta).
    """
    try:
        r_int = normalize_range_to_int64(o.get("range"))
        if r_int is None:
            return None

        out = {
            "duration": int(o.get("duration")),
            "is_buy_order": bool(o.get("is_buy_order")),
            "issued": str(o.get("issued")),
            "location_id": int(o.get("location_id")),
            "min_volume": int(o.get("min_volume")),
            "order_id": int(o.get("order_id")),
            "price": float(o.get("price")),
            "range": int(r_int),
            "system_id": int(o.get("system_id")),
            "type_id": int(o.get("type_id")),
            "volume_remain": int(o.get("volume_remain")),
            "volume_total": int(o.get("volume_total")),
        }
        return out
    except Exception:
        return None

# ----------------------------
# Entity fetching with exact state machine (A/B)
# - NO logs per page
# - Dedupe por worker (C): seen_order_ids es compartido
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
    rows_emitted: int   # unique orders kept (dedupe)
    rows_seen: int      # orders parsed (before dedupe/skips)

def fetch_entity_pages(
    *,
    entity_kind: str,
    entity_id: int,
    entity_name: str,
    rotator: Optional[TokenRotator],
    budget: GlobalBudget,
    worker_label: str,
    seen_order_ids_worker: set,
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
        # Build URL + headers
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
            headers = {"Accept": "application/json", "User-Agent": USER_AGENT, "Authorization": f"Bearer {tok}"}

        params = {"datasource": ESI_DATASOURCE, "page": page}

        try:
            resp = http_get(url, headers=headers, params=params, timeout_s=60)
        except requests.RequestException as e:
            # tratar como retryable (equivalente a 5xx) bajo regla (4)
            budget.consume_or_fail("net_err", entity_kind, entity_id, page, active_char, worker_label)
            sleep_s = 30
            vlog(
                event="retry",
                worker=worker_label,
                kind=entity_kind,
                entity_id=entity_id,
                entity_name=entity_name,
                msg=f"reason=net_err page={page} sleep_s={sleep_s} err={type(e).__name__}",
            )
            time.sleep(sleep_s)
            continue

        status = resp.status_code

        # Track maxima of Expires/Last-Modified (para finalize)
        exp_e = take_expires_epoch(resp) or 0
        lm_e = take_last_modified_epoch(resp) or 0
        if exp_e > max_expires_epoch:
            max_expires_epoch = exp_e
        if lm_e > max_last_modified_epoch:
            max_last_modified_epoch = lm_e

        xpages_h = header_int(resp, "X-Pages")

        # ----------------------------
        # B) rules by status
        # ----------------------------

        if status == 200:
            pages_ok += 1

            # Parse
            try:
                body = resp.json()
                if not isinstance(body, list):
                    body = []
            except Exception:
                body = []

            # Raw normalize + dedupe por worker
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

                # NO se guarda ni se comitea nada (solo se "trata" en crudo)
                # Si en el futuro quieres emitir a disco temporal, aquí sería el sitio.

            # X-Pages logic (A/B(1))
            if xpages_h is not None:
                had_xpages = True
                xpages = xpages_h
                if page < xpages:
                    page += 1
                    continue
                if page == xpages:
                    break  # FIN without asking page+1
                break

            # No X-Pages => keep incrementing until collision
            page += 1
            continue

        if status == 404:
            if entity_kind == "REGION":
                break

            # STRUCTURE
            if page == 1:
                if attempts_404_page1 < 1:
                    attempts_404_page1 += 1
                    sleep_s = 5
                    vlog(
                        event="retry",
                        worker=worker_label,
                        kind=entity_kind,
                        entity_id=entity_id,
                        entity_name=entity_name,
                        msg=f"reason=404_page1_retry page={page} sleep_s={sleep_s}",
                    )
                    time.sleep(sleep_s)
                    continue

                vlog(
                    event="entity_ignored",
                    worker=worker_label,
                    kind=entity_kind,
                    entity_id=entity_id,
                    entity_name=entity_name,
                    msg="reason=404_page1_after_retry",
                )
                return EntityResult(
                    entity_kind=entity_kind,
                    entity_id=entity_id,
                    pages_ok=0,
                    had_xpages=False,
                    xpages=None,
                    ignored=True,
                    max_expires_epoch=max_expires_epoch,
                    max_last_modified_epoch=max_last_modified_epoch,
                    rows_emitted=0,
                    rows_seen=rows_seen,
                )

            # page > 1 => end pagination
            break

        if status == 401:
            if entity_kind != "STRUCTURE":
                raise RuntimeError(f"Unexpected 401 for REGION entity_id={entity_id} page={page}")

            cid_before, _ = rotator.current() if rotator else (None, None)
            budget.consume_or_fail("401", entity_kind, entity_id, page, cid_before, worker_label)
            cid_new, _ = rotator.rotate_next() if rotator else (None, None)
            sleep_s = 5
            vlog(
                event="retry",
                worker=worker_label,
                kind=entity_kind,
                entity_id=entity_id,
                entity_name=entity_name,
                msg=f"reason=401 rotate {cid_before}->{cid_new} page={page} sleep_s={sleep_s}",
            )
            time.sleep(sleep_s)
            continue

        if status in (420, 429) or (500 <= status <= 599):
            ra = parse_retry_after_seconds(resp)
            sleep_s = ra if (ra is not None and ra > 0) else 30
            budget.consume_or_fail(str(status), entity_kind, entity_id, page, (rotator.current()[0] if rotator else None), worker_label)
            vlog(
                event="retry",
                worker=worker_label,
                kind=entity_kind,
                entity_id=entity_id,
                entity_name=entity_name,
                msg=f"reason={status} page={page} sleep_s={sleep_s}",
            )
            time.sleep(sleep_s)
            continue

        if 400 <= status <= 499:
            # other 4xx
            if entity_kind == "STRUCTURE":
                vlog(
                    event="entity_ignored",
                    worker=worker_label,
                    kind=entity_kind,
                    entity_id=entity_id,
                    entity_name=entity_name,
                    msg=f"reason=other4xx status={status} page={page}",
                )
                return EntityResult(
                    entity_kind=entity_kind,
                    entity_id=entity_id,
                    pages_ok=0,
                    had_xpages=had_xpages,
                    xpages=xpages,
                    ignored=True,
                    max_expires_epoch=max_expires_epoch,
                    max_last_modified_epoch=max_last_modified_epoch,
                    rows_emitted=0,
                    rows_seen=rows_seen,
                )
            raise RuntimeError(f"Unexpected {status} for REGION entity_id={entity_id} page={page}")

        raise RuntimeError(f"Unexpected status={status} kind={entity_kind} entity_id={entity_id} page={page}")

    vlog(
        event="entity_done",
        worker=worker_label,
        kind=entity_kind,
        entity_id=entity_id,
        entity_name=entity_name,
        msg=f"pages_ok={pages_ok} had_xpages={had_xpages} xpages={xpages} raw_seen={rows_seen} unique_emitted={rows_emitted}",
    )

    return EntityResult(
        entity_kind=entity_kind,
        entity_id=entity_id,
        pages_ok=pages_ok,
        had_xpages=had_xpages,
        xpages=xpages,
        ignored=False,
        max_expires_epoch=max_expires_epoch,
        max_last_modified_epoch=max_last_modified_epoch,
        rows_emitted=rows_emitted,
        rows_seen=rows_seen,
    )

# ----------------------------
# MAX adaptation (E) with anti-oscillation
# ----------------------------

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
# Main
# ----------------------------

def main() -> int:
    ensure_dir(OUT_DIR)

    vlog(event="phase", msg="loading indices...")

    regions = read_jsonl_gz(SDE_REGIONS_PATH)
    region_ids = [int(r["regionID"]) for r in regions if "regionID" in r]
    region_name_by_id = {int(r["regionID"]): str(r.get("region", "")) for r in regions if "regionID" in r}

    structures = read_jsonl_gz(STRUCTURES_FILE)
    structure_ids = [int(s["stationID"]) for s in structures if "stationID" in s]
    structure_name_by_id = {int(s["stationID"]): str(s.get("station", "")) for s in structures if "stationID" in s}

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

    # métricas para sanity-check
    total_raw_seen = 0
    total_unique_emitted = 0

    def run_region_worker(worker_idx: int, ids: List[int]) -> List[EntityResult]:
        label = f"regions_w{worker_idx+1}/{reg_workers}"
        vlog(event="worker_start", worker=label, kind="REGION", msg=f"entities={len(ids)}")

        seen_order_ids_worker: set[int] = set()

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
            )
            out.append(res)

        vlog(event="worker_done", worker=label, kind="REGION", msg=f"done unique_orders={len(seen_order_ids_worker)}")
        return out

    def run_struct_worker(worker_idx: int, ids: List[int]) -> List[EntityResult]:
        label = f"structs_w{worker_idx+1}/{str_workers}"
        vlog(event="worker_start", worker=label, kind="STRUCTURE", msg=f"entities={len(ids)}")

        seen_order_ids_worker: set[int] = set()

        out: List[EntityResult] = []
        for sid in ids:
            res = fetch_entity_pages(
                entity_kind="STRUCTURE",
                entity_id=sid,
                entity_name=structure_name_by_id.get(sid, ""),
                rotator=rotator,
                budget=budget,
                worker_label=label,
                seen_order_ids_worker=seen_order_ids_worker,
            )
            out.append(res)

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

    # Rebuild cache (exclude ignored structures)
    new_stations = [
        CacheStation(regionID=rid, region=region_name_by_id.get(rid, ""), pages=int(region_pages_observed.get(rid, 0)))
        for rid in sorted(region_ids)
    ]
    new_structs: List[CacheStructure] = []
    for sid in sorted(structure_ids):
        if sid in ignored_structures:
            continue
        new_structs.append(
            CacheStructure(stationID=sid, station=structure_name_by_id.get(sid, ""), pages=int(struct_pages_observed.get(sid, 0)))
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

    # Exports for finalize step
    print(f"max_expires_epoch={max_expires_epoch}", flush=True)
    print(f"max_last_modified_epoch={max_last_modified_epoch}", flush=True)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        vlog(event="fatal", msg=str(e))
        raise
