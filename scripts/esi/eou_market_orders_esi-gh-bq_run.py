#!/usr/bin/env python3
import gzip
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests import Response

# =========================
# Config / constants
# =========================

GLOBAL_WORKERS_SUM_LIMIT = 13

# No optimizaciones agresivas: page-by-page secuencial dentro de cada entidad.
REQUEST_TIMEOUT = 30  # seconds
SLEEP_AUTH_RETRY_SECONDS = 30
SLEEP_ERROR_RETRY_SECONDS = 30
SLEEP_STRUCTURE_404_PAGE1_RETRY_MS = 5000

UA = "EOU/market-orders (github-actions)"

# =========================
# Helpers
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def epoch_seconds(dt: datetime) -> int:
    return int(dt.timestamp())

def parse_http_date_to_epoch(value: str) -> Optional[int]:
    # ESI uses RFC1123 dates (e.g. Sun, 01 Feb 2026 22:44:34 GMT)
    try:
        dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
        dt = dt.replace(tzinfo=timezone.utc)
        return epoch_seconds(dt)
    except Exception:
        return None

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def read_gz_jsonl(path: str) -> Iterable[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def log(msg: str) -> None:
    print(msg, flush=True)

def must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env: {name}")
    return v

def opt_env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def bsearch_excluded(excluded_sorted: List[int], type_id: int) -> bool:
    lo, hi = 0, len(excluded_sorted) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        v = excluded_sorted[mid]
        if v == type_id:
            return True
        if v < type_id:
            lo = mid + 1
        else:
            hi = mid - 1
    return False

def parse_range_to_int(r: Any) -> int:
    if r is None:
        return 0
    if isinstance(r, int):
        return r
    s = str(r).strip().lower()
    if s in ("station", "solarsystem"):
        return 0
    if s == "region":
        return 1000
    try:
        return int(s)
    except Exception:
        return 0

def compute_until_and_timeleft(issued_iso: str, duration_days: int) -> Tuple[str, str]:
    issued = datetime.fromisoformat(issued_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    until = issued + timedelta(days=int(duration_days))
    now = utc_now()
    delta = until - now
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = total_seconds % 60
    timeleft = f"{hh:02d}:{mm:02d}:{ss:02d}"
    return until.isoformat().replace("+00:00", "Z"), timeleft

# =========================
# Tokens (EXACT policy)
# =========================

def load_tokens_from_secrets(primary_char_id: int) -> List[Tuple[int, str]]:
    s1 = os.environ.get("EOU_ACCESS_TOKENS_1", "") or "{}"
    s2 = os.environ.get("EOU_ACCESS_TOKENS_2", "") or "{}"
    try:
        d1 = json.loads(s1)
        d2 = json.loads(s2)
    except Exception as e:
        raise RuntimeError(f"Failed to parse EOU_ACCESS_TOKENS_1/2 as JSON: {e}")

    merged: Dict[int, str] = {}
    for k, v in {**d1, **d2}.items():
        try:
            cid = int(k)
        except Exception:
            continue
        if isinstance(v, str) and v:
            merged[cid] = v

    ordered: List[Tuple[int, str]] = []
    if primary_char_id in merged:
        ordered.append((primary_char_id, merged[primary_char_id]))
    rest = sorted([cid for cid in merged.keys() if cid != primary_char_id], reverse=True)
    for cid in rest:
        ordered.append((cid, merged[cid]))  # <-- FIX

    return ordered

# =========================
# Global retry budget (shared)
# =========================

class RetryBudget:
    def __init__(self, initial: int):
        self.remaining = initial

    def consume_or_fail(self, reason: str, entity_kind: str, entity_id: int, page: int, char_id: Optional[int]) -> None:
        self.remaining -= 1
        log(f"auth_budget_consume reason={reason} entity_kind={entity_kind} entity_id={entity_id} page={page} char_id={char_id} remaining_budget={self.remaining}")
        if self.remaining < 0:
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated/error retries (401/420/429/5xx).")

# =========================
# Cache structure
# =========================

@dataclass
class CachePlanner:
    REGIONS_WORKERS_MAX: int
    STRUCTS_WORKERS_MAX: int

@dataclass
class StationCacheEntry:
    regionID: int
    region: str
    pages: int

@dataclass
class StructureCacheEntry:
    stationID: int
    station: str
    pages: int

@dataclass
class PagesCache:
    planner: CachePlanner
    stations: List[StationCacheEntry]
    structures: List[StructureCacheEntry]

def default_cache() -> PagesCache:
    return PagesCache(
        planner=CachePlanner(REGIONS_WORKERS_MAX=8, STRUCTS_WORKERS_MAX=3),
        stations=[],
        structures=[],
    )

def load_pages_cache(path: str) -> PagesCache:
    if not os.path.exists(path):
        return default_cache()
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    planner_raw = raw.get("planner", {}) if isinstance(raw, dict) else {}
    regions_max = safe_int(planner_raw.get("REGIONS_WORKERS_MAX", 8), 8)
    structs_max = safe_int(planner_raw.get("STRUCTS_WORKERS_MAX", 3), 3)

    stations_raw = raw.get("stations", []) if isinstance(raw, dict) else []
    structures_raw = raw.get("structures", []) if isinstance(raw, dict) else []

    stations: List[StationCacheEntry] = []
    for it in stations_raw:
        try:
            stations.append(StationCacheEntry(
                regionID=int(it["regionID"]),
                region=str(it.get("region", "")),
                pages=int(it.get("pages", 0)),
            ))
        except Exception:
            continue

    structures: List[StructureCacheEntry] = []
    for it in structures_raw:
        try:
            structures.append(StructureCacheEntry(
                stationID=int(it["stationID"]),
                station=str(it.get("station", "")),
                pages=int(it.get("pages", 0)),
            ))
        except Exception:
            continue

    return PagesCache(
        planner=CachePlanner(REGIONS_WORKERS_MAX=regions_max, STRUCTS_WORKERS_MAX=structs_max),
        stations=stations,
        structures=structures,
    )

def write_pages_cache_pretty_atomic(path: str, cache: PagesCache) -> None:
    tmp = path + ".tmp"
    obj = {
        "planner": {
            "REGIONS_WORKERS_MAX": cache.planner.REGIONS_WORKERS_MAX,
            "STRUCTS_WORKERS_MAX": cache.planner.STRUCTS_WORKERS_MAX,
        },
        "stations": [
            {"regionID": e.regionID, "region": e.region, "pages": e.pages}
            for e in cache.stations
        ],
        "structures": [
            {"stationID": e.stationID, "station": e.station, "pages": e.pages}
            for e in cache.structures
        ],
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")
    os.replace(tmp, path)

# =========================
# Worker planning
# =========================

def compute_workers_needed(total_pages: int, min_workers: int, max_workers: int, target_pages_per_worker: int) -> int:
    if target_pages_per_worker <= 0:
        return max_workers
    needed = max(1, math.ceil(total_pages / target_pages_per_worker))
    return max(min_workers, min(max_workers, needed))

def greedy_balance(items: List[Tuple[int, Any]], k: int) -> List[List[Any]]:
    if k <= 0:
        return []
    bins: List[List[Any]] = [[] for _ in range(k)]
    loads: List[int] = [0 for _ in range(k)]
    items_sorted = sorted(items, key=lambda x: x[0], reverse=True)
    for w, payload in items_sorted:
        idx = loads.index(min(loads))
        bins[idx].append(payload)
        loads[idx] += w
    return bins

# =========================
# HTTP (ESI)
# =========================

def esi_get(session: requests.Session, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Response:
    return session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

def parse_expires_epoch(resp: Response) -> Optional[int]:
    exp = resp.headers.get("Expires") or resp.headers.get("expires")
    if not exp:
        return None
    return parse_http_date_to_epoch(exp)

def parse_last_modified_epoch(resp: Response) -> Optional[int]:
    lm = resp.headers.get("Last-Modified") or resp.headers.get("last-modified")
    if not lm:
        return None
    return parse_http_date_to_epoch(lm)

def parse_xpages(resp: Response) -> Optional[int]:
    xp = resp.headers.get("X-Pages") or resp.headers.get("x-pages")
    if not xp:
        return None
    try:
        return int(xp)
    except Exception:
        return None

def sleep_retry_after_or_default(resp: Response, default_seconds: int) -> None:
    ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
    if ra:
        try:
            sec = int(ra)
            if sec < 0:
                sec = default_seconds
            time.sleep(sec)
            return
        except Exception:
            pass
    time.sleep(default_seconds)

# =========================
# Entity loop state machine (A/B)
# =========================

@dataclass
class EntityResult:
    pages_ok: int
    had_xpages: bool
    xpages: int
    ignored: bool
    max_expires_epoch: int
    max_last_modified_epoch: int

def fetch_entity_pages(
    session: requests.Session,
    entity_kind: str,
    entity_id: int,
    entity_name: str,
    url: str,
    auth_tokens: Optional[List[Tuple[int, str]]],
    retry_budget: RetryBudget,
    is_authed: bool,
    dedupe_seen: set,
    out_rows: List[dict],
    excluded_type_ids_sorted: List[int],
    type_id_to_name: Dict[int, str],
    station_id_to_name: Dict[int, str],
    structure_id_to_name: Dict[int, str],
    solar_system_id_to_meta: Dict[int, Tuple[str, str, str]],
    jita44_location_id: int,
    jita44_buy_rows: List[dict],
    jita44_sell_rows: List[dict],
) -> EntityResult:
    page = 1
    pages_ok = 0
    had_xpages = False
    xpages_val = 0
    attempts_404_page1 = 0

    token_idx = 0
    current_char_id: Optional[int] = None
    current_token: Optional[str] = None
    if is_authed:
        if not auth_tokens:
            raise RuntimeError("Authenticated endpoint requested but no tokens available.")
        current_char_id, current_token = auth_tokens[token_idx]

    max_expires_epoch = 0
    max_last_modified_epoch = 0

    def build_headers() -> Dict[str, str]:
        h = {
            "User-Agent": UA,
            "Accept": "application/json",
        }
        if is_authed and current_token:
            h["Authorization"] = f"Bearer {current_token}"
        return h

    while True:
        params = {"datasource": os.environ.get("ESI_DATASOURCE", "tranquility"), "page": page}

        resp = esi_get(session, url, build_headers(), params)
        status = resp.status_code

        exp_epoch = parse_expires_epoch(resp) or 0
        lm_epoch = parse_last_modified_epoch(resp) or 0
        if exp_epoch > max_expires_epoch:
            max_expires_epoch = exp_epoch
        if lm_epoch > max_last_modified_epoch:
            max_last_modified_epoch = lm_epoch

        if status == 200:
            pages_ok += 1
            xp = parse_xpages(resp)
            if xp is not None:
                had_xpages = True
                xpages_val = xp

            try:
                items = resp.json()
                if not isinstance(items, list):
                    items = []
            except Exception:
                items = []

            for o in items:
                try:
                    order_id = int(o.get("order_id"))
                except Exception:
                    continue

                if order_id in dedupe_seen:
                    continue
                dedupe_seen.add(order_id)

                try:
                    type_id = int(o.get("type_id"))
                except Exception:
                    continue
                if bsearch_excluded(excluded_type_ids_sorted, type_id):
                    continue

                is_buy = bool(o.get("is_buy_order", False))

                issued = str(o.get("issued"))
                duration = safe_int(o.get("duration", 0), 0)
                until_iso, time_left = compute_until_and_timeleft(issued, duration)

                loc_id = safe_int(o.get("location_id", 0), 0)
                sys_id = safe_int(o.get("system_id", 0), 0)

                type_name = type_id_to_name.get(type_id, str(type_id))
                solarSystem, constellation, region = solar_system_id_to_meta.get(sys_id, ("", "", ""))

                if loc_id < 1000000000000:
                    station_name = station_id_to_name.get(loc_id, str(loc_id))
                else:
                    station_name = structure_id_to_name.get(loc_id, str(loc_id))

                row_common = {
                    "type": type_name,
                    "orderPrice": float(o.get("price", 0.0)),
                    "volRemain": safe_int(o.get("volume_remain", 0), 0),
                    "volTotal": safe_int(o.get("volume_total", 0), 0),
                    "volMin": safe_int(o.get("min_volume", 0), 0),
                    "station": station_name,
                    "solarSystem": solarSystem,
                    "constellation": constellation,
                    "region": region,
                    "issued": issued,
                    "until": until_iso,
                    "timeLeft": time_left,
                    "orderID": order_id,
                    "ordeRange": parse_range_to_int(o.get("range")) if "range" in o else 0,
                }

                if is_buy:
                    out_rows.append({**row_common, "is_buy_order": True})
                else:
                    out_rows.append({**row_common, "is_buy_order": False})

                if loc_id == jita44_location_id:
                    jita_row = {
                        "type": type_name,
                        "orderPrice": float(o.get("price", 0.0)),
                        "volRemain": safe_int(o.get("volume_remain", 0), 0),
                        "volTotal": safe_int(o.get("volume_total", 0), 0),
                        "volMin": safe_int(o.get("min_volume", 0), 0),
                        "issued": issued,
                        "until": until_iso,
                        "timeLeft": time_left,
                        "orderID": order_id,
                    }
                    if "range" in o:
                        jita_row["ordeRange"] = parse_range_to_int(o.get("range"))

                    if is_buy:
                        jita44_buy_rows.append(jita_row)
                    else:
                        jita44_sell_rows.append(jita_row)

            if had_xpages:
                log(f"entity_page status=200 kind={entity_kind} id={entity_id} page={page} xpages={xpages_val}")
                if page < xpages_val:
                    page += 1
                    continue
                if page == xpages_val:
                    return EntityResult(
                        pages_ok=pages_ok,
                        had_xpages=True,
                        xpages=xpages_val,
                        ignored=False,
                        max_expires_epoch=max_expires_epoch,
                        max_last_modified_epoch=max_last_modified_epoch,
                    )
                return EntityResult(pages_ok, True, xpages_val, False, max_expires_epoch, max_last_modified_epoch)

            log(f"entity_page status=200 kind={entity_kind} id={entity_id} page={page} xpages=none")
            page += 1
            continue

        if status == 404:
            log(f"entity_page status=404 kind={entity_kind} id={entity_id} page={page}")
            if entity_kind == "REGION":
                return EntityResult(pages_ok, had_xpages, xpages_val, False, max_expires_epoch, max_last_modified_epoch)

            if page == 1:
                if attempts_404_page1 == 0:
                    attempts_404_page1 += 1
                    time.sleep(SLEEP_STRUCTURE_404_PAGE1_RETRY_MS / 1000.0)
                    continue
                log(f"structure_ignored reason=404_page1 kind=STRUCTURE id={entity_id} name={entity_name}")
                return EntityResult(pages_ok, had_xpages, xpages_val, True, max_expires_epoch, max_last_modified_epoch)

            return EntityResult(pages_ok, had_xpages, xpages_val, False, max_expires_epoch, max_last_modified_epoch)

        if status == 401 and is_authed:
            retry_budget.consume_or_fail("401", entity_kind, entity_id, page, current_char_id)
            token_idx += 1
            if not auth_tokens:
                raise RuntimeError("No tokens available for 401 rotation.")
            if token_idx >= len(auth_tokens):
                token_idx = 0
            current_char_id, current_token = auth_tokens[token_idx]
            log(f"auth_retry=401 kind={entity_kind} id={entity_id} page={page} new_char_id={current_char_id}")
            time.sleep(SLEEP_AUTH_RETRY_SECONDS)
            continue

        if status in (420, 429) or (500 <= status <= 599):
            retry_budget.consume_or_fail(str(status), entity_kind, entity_id, page, current_char_id)
            log(f"retry status={status} kind={entity_kind} id={entity_id} page={page}")
            sleep_retry_after_or_default(resp, SLEEP_ERROR_RETRY_SECONDS)
            continue

        if 400 <= status <= 499:
            log(f"entity_page status={status} kind={entity_kind} id={entity_id} page={page}")
            if entity_kind == "STRUCTURE":
                log(f"structure_ignored reason=other_4xx status={status} id={entity_id} name={entity_name}")
                return EntityResult(pages_ok, had_xpages, xpages_val, True, max_expires_epoch, max_last_modified_epoch)
            raise RuntimeError(f"Unexpected 4xx for REGION {entity_id} page={page}: status={status}")

        raise RuntimeError(f"Unexpected status {status} for {entity_kind} {entity_id} page={page}")

# =========================
# Workers (no pipelining within entity)
# =========================

def worker_run_entities(
    worker_kind: str,
    entities: List[Tuple[int, str]],
    session: requests.Session,
    retry_budget: RetryBudget,
    auth_tokens: Optional[List[Tuple[int, str]]],
    excluded_type_ids_sorted: List[int],
    type_id_to_name: Dict[int, str],
    station_id_to_name: Dict[int, str],
    structure_id_to_name: Dict[int, str],
    solar_system_id_to_meta: Dict[int, Tuple[str, str, str]],
    jita44_location_id: int,
) -> Tuple[List[dict], List[dict], List[dict], Dict[int, int], Dict[int, int], int, int]:
    seen_order_ids = set()
    all_rows: List[dict] = []
    jita44_buy_rows: List[dict] = []
    jita44_sell_rows: List[dict] = []

    pages_by_region: Dict[int, int] = {}
    pages_by_structure: Dict[int, int] = {}

    max_expires_epoch = 0
    max_last_modified_epoch = 0

    for idx, (eid, name) in enumerate(entities, start=1):
        if worker_kind == "REGION":
            url = f"{os.environ.get('ESI_BASE_URL','https://esi.evetech.net')}/markets/{eid}/orders/"
            log(f"entity_start kind=REGION idx={idx}/{len(entities)} region_id={eid} region={name}")
            res = fetch_entity_pages(
                session=session,
                entity_kind="REGION",
                entity_id=eid,
                entity_name=name,
                url=url,
                auth_tokens=None,
                retry_budget=retry_budget,
                is_authed=False,
                dedupe_seen=seen_order_ids,
                out_rows=all_rows,
                excluded_type_ids_sorted=excluded_type_ids_sorted,
                type_id_to_name=type_id_to_name,
                station_id_to_name=station_id_to_name,
                structure_id_to_name=structure_id_to_name,
                solar_system_id_to_meta=solar_system_id_to_meta,
                jita44_location_id=jita44_location_id,
                jita44_buy_rows=jita44_buy_rows,
                jita44_sell_rows=jita44_sell_rows,
            )
            pages_effective = res.xpages if res.had_xpages else res.pages_ok
            pages_by_region[eid] = pages_effective
            log(f"entity_done kind=REGION region_id={eid} pages_ok={res.pages_ok} had_xpages={res.had_xpages} xpages={res.xpages}")

        else:
            url = f"{os.environ.get('ESI_BASE_URL','https://esi.evetech.net')}/markets/structures/{eid}/"
            log(f"entity_start kind=STRUCTURE idx={idx}/{len(entities)} structure_id={eid} station={name}")
            res = fetch_entity_pages(
                session=session,
                entity_kind="STRUCTURE",
                entity_id=eid,
                entity_name=name,
                url=url,
                auth_tokens=auth_tokens,
                retry_budget=retry_budget,
                is_authed=True,
                dedupe_seen=seen_order_ids,
                out_rows=all_rows,
                excluded_type_ids_sorted=excluded_type_ids_sorted,
                type_id_to_name=type_id_to_name,
                station_id_to_name=station_id_to_name,
                structure_id_to_name=structure_id_to_name,
                solar_system_id_to_meta=solar_system_id_to_meta,
                jita44_location_id=jita44_location_id,
                jita44_buy_rows=jita44_buy_rows,
                jita44_sell_rows=jita44_sell_rows,
            )
            if not res.ignored:
                pages_effective = res.xpages if res.had_xpages else res.pages_ok
                pages_by_structure[eid] = pages_effective
                log(f"entity_done kind=STRUCTURE structure_id={eid} pages_ok={res.pages_ok} had_xpages={res.had_xpages} xpages={res.xpages}")
            else:
                log(f"entity_done kind=STRUCTURE structure_id={eid} ignored=true pages_ok={res.pages_ok}")

        if res.max_expires_epoch > max_expires_epoch:
            max_expires_epoch = res.max_expires_epoch
        if res.max_last_modified_epoch > max_last_modified_epoch:
            max_last_modified_epoch = res.max_last_modified_epoch

    return (
        all_rows,
        jita44_buy_rows,
        jita44_sell_rows,
        pages_by_region,
        pages_by_structure,
        max_expires_epoch,
        max_last_modified_epoch,
    )

# =========================
# Workers MAX slow adaptation (E)
# =========================

def slow_update_max(
    current: int,
    recommended: int,
    min_workers: int,
    other_current: int,
    global_sum_limit: int = GLOBAL_WORKERS_SUM_LIMIT,
) -> int:
    current = max(current, min_workers * 2)
    recommended = max(recommended, min_workers)

    new_val = current

    if recommended > current:
        new_val = current + 1
    else:
        if recommended < (current / 4.0):
            cap = math.ceil(current / 2.0)
            new_val = max(current - cap, min_workers * 2)
        else:
            new_val = current

    if new_val + other_current > global_sum_limit:
        new_val = max(min_workers * 2, global_sum_limit - other_current)
    return new_val

# =========================
# Main
# =========================

def main() -> int:
    out_dir = must_env("OUT_DIR")
    pages_cache_path = must_env("PAGES_CACHE_PATH")

    sde_regions_path = must_env("SDE_REGIONS_PATH")
    sde_types_path = must_env("SDE_TYPES_PATH")
    sde_excluded_path = must_env("SDE_EXCLUDED_TYPES_PATH")
    sde_stations_path = must_env("SDE_STATIONS_PATH")
    sde_solarsystems_path = must_env("SDE_SOLARSYSTEMS_PATH")
    structures_file = must_env("STRUCTURES_FILE")

    regions_min = int(must_env("REGIONS_WORKERS_MIN"))
    structs_min = int(must_env("STRUCTS_WORKERS_MIN"))
    if regions_min <= 0 or structs_min <= 0:
        raise RuntimeError("REGIONS_WORKERS_MIN and STRUCTS_WORKERS_MIN must be > 0")

    regions_target = int(must_env("REGIONS_TARGET_PAGES_PER_WORKER"))
    structs_target = int(must_env("STRUCTS_TARGET_PAGES_PER_WORKER"))

    primary_char_id = int(must_env("PRIMARY_CHAR_ID"))
    retry_budget_initial = int(must_env("RETRY_BUDGET"))
    jita44_location_id = int(must_env("JITA44_LOCATION_ID"))

    ensure_dir(out_dir)
    ensure_dir(os.path.dirname(pages_cache_path) or ".")

    cache = load_pages_cache(pages_cache_path)
    regions_max_bound = max(cache.planner.REGIONS_WORKERS_MAX, regions_min)
    structs_max_bound = max(cache.planner.STRUCTS_WORKERS_MAX, structs_min)

    log("loading indices...")

    regions_truth: List[Tuple[int, str]] = []
    for row in read_gz_jsonl(sde_regions_path):
        rid = safe_int(row.get("regionID", 0), 0)
        rname = str(row.get("region", ""))
        if rid > 0:
            regions_truth.append((rid, rname))

    structures_truth: List[Tuple[int, str]] = []
    for row in read_gz_jsonl(structures_file):
        if not bool(row.get("market", False)):
            continue
        sid = safe_int(row.get("stationID", 0), 0)
        sname = str(row.get("station", ""))
        if sid > 0:
            structures_truth.append((sid, sname))

    type_id_to_name: Dict[int, str] = {}
    for row in read_gz_jsonl(sde_types_path):
        tid = safe_int(row.get("typeID", 0), 0)
        tname = row.get("type")
        if tid > 0 and isinstance(tname, str):
            type_id_to_name[tid] = tname

    excluded_ids: List[int] = []
    for row in read_gz_jsonl(sde_excluded_path):
        tid = safe_int(row.get("typeID", 0), 0)
        if tid > 0:
            excluded_ids.append(tid)
    excluded_ids.sort()

    station_id_to_name: Dict[int, str] = {}
    for row in read_gz_jsonl(sde_stations_path):
        sid = safe_int(row.get("stationID", 0), 0)
        sname = row.get("station")
        if sid > 0 and isinstance(sname, str):
            station_id_to_name[sid] = sname

    solar_system_id_to_meta: Dict[int, Tuple[str, str, str]] = {}
    for row in read_gz_jsonl(sde_solarsystems_path):
        ssid = safe_int(row.get("solarSystemID", 0), 0)
        if ssid <= 0:
            continue
        solar = str(row.get("solarSystem", ""))
        const = str(row.get("constellation", ""))
        reg = str(row.get("region", ""))
        solar_system_id_to_meta[ssid] = (solar, const, reg)

    structure_id_to_name: Dict[int, str] = {sid: name for sid, name in structures_truth}

    auth_tokens = load_tokens_from_secrets(primary_char_id)
    log(f"regions_count={len(regions_truth)}")
    log(f"structures_market_count={len(structures_truth)}")
    log(f"tokens_count={len(auth_tokens)} primary_char_id={primary_char_id}")

    prev_pages_by_region: Dict[int, int] = {e.regionID: e.pages for e in cache.stations}
    prev_pages_by_struct: Dict[int, int] = {e.stationID: e.pages for e in cache.structures}

    total_region_pages = sum(prev_pages_by_region.get(rid, 0) for rid, _ in regions_truth)
    total_struct_pages = sum(prev_pages_by_struct.get(sid, 0) for sid, _ in structures_truth)

    regions_workers = compute_workers_needed(total_region_pages, regions_min, regions_max_bound, regions_target)
    structs_workers = compute_workers_needed(total_struct_pages, structs_min, structs_max_bound, structs_target)

    if regions_workers + structs_workers > GLOBAL_WORKERS_SUM_LIMIT:
        excess = (regions_workers + structs_workers) - GLOBAL_WORKERS_SUM_LIMIT
        structs_workers = max(structs_min, structs_workers - excess)

    log(f"planner_run regions_workers={regions_workers} structs_workers={structs_workers} regions_max_bound={regions_max_bound} structs_max_bound={structs_max_bound}")

    region_items = [(prev_pages_by_region.get(rid, 0), (rid, rname)) for rid, rname in regions_truth]
    struct_items = [(prev_pages_by_struct.get(sid, 0), (sid, sname)) for sid, sname in structures_truth]

    regions_bins = greedy_balance(region_items, regions_workers)
    structs_bins = greedy_balance(struct_items, structs_workers)

    retry_budget = RetryBudget(retry_budget_initial)

    session = requests.Session()

    all_rows_global: List[dict] = []
    jita_buy_global: List[dict] = []
    jita_sell_global: List[dict] = []
    pages_by_region_new: Dict[int, int] = {}
    pages_by_struct_new: Dict[int, int] = {}
    max_expires_epoch = 0
    max_last_modified_epoch = 0

    log("phase=regions start")
    for wi, entities in enumerate(regions_bins, start=1):
        log(f"worker_start kind=REGION worker={wi}/{len(regions_bins)} entities={len(entities)}")
        rows, jb, js, pbr, _, me, ml = worker_run_entities(
            worker_kind="REGION",
            entities=entities,
            session=session,
            retry_budget=retry_budget,
            auth_tokens=None,
            excluded_type_ids_sorted=excluded_ids,
            type_id_to_name=type_id_to_name,
            station_id_to_name=station_id_to_name,
            structure_id_to_name=structure_id_to_name,
            solar_system_id_to_meta=solar_system_id_to_meta,
            jita44_location_id=jita44_location_id,
        )
        all_rows_global.extend(rows)
        jita_buy_global.extend(jb)
        jita_sell_global.extend(js)
        pages_by_region_new.update(pbr)
        max_expires_epoch = max(max_expires_epoch, me)
        max_last_modified_epoch = max(max_last_modified_epoch, ml)
        log(f"worker_done kind=REGION worker={wi}")
    log("phase=regions end")

    log("phase=structures start")
    for wi, entities in enumerate(structs_bins, start=1):
        log(f"worker_start kind=STRUCTURE worker={wi}/{len(structs_bins)} entities={len(entities)}")
        rows, jb, js, _, pbs, me, ml = worker_run_entities(
            worker_kind="STRUCTURE",
            entities=entities,
            session=session,
            retry_budget=retry_budget,
            auth_tokens=auth_tokens,
            excluded_type_ids_sorted=excluded_ids,
            type_id_to_name=type_id_to_name,
            station_id_to_name=station_id_to_name,
            structure_id_to_name=structure_id_to_name,
            solar_system_id_to_meta=solar_system_id_to_meta,
            jita44_location_id=jita44_location_id,
        )
        all_rows_global.extend(rows)
        jita_buy_global.extend(jb)
        jita_sell_global.extend(js)
        pages_by_struct_new.update(pbs)
        max_expires_epoch = max(max_expires_epoch, me)
        max_last_modified_epoch = max(max_last_modified_epoch, ml)
        log(f"worker_done kind=STRUCTURE worker={wi}")
    log("phase=structures end")

    buy_rows = [r for r in all_rows_global if r.get("is_buy_order") is True]
    sell_rows = [r for r in all_rows_global if r.get("is_buy_order") is False]

    def write_ndjson_gz(path: str, rows: List[dict]) -> None:
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    write_ndjson_gz(os.path.join(out_dir, "buy_orders.jsonl.gz"), buy_rows)
    write_ndjson_gz(os.path.join(out_dir, "sell_orders.jsonl.gz"), sell_rows)
    write_ndjson_gz(os.path.join(out_dir, "jita44_buy_orders.jsonl.gz"), jita_buy_global)
    write_ndjson_gz(os.path.join(out_dir, "jita44_sell_orders.jsonl.gz"), jita_sell_global)

    log(f"out buy_rows={len(buy_rows)} sell_rows={len(sell_rows)} jita_buy_rows={len(jita_buy_global)} jita_sell_rows={len(jita_sell_global)}")

    stations_cache_entries: List[StationCacheEntry] = []
    for rid, rname in regions_truth:
        pages = pages_by_region_new.get(rid, 0)
        stations_cache_entries.append(StationCacheEntry(regionID=rid, region=rname, pages=pages))

    structures_cache_entries: List[StructureCacheEntry] = []
    for sid, sname in structures_truth:
        if sid not in pages_by_struct_new:
            continue
        structures_cache_entries.append(StructureCacheEntry(stationID=sid, station=sname, pages=pages_by_struct_new[sid]))

    total_region_pages_new = sum(e.pages for e in stations_cache_entries)
    total_struct_pages_new = sum(e.pages for e in structures_cache_entries)

    recommended_regions_max = compute_workers_needed(total_region_pages_new, regions_min, 9999, regions_target)
    recommended_structs_max = compute_workers_needed(total_struct_pages_new, structs_min, 9999, structs_target)

    new_regions_max = slow_update_max(
        current=cache.planner.REGIONS_WORKERS_MAX,
        recommended=recommended_regions_max,
        min_workers=regions_min,
        other_current=cache.planner.STRUCTS_WORKERS_MAX,
        global_sum_limit=GLOBAL_WORKERS_SUM_LIMIT,
    )
    new_structs_max = slow_update_max(
        current=cache.planner.STRUCTS_WORKERS_MAX,
        recommended=recommended_structs_max,
        min_workers=structs_min,
        other_current=new_regions_max,
        global_sum_limit=GLOBAL_WORKERS_SUM_LIMIT,
    )

    if new_regions_max + new_structs_max > GLOBAL_WORKERS_SUM_LIMIT:
        new_structs_max = max(structs_min * 2, GLOBAL_WORKERS_SUM_LIMIT - new_regions_max)

    new_cache = PagesCache(
        planner=CachePlanner(REGIONS_WORKERS_MAX=new_regions_max, STRUCTS_WORKERS_MAX=new_structs_max),
        stations=stations_cache_entries,
        structures=structures_cache_entries,
    )

    write_pages_cache_pretty_atomic(pages_cache_path, new_cache)
    log(f"cache_written path={pages_cache_path} regions_max={new_regions_max} structs_max={new_structs_max}")

    log(f"max_expires_epoch={max_expires_epoch}")
    log(f"max_last_modified_epoch={max_last_modified_epoch}")

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"FATAL {e}")
        raise
