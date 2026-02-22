#!/usr/bin/env python3
from __future__ import annotations

import gzip
import json
import os
import sys
import time
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# =============================================================================
# Config (env)
# =============================================================================

ESI_BASE_URL = os.environ.get("ESI_BASE_URL", "https://esi.evetech.net").rstrip("/")
ESI_DATASOURCE = os.environ.get("ESI_DATASOURCE", "tranquility")

PRIMARY_CHAR_ID = int(os.environ.get("PRIMARY_CHAR_ID", "2124070822"))
RETRY_BUDGET = int(os.environ.get("RETRY_BUDGET", "3"))

REGIONS_WORKERS_MIN = int(os.environ.get("REGIONS_WORKERS_MIN", "2"))
REGIONS_WORKERS_MAX = int(os.environ.get("REGIONS_WORKERS_MAX", "6"))
REGIONS_TARGET = int(os.environ.get("REGIONS_TARGET_PAGES_PER_WORKER", "220"))

STRUCTS_WORKERS_MIN = int(os.environ.get("STRUCTS_WORKERS_MIN", "1"))
STRUCTS_WORKERS_MAX = int(os.environ.get("STRUCTS_WORKERS_MAX", "3"))
STRUCTS_TARGET = int(os.environ.get("STRUCTS_TARGET_PAGES_PER_WORKER", "60"))

SDE_REGIONS_PATH = os.environ["SDE_REGIONS_PATH"]
SDE_TYPES_PATH = os.environ["SDE_TYPES_PATH"]
SDE_EXCLUDED_TYPES_PATH = os.environ["SDE_EXCLUDED_TYPES_PATH"]
SDE_STATIONS_PATH = os.environ["SDE_STATIONS_PATH"]
SDE_SOLARSYSTEMS_PATH = os.environ["SDE_SOLARSYSTEMS_PATH"]
STRUCTURES_FILE = os.environ["STRUCTURES_FILE"]

JITA44_LOCATION_ID = int(os.environ.get("JITA44_LOCATION_ID", "60003760"))

PAGES_CACHE_PATH = os.environ.get("PAGES_CACHE_PATH", "states/market_orders_pages.json")

OUT_DIR = os.environ.get("OUT_DIR", ".tmp_eou_market_orders")
os.makedirs(OUT_DIR, exist_ok=True)

USER_AGENT = "EOU Market Orders (GitHub Actions)"


# =============================================================================
# Utils
# =============================================================================

def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_http_date(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        dt = parsedate_to_datetime(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_iso_utc(v: str) -> datetime:
    v = (v or "").strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return datetime.fromisoformat(v).astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def hhmmss(td: timedelta) -> str:
    total = int(td.total_seconds())
    if total < 0:
        total = 0
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


def range_to_int(range_val: Any) -> int:
    if range_val is None:
        return 0
    if isinstance(range_val, int):
        return range_val
    s = str(range_val)
    if s in ("station", "solarsystem"):
        return 0
    if s == "region":
        return 1000
    return safe_int(s, default=0)


def read_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


# =============================================================================
# Load indices
# =============================================================================

def load_regions(path: str) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for obj in read_jsonl_gz(path):
        rid = obj.get("regionID")
        name = obj.get("region")
        if rid is None or name is None:
            continue
        out.append((int(rid), str(name)))
    return out


def load_types_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in read_jsonl_gz(path):
        tid = obj.get("typeID")
        name = obj.get("type")
        if tid is None or name is None:
            continue
        out[int(tid)] = str(name)
    return out


def load_excluded_types(path: str) -> set[int]:
    out: set[int] = set()
    for obj in read_jsonl_gz(path):
        tid = obj.get("typeID")
        if tid is None:
            continue
        out.add(int(tid))
    return out


def load_stations_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in read_jsonl_gz(path):
        sid = obj.get("stationID")
        name = obj.get("station")
        if sid is None or name is None:
            continue
        out[int(sid)] = str(name)
    return out


def load_structures_market(path: str) -> Tuple[List[int], Dict[int, str]]:
    ids: List[int] = []
    names: Dict[int, str] = {}
    for obj in read_jsonl_gz(path):
        sid = obj.get("stationID")
        if sid is None:
            continue
        if not bool(obj.get("market", False)):
            continue
        sid_i = int(sid)
        ids.append(sid_i)
        if obj.get("station"):
            names[sid_i] = str(obj.get("station"))
    # stable unique
    seen = set()
    uniq: List[int] = []
    for x in ids:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq, names


def load_solarsystems(path: str) -> Dict[int, Tuple[str, str, str]]:
    out: Dict[int, Tuple[str, str, str]] = {}
    for obj in read_jsonl_gz(path):
        sid = obj.get("solarSystemID")
        if sid is None:
            continue
        out[int(sid)] = (
            str(obj.get("solarSystem") or ""),
            str(obj.get("constellation") or ""),
            str(obj.get("region") or ""),
        )
    return out


def load_pages_cache(path: str) -> Tuple[Dict[int, int], Dict[int, int]]:
    # Returns: (regionID->pages, stationID->pages)
    if not os.path.exists(path):
        return {}, {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        reg = {int(x["regionID"]): int(x.get("pages", 0)) for x in d.get("stations", []) if "regionID" in x}
        st = {int(x["stationID"]): int(x.get("pages", 0)) for x in d.get("structures", []) if "stationID" in x}
        return reg, st
    except Exception:
        return {}, {}


# =============================================================================
# Tokens (EXACT policy)
# =============================================================================

def normalize_token(tok: str) -> str:
    tok = (tok or "").strip()
    if tok.lower().startswith("bearer "):
        parts = tok.split(None, 1)
        tok = parts[1].strip() if len(parts) > 1 else ""
    return tok


class TokenPool:
    def __init__(self, primary_char_id: int, token_map: Dict[int, str]):
        self.primary = int(primary_char_id)
        self.token_map = {int(k): str(v) for k, v in token_map.items() if str(v).strip()}
        ids = list(self.token_map.keys())
        order: List[int] = []
        if self.primary in self.token_map:
            order.append(self.primary)
        order.extend(sorted([i for i in ids if i != self.primary], reverse=True))
        self.order = order
        self.idx = 0
        self.lock = threading.Lock()

    def count(self) -> int:
        return len(self.order)

    def current(self) -> Tuple[Optional[int], str]:
        with self.lock:
            if not self.order:
                return None, ""
            cid = self.order[self.idx]
            return cid, normalize_token(self.token_map.get(cid, ""))

    def rotate(self) -> None:
        with self.lock:
            if not self.order:
                return
            self.idx = (self.idx + 1) % len(self.order)


def load_tokens_from_env() -> Dict[int, str]:
    t1 = os.environ.get("EOU_ACCESS_TOKENS_1", "").strip()
    t2 = os.environ.get("EOU_ACCESS_TOKENS_2", "").strip()

    def parse(s: str) -> Dict[int, str]:
        if not s:
            return {}
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return {}
        out: Dict[int, str] = {}
        for k, v in obj.items():
            try:
                out[int(k)] = str(v)
            except Exception:
                continue
        return out

    d: Dict[int, str] = {}
    d.update(parse(t1))
    d.update(parse(t2))
    return d


# =============================================================================
# ESI client with gentle retries and error-limit awareness
# =============================================================================

@dataclass
class EsiResp:
    status: int
    headers: Dict[str, str]
    data: Any


class EsiClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": USER_AGENT})

    def _request(
        self,
        method: str,
        url: str,
        params: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 90,
    ) -> EsiResp:
        h = dict(headers or {})
        r = self.s.request(method, url, params=params, headers=h, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = None
        return EsiResp(status=r.status_code, headers=dict(r.headers), data=data)


# =============================================================================
# Scheduling helpers (dynamic workers + LPT bin packing)
# =============================================================================

def clamp(lo: int, x: int, hi: int) -> int:
    return max(lo, min(hi, x))


def choose_workers(total_weight: int, target: int, wmin: int, wmax: int) -> int:
    if target <= 0:
        return wmin
    need = (total_weight + target - 1) // target
    return clamp(wmin, need, wmax)


def lpt_shards(items: List[Tuple[int, str, int]], workers: int) -> List[List[Tuple[int, str, int]]]:
    # items: (id, name, weight)
    if workers <= 1:
        return [items]
    bins: List[List[Tuple[int, str, int]]] = [[] for _ in range(workers)]
    load: List[int] = [0 for _ in range(workers)]
    for it in sorted(items, key=lambda x: x[2], reverse=True):
        j = min(range(workers), key=lambda k: load[k])
        bins[j].append(it)
        load[j] += it[2]
    return bins


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    # Load indices
    log("loading indices...")
    regions = load_regions(SDE_REGIONS_PATH)
    types_map = load_types_map(SDE_TYPES_PATH)
    excluded = load_excluded_types(SDE_EXCLUDED_TYPES_PATH)
    stations_map = load_stations_map(SDE_STATIONS_PATH)
    structures_ids, structures_map = load_structures_market(STRUCTURES_FILE)
    systems_map = load_solarsystems(SDE_SOLARSYSTEMS_PATH)

    reg_cache, struct_cache = load_pages_cache(PAGES_CACHE_PATH)

    log(f"regions_count={len(regions)}")
    log(f"structures_market_count={len(structures_ids)}")

    # Tokens
    token_map = load_tokens_from_env()
    pool = TokenPool(PRIMARY_CHAR_ID, token_map)
    log(f"tokens_count={pool.count()} primary_char_id={PRIMARY_CHAR_ID}")
    for cid in pool.order[:10]:
        raw = (pool.token_map.get(cid, "") or "").strip()
        log(f"token_shape char_id={cid} has_bearer_prefix={raw.lower().startswith('bearer ')} len={len(raw)}")

    # Planner weights (pages_est, but for scheduling weight>=1)
    region_items: List[Tuple[int, str, int]] = []
    for rid, rname in regions:
        pages_est = int(reg_cache.get(rid, 0))
        weight = max(1, pages_est)
        region_items.append((rid, rname, weight))

    struct_items: List[Tuple[int, str, int]] = []
    for sid in structures_ids:
        sname = structures_map.get(sid, "")
        pages_est = int(struct_cache.get(sid, 0))
        weight = max(1, pages_est)
        struct_items.append((sid, sname, weight))

    total_reg_weight = sum(w for _, _, w in region_items)
    total_struct_weight = sum(w for _, _, w in struct_items)

    reg_workers = choose_workers(total_reg_weight, REGIONS_TARGET, REGIONS_WORKERS_MIN, REGIONS_WORKERS_MAX)
    struct_workers = choose_workers(total_struct_weight, STRUCTS_TARGET, STRUCTS_WORKERS_MIN, STRUCTS_WORKERS_MAX)

    log(f"planner regions_workers={reg_workers} total_weight={total_reg_weight} target={REGIONS_TARGET}")
    log(f"planner structs_workers={struct_workers} total_weight={total_struct_weight} target={STRUCTS_TARGET}")

    region_shards = lpt_shards(region_items, reg_workers)
    struct_shards = lpt_shards(struct_items, struct_workers)

    # Output files per shard
    def shard_paths(prefix: str, idx: int) -> Dict[str, str]:
        base = os.path.join(OUT_DIR, f"{prefix}_w{idx:02d}")
        return {
            "buy": base + "_buy.jsonl",
            "sell": base + "_sell.jsonl",
            "jita_buy": base + "_jita_buy.jsonl",
            "jita_sell": base + "_jita_sell.jsonl",
        }

    # Shared stats for Sheets finalize
    max_expires_lock = threading.Lock()
    max_expires: Optional[datetime] = None
    max_last_modified: Optional[datetime] = None

    def update_cache_headers(headers: Dict[str, str]) -> None:
        nonlocal max_expires, max_last_modified
        exp = parse_http_date(headers.get("Expires"))
        if exp is None:
            return
        with max_expires_lock:
            if max_expires is None or exp > max_expires:
                max_expires = exp
                max_last_modified = parse_http_date(headers.get("Last-Modified"))

    # Station name resolution
    def station_name(location_id: int) -> str:
        if location_id < 1_000_000_000_000:
            return stations_map.get(location_id, "")
        return structures_map.get(location_id, "")

    def system_info(system_id: Any) -> Tuple[str, str, str]:
        if system_id is None:
            return "", "", ""
        return systems_map.get(int(system_id), ("", "", ""))

    # Global budget for authenticated 401/420 (EXACT policy)
    budget_lock = threading.Lock()
    auth_budget = {"remaining": RETRY_BUDGET}

    def consume_budget_or_fail(reason: str, structure_id: int, page: int, char_id: Optional[int]) -> None:
        with budget_lock:
            if auth_budget["remaining"] <= 0:
                log(f"RETRY_BUDGET_EXHAUSTED reason={reason} structure_id={structure_id} page={page} char_id={char_id}")
                raise RuntimeError("RETRY_BUDGET exhausted for authenticated requests (401/420).")
            auth_budget["remaining"] -= 1
            remaining = auth_budget["remaining"]
        log(f"auth_retry={reason} structure_id={structure_id} page={page} char_id={char_id} remaining_budget={remaining}")

    client = EsiClient()
    now_ref = utc_now()

    # Observed pages to write cache on success
    observed_region_pages_lock = threading.Lock()
    observed_region_pages: Dict[int, int] = {}

    observed_struct_pages_lock = threading.Lock()
    observed_struct_pages: Dict[int, int] = {}

    ignored_structures_lock = threading.Lock()
    ignored_structures: set[int] = set()

    # Writers
    def write_line(fp, obj: Dict[str, Any]) -> None:
        fp.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # Pagination policy: page=1.. until ESI says "page doesn't exist"
    # - For regions: treat non-200 on page>1 as "end" (no retry) except transient 429/502/503/504/520/420.
    # - For structures: use auth policy for 401/420; and special ignore for 403/404 after one 5s retry.

    transient_status = {420, 429, 502, 503, 504, 520}

    def region_fetch_page(region_id: int, page: int) -> EsiResp:
        url = f"{ESI_BASE_URL}/latest/markets/{region_id}/orders/"
        params = {"datasource": ESI_DATASOURCE, "order_type": "all", "page": page}
        return client._request("GET", url, params=params)

    def structure_fetch_page(structure_id: int, page: int) -> EsiResp:
        cid, tok = pool.current()
        if not tok:
            return EsiResp(status=0, headers={}, data=None)
        url = f"{ESI_BASE_URL}/latest/markets/structures/{structure_id}/"
        params = {"datasource": ESI_DATASOURCE, "page": page}
        hdrs = {"Authorization": f"Bearer {tok}"}
        return client._request("GET", url, params=params, headers=hdrs)

    def process_orders_list(
        orders: List[Dict[str, Any]],
        buy_fp,
        sell_fp,
        jita_buy_fp,
        jita_sell_fp,
        seen_buy: set[int],
        seen_sell: set[int],
        seen_jita_buy: set[int],
        seen_jita_sell: set[int],
    ) -> Tuple[int, int, int, int]:
        buy_n = sell_n = jb_n = js_n = 0
        now_local = utc_now()

        for o in orders:
            try:
                type_id = int(o.get("type_id"))
                if type_id in excluded:
                    continue

                order_id = safe_int(o.get("order_id"))
                if order_id <= 0:
                    continue

                is_buy = bool(o.get("is_buy_order"))

                issued_dt = parse_iso_utc(str(o.get("issued")))
                duration_days = int(o.get("duration"))
                until_dt = issued_dt + timedelta(days=duration_days)
                time_left = hhmmss(until_dt - now_local)

                row_common = {
                    "type": types_map.get(type_id),
                    "orderPrice": safe_float(o.get("price")),
                    "volRemain": safe_int(o.get("volume_remain")),
                    "volTotal": safe_int(o.get("volume_total")),
                    "volMin": safe_int(o.get("min_volume")),
                    "issued": iso_utc(issued_dt),
                    "until": iso_utc(until_dt),
                    "timeLeft": time_left,
                    "orderID": order_id,
                }

                loc_id = safe_int(o.get("location_id"))
                sys_id = o.get("system_id")

                if is_buy:
                    if order_id in seen_buy:
                        continue
                    seen_buy.add(order_id)

                    rint = range_to_int(o.get("range"))
                    solar, const, reg = system_info(sys_id)

                    out = dict(row_common)
                    out.update({
                        "station": station_name(loc_id),
                        "solarSystem": solar,
                        "constellation": const,
                        "region": reg,
                        "ordeRange": rint,
                    })
                    write_line(buy_fp, out)
                    buy_n += 1

                    if loc_id == JITA44_LOCATION_ID:
                        if order_id not in seen_jita_buy:
                            seen_jita_buy.add(order_id)
                            j = {
                                "type": row_common["type"],
                                "orderPrice": row_common["orderPrice"],
                                "volRemain": row_common["volRemain"],
                                "volTotal": row_common["volTotal"],
                                "volMin": row_common["volMin"],
                                "issued": row_common["issued"],
                                "until": row_common["until"],
                                "timeLeft": row_common["timeLeft"],
                                "orderID": row_common["orderID"],
                                "ordeRange": rint,
                            }
                            write_line(jita_buy_fp, j)
                            jb_n += 1
                else:
                    if order_id in seen_sell:
                        continue
                    seen_sell.add(order_id)

                    solar, const, reg = system_info(sys_id)

                    out = dict(row_common)
                    out.update({
                        "station": station_name(loc_id),
                        "solarSystem": solar,
                        "constellation": const,
                        "region": reg,
                    })
                    write_line(sell_fp, out)
                    sell_n += 1

                    if loc_id == JITA44_LOCATION_ID:
                        if order_id not in seen_jita_sell:
                            seen_jita_sell.add(order_id)
                            j = {
                                "type": row_common["type"],
                                "orderPrice": row_common["orderPrice"],
                                "volRemain": row_common["volRemain"],
                                "volTotal": row_common["volTotal"],
                                "volMin": row_common["volMin"],
                                "issued": row_common["issued"],
                                "until": row_common["until"],
                                "timeLeft": row_common["timeLeft"],
                                "orderID": row_common["orderID"],
                            }
                            write_line(jita_sell_fp, j)
                            js_n += 1
            except Exception:
                continue

        return buy_n, sell_n, jb_n, js_n

    # Worker functions
    def run_region_shard(shard_idx: int, shard: List[Tuple[int, str, int]]) -> Dict[str, Any]:
        paths = shard_paths("regions", shard_idx)
        seen_buy: set[int] = set()
        seen_sell: set[int] = set()
        seen_jita_buy: set[int] = set()
        seen_jita_sell: set[int] = set()

        buy_n = sell_n = jb_n = js_n = 0
        region_pages_local: Dict[int, int] = {}

        with open(paths["buy"], "w", encoding="utf-8") as f_buy, \
             open(paths["sell"], "w", encoding="utf-8") as f_sell, \
             open(paths["jita_buy"], "w", encoding="utf-8") as f_jb, \
             open(paths["jita_sell"], "w", encoding="utf-8") as f_js:

            for rid, rname, _w in shard:
                log(f"region_start worker={shard_idx} region_id={rid} \"{rname}\"")
                page = 1
                pages_ok = 0

                while True:
                    resp = region_fetch_page(rid, page)
                    st = resp.status

                    # Update global cache headers on any 200
                    if st == 200:
                        update_cache_headers(resp.headers)

                    # Transient retries
                    if st in transient_status:
                        # Gentle backoff
                        time.sleep(5 + random.random() * 2)
                        continue

                    if st != 200:
                        # page=1 failing -> log and stop region
                        if page == 1:
                            log(f"region_page status={st} region_id={rid} page=1 -> skip_region")
                        else:
                            log(f"region_end status={st} region_id={rid} page={page} pages_ok={pages_ok}")
                        break

                    if not isinstance(resp.data, list):
                        log(f"region_end nonlist region_id={rid} page={page} pages_ok={pages_ok}")
                        break

                    pages_ok = page
                    b, s, jb, js = process_orders_list(resp.data, f_buy, f_sell, f_jb, f_js,
                                                      seen_buy, seen_sell, seen_jita_buy, seen_jita_sell)
                    buy_n += b
                    sell_n += s
                    jb_n += jb
                    js_n += js

                    page += 1

                region_pages_local[rid] = pages_ok
                log(f"region_done worker={shard_idx} region_id={rid} pages={pages_ok}")

        with observed_region_pages_lock:
            observed_region_pages.update(region_pages_local)

        return {"buy": buy_n, "sell": sell_n, "jita_buy": jb_n, "jita_sell": js_n}

    def run_struct_shard(shard_idx: int, shard: List[Tuple[int, str, int]]) -> Dict[str, Any]:
        paths = shard_paths("structs", shard_idx)
        seen_buy: set[int] = set()
        seen_sell: set[int] = set()
        seen_jita_buy: set[int] = set()
        seen_jita_sell: set[int] = set()

        buy_n = sell_n = jb_n = js_n = 0
        struct_pages_local: Dict[int, int] = {}
        ignored_local: set[int] = set()

        with open(paths["buy"], "w", encoding="utf-8") as f_buy, \
             open(paths["sell"], "w", encoding="utf-8") as f_sell, \
             open(paths["jita_buy"], "w", encoding="utf-8") as f_jb, \
             open(paths["jita_sell"], "w", encoding="utf-8") as f_js:

            for sid, sname, _w in shard:
                label = f"{sid}" + (f' "{sname}"' if sname else "")
                log(f"structure_start worker={shard_idx} structure_id={label}")

                page = 1
                pages_ok = 0

                # First-page permission/no-exist rule: retry after 5000ms then ignore
                first_page_attempts = 0

                while True:
                    cid, _tok = pool.current()
                    resp = structure_fetch_page(sid, page)
                    st = resp.status

                    if st == 200:
                        update_cache_headers(resp.headers)

                    if st == 401:
                        consume_budget_or_fail("401", sid, page, cid)
                        time.sleep(30)
                        pool.rotate()
                        continue

                    if st == 420:
                        consume_budget_or_fail("420", sid, page, cid)
                        time.sleep(30)
                        continue

                    if st in (403, 404):
                        if page == 1:
                            first_page_attempts += 1
                            if first_page_attempts == 1:
                                log(f"structure_perm_or_missing status={st} structure_id={sid} retry_ms=5000")
                                time.sleep(5)
                                continue
                            log(f"structure_ignored status={st} structure_id={sid}")
                            ignored_local.add(sid)
                            break
                        # if mid pages suddenly 403/404 -> stop structure
                        log(f"structure_end status={st} structure_id={sid} page={page} pages_ok={pages_ok}")
                        break

                    if st in transient_status:
                        time.sleep(5 + random.random() * 2)
                        continue

                    if st != 200:
                        if page == 1:
                            log(f"structure_page status={st} structure_id={sid} page=1 -> skip_structure")
                        else:
                            log(f"structure_end status={st} structure_id={sid} page={page} pages_ok={pages_ok}")
                        break

                    if not isinstance(resp.data, list):
                        log(f"structure_end nonlist structure_id={sid} page={page} pages_ok={pages_ok}")
                        break

                    pages_ok = page
                    b, s, jb, js = process_orders_list(resp.data, f_buy, f_sell, f_jb, f_js,
                                                      seen_buy, seen_sell, seen_jita_buy, seen_jita_sell)
                    buy_n += b
                    sell_n += s
                    jb_n += jb
                    js_n += js

                    page += 1

                if sid not in ignored_local:
                    struct_pages_local[sid] = pages_ok
                    log(f"structure_done worker={shard_idx} structure_id={sid} pages={pages_ok}")

        with observed_struct_pages_lock:
            observed_struct_pages.update(struct_pages_local)

        with ignored_structures_lock:
            ignored_structures.update(ignored_local)

        return {"buy": buy_n, "sell": sell_n, "jita_buy": jb_n, "jita_sell": js_n}

    # Run regions then structures (to limit concurrency pressure on ESI)
    log("phase=regions start")
    reg_totals = {"buy": 0, "sell": 0, "jita_buy": 0, "jita_sell": 0}
    with ThreadPoolExecutor(max_workers=reg_workers) as ex:
        futs = [ex.submit(run_region_shard, i + 1, shard) for i, shard in enumerate(region_shards)]
        for fut in as_completed(futs):
            res = fut.result()
            for k in reg_totals:
                reg_totals[k] += int(res.get(k, 0))
    log("phase=regions end")

    log("phase=structures start")
    struct_totals = {"buy": 0, "sell": 0, "jita_buy": 0, "jita_sell": 0}
    if pool.count() == 0:
        log("no_tokens -> structures skipped")
    else:
        with ThreadPoolExecutor(max_workers=struct_workers) as ex:
            futs = [ex.submit(run_struct_shard, i + 1, shard) for i, shard in enumerate(struct_shards)]
            for fut in as_completed(futs):
                res = fut.result()
                for k in struct_totals:
                    struct_totals[k] += int(res.get(k, 0))
    log("phase=structures end")

    # Merge shard outputs into final outputs (no global dedupe unless you add anomaly mode)
    def merge_files(glob_prefix: str, out_name: str) -> int:
        out_path = os.path.join(OUT_DIR, out_name)
        count = 0
        with open(out_path, "w", encoding="utf-8") as w:
            for fn in sorted(os.listdir(OUT_DIR)):
                if fn.startswith(glob_prefix) and fn.endswith(".jsonl"):
                    with open(os.path.join(OUT_DIR, fn), "r", encoding="utf-8") as r:
                        for line in r:
                            w.write(line)
                            count += 1
        return count

    log("phase=merge start")
    rows_buy = merge_files("regions_w", "_final_buy_orders.jsonl") + merge_files("structs_w", "_final_buy_orders_append.jsonl")
    # Merge two stage outputs into one stable name
    # (We keep it simple: concatenate the appended file into final file, then remove appended)
    final_buy = os.path.join(OUT_DIR, "buy_orders.jsonl")
    tmp_buy_a = os.path.join(OUT_DIR, "_final_buy_orders.jsonl")
    tmp_buy_b = os.path.join(OUT_DIR, "_final_buy_orders_append.jsonl")
    with open(final_buy, "w", encoding="utf-8") as w:
        if os.path.exists(tmp_buy_a):
            w.write(open(tmp_buy_a, "r", encoding="utf-8").read())
        if os.path.exists(tmp_buy_b):
            w.write(open(tmp_buy_b, "r", encoding="utf-8").read())

    tmp_sell_a = os.path.join(OUT_DIR, "_final_sell_orders.jsonl")
    tmp_sell_b = os.path.join(OUT_DIR, "_final_sell_orders_append.jsonl")
    merge_files("regions_w", "_final_sell_orders.jsonl")
    merge_files("structs_w", "_final_sell_orders_append.jsonl")
    final_sell = os.path.join(OUT_DIR, "sell_orders.jsonl")
    with open(final_sell, "w", encoding="utf-8") as w:
        if os.path.exists(tmp_sell_a):
            w.write(open(tmp_sell_a, "r", encoding="utf-8").read())
        if os.path.exists(tmp_sell_b):
            w.write(open(tmp_sell_b, "r", encoding="utf-8").read())

    tmp_jb_a = os.path.join(OUT_DIR, "_final_jita_buy.jsonl")
    tmp_jb_b = os.path.join(OUT_DIR, "_final_jita_buy_append.jsonl")
    merge_files("regions_w", "_final_jita_buy.jsonl")
    merge_files("structs_w", "_final_jita_buy_append.jsonl")
    final_jb = os.path.join(OUT_DIR, "jita44_buy_orders.jsonl")
    with open(final_jb, "w", encoding="utf-8") as w:
        if os.path.exists(tmp_jb_a):
            w.write(open(tmp_jb_a, "r", encoding="utf-8").read())
        if os.path.exists(tmp_jb_b):
            w.write(open(tmp_jb_b, "r", encoding="utf-8").read())

    tmp_js_a = os.path.join(OUT_DIR, "_final_jita_sell.jsonl")
    tmp_js_b = os.path.join(OUT_DIR, "_final_jita_sell_append.jsonl")
    merge_files("regions_w", "_final_jita_sell.jsonl")
    merge_files("structs_w", "_final_jita_sell_append.jsonl")
    final_js = os.path.join(OUT_DIR, "jita44_sell_orders.jsonl")
    with open(final_js, "w", encoding="utf-8") as w:
        if os.path.exists(tmp_js_a):
            w.write(open(tmp_js_a, "r", encoding="utf-8").read())
        if os.path.exists(tmp_js_b):
            w.write(open(tmp_js_b, "r", encoding="utf-8").read())

    log("phase=merge end")

    # Build and write pages cache JSON (ONLY if overall successful)
    # - Regions: all from SDE
    # - Structures: only those not ignored
    stations_out = []
    with observed_region_pages_lock:
        obs_reg = dict(observed_region_pages)
    for rid, rname in regions:
        stations_out.append({"regionID": rid, "region": rname, "pages": int(obs_reg.get(rid, 0))})

    with observed_struct_pages_lock:
        obs_struct = dict(observed_struct_pages)
    with ignored_structures_lock:
        ignored = set(ignored_structures)

    structures_out = []
    for sid in structures_ids:
        if sid in ignored:
            continue
        # If it was present but got 0 pages, keep it (means fetched and ended quickly).
        structures_out.append({"stationID": sid, "station": structures_map.get(sid, ""), "pages": int(obs_struct.get(sid, 0))})

    cache_obj = {"stations": stations_out, "structures": structures_out}

    os.makedirs(os.path.dirname(PAGES_CACHE_PATH) or ".", exist_ok=True)
    tmp_cache = PAGES_CACHE_PATH + ".tmp"
    with open(tmp_cache, "w", encoding="utf-8") as f:
        json.dump(cache_obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp_cache, PAGES_CACHE_PATH)

    # Print summary
    log(
        "summary "
        f"regions_buy={reg_totals['buy']} regions_sell={reg_totals['sell']} "
        f"regions_jita_buy={reg_totals['jita_buy']} regions_jita_sell={reg_totals['jita_sell']} "
        f"structs_buy={struct_totals['buy']} structs_sell={struct_totals['sell']} "
        f"structs_jita_buy={struct_totals['jita_buy']} structs_jita_sell={struct_totals['jita_sell']} "
        f"remaining_auth_budget={auth_budget['remaining']}"
    )
    log(f"outputs_dir={OUT_DIR}")
    log(f"pages_cache_written={PAGES_CACHE_PATH} ignored_structures_count={len(ignored)}")

    # Outputs for Sheets finalize
    if max_expires is None:
        max_expires = utc_now()
        max_last_modified = None

    max_expires_epoch = int(max_expires.timestamp())
    max_last_modified_epoch = int(max_last_modified.timestamp()) if max_last_modified else 0

    print(f"max_expires_epoch={max_expires_epoch}")
    print(f"max_last_modified_epoch={max_last_modified_epoch}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
