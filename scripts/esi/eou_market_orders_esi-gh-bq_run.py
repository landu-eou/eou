#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# =============================================================================
# Logging
# =============================================================================

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# =============================================================================
# Time / Sheets serial
# =============================================================================

GS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)  # Google Sheets serial epoch


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_gs_serial(dt: datetime) -> float:
    dt = dt.astimezone(timezone.utc)
    delta = dt - GS_EPOCH
    return delta.days + (delta.seconds + delta.microseconds / 1_000_000) / 86400.0


def parse_http_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_iso_utc(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return datetime.fromisoformat(v).astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


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


def hh_mm_ss(td: timedelta) -> str:
    total = int(td.total_seconds())
    if total < 0:
        total = 0
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


# =============================================================================
# ESI client (light)
# =============================================================================

@dataclass
class EsiResp:
    status: int
    data: Any
    headers: Dict[str, str]


class EsiClient:
    def __init__(self, base_url: str, datasource: str):
        self.base_url = base_url.rstrip("/")
        self.datasource = datasource
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "EOU Market Orders (GitHub Actions)"})

    def _req(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 90,
    ) -> EsiResp:
        url = f"{self.base_url}{path}"
        p = dict(params or {})
        p.setdefault("datasource", self.datasource)

        r = self.s.get(url, params=p, headers=headers, timeout=timeout)
        try:
            j = r.json()
        except Exception:
            j = None
        return EsiResp(status=r.status_code, data=j, headers=dict(r.headers))

    def region_orders_page(self, region_id: int, page: int) -> EsiResp:
        return self._req(
            f"/latest/markets/{region_id}/orders/",
            params={"order_type": "all", "page": page},
        )

    def structure_orders_page(self, structure_id: int, page: int, bearer: str) -> EsiResp:
        return self._req(
            f"/latest/markets/structures/{structure_id}/",
            params={"page": page},
            headers={"Authorization": f"Bearer {bearer}"},
        )


# =============================================================================
# Token pool (EXACT policy requested)
# =============================================================================

def normalize_token(tok: str) -> str:
    tok = (tok or "").strip()
    if tok.lower().startswith("bearer "):
        parts = tok.split(None, 1)
        tok = parts[1].strip() if len(parts) > 1 else ""
    return tok


class TokenPool:
    """
    - Parse two JSON env secrets and merge
    - Selection order:
      1) primary char_id if exists
      2) rest by char_id desc
    - Rotation on 401
    """

    def __init__(self, primary_char_id: int, token_map: Dict[int, str]):
        self.primary_char_id = int(primary_char_id)
        self.token_map = {int(k): str(v) for k, v in token_map.items() if str(v).strip()}
        self.order: List[int] = self._build_order()
        self.idx = 0
        self.lock = threading.Lock()

    def _build_order(self) -> List[int]:
        ids = list(self.token_map.keys())
        out: List[int] = []
        if self.primary_char_id in self.token_map:
            out.append(self.primary_char_id)
        rest = sorted([i for i in ids if i != self.primary_char_id], reverse=True)
        out.extend(rest)
        return out

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

    t1 = os.environ.get("EOU_ACCESS_TOKENS_1", "").strip()
    t2 = os.environ.get("EOU_ACCESS_TOKENS_2", "").strip()

    d: Dict[int, str] = {}
    d.update(parse(t1))
    d.update(parse(t2))
    return d


# =============================================================================
# SDE/ESI indexes
# =============================================================================

def read_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_types_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for o in read_jsonl_gz(path):
        tid = o.get("typeID")
        name = o.get("type")
        if tid is None or name is None:
            continue
        out[int(tid)] = str(name)
    return out


def load_excluded_set(path: str) -> set[int]:
    out: set[int] = set()
    for o in read_jsonl_gz(path):
        tid = o.get("typeID")
        if tid is None:
            continue
        out.add(int(tid))
    return out


def load_stations_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for o in read_jsonl_gz(path):
        sid = o.get("stationID")
        name = o.get("station")
        if sid is None or name is None:
            continue
        out[int(sid)] = str(name)
    return out


def load_structures_market(path: str) -> Tuple[List[int], Dict[int, str]]:
    ids: List[int] = []
    names: Dict[int, str] = {}
    for o in read_jsonl_gz(path):
        sid = o.get("stationID")
        if sid is None:
            continue
        if not bool(o.get("market", False)):
            continue
        sid_i = int(sid)
        ids.append(sid_i)
        if o.get("station") is not None:
            names[sid_i] = str(o.get("station"))
    # unique preserving order
    seen = set()
    uniq: List[int] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        uniq.append(i)
    return uniq, names


def load_solarsystems_map(path: str) -> Dict[int, Tuple[str, str, str]]:
    out: Dict[int, Tuple[str, str, str]] = {}
    for o in read_jsonl_gz(path):
        sid = o.get("solarSystemID")
        if sid is None:
            continue
        out[int(sid)] = (
            str(o.get("solarSystem") or ""),
            str(o.get("constellation") or ""),
            str(o.get("region") or ""),
        )
    return out


def load_regions_list(path: str) -> List[Tuple[int, str]]:
    """
    Esperado: data/sde/regions.jsonl.gz con claves regionID y region.
    """
    out: List[Tuple[int, str]] = []
    for o in read_jsonl_gz(path):
        rid = o.get("regionID")
        name = o.get("region")
        if rid is None:
            continue
        out.append((int(rid), str(name or "")))
    # orden estable por ID asc (útil)
    out.sort(key=lambda x: x[0])
    return out


# =============================================================================
# Pages cache (repo)
# =============================================================================

def load_pages_cache(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"stations": [], "structures": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"stations": [], "structures": []}


def write_pages_cache_atomic(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)


# =============================================================================
# Sheets update (v4 API, bearer token)
# =============================================================================

def sheets_values_update(spreadsheet_id: str, range_a1: str, values: List[List[Any]]) -> None:
    tok = os.environ.get("GOOGLE_SHEETS_ACCESS_TOKEN", "").strip()
    if not tok:
        raise RuntimeError("Missing GOOGLE_SHEETS_ACCESS_TOKEN")

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{range_a1}?valueInputOption=USER_ENTERED"
    body = {"range": range_a1, "majorDimension": "ROWS", "values": values}

    r = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
        data=json.dumps(body),
        timeout=60,
    )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"Sheets update failed {r.status_code}: {r.text[:200]}")


def sheets_set_workflow_cells(status: str, next_run_dt: datetime, last_modified_dt: Optional[datetime]) -> None:
    """
    Tab 'workflows' del SHEETS_ID:
      - columna B: status
      - columna D: next_run (serial)
      - columna I: last_modified (serial) (solo cuando status != failed)
    """
    sid = os.environ["SHEETS_ID"]
    tab = os.environ["SHEET_TAB"]
    row = int(os.environ["SHEETS_WORKFLOW_ROW"])

    status_cell = f"{tab}!B{row}"
    next_run_cell = f"{tab}!D{row}"
    last_mod_cell = f"{tab}!I{row}"

    sheets_values_update(sid, status_cell, [[status]])
    sheets_values_update(sid, next_run_cell, [[dt_to_gs_serial(next_run_dt)]])

    if last_modified_dt is not None:
        sheets_values_update(sid, last_mod_cell, [[dt_to_gs_serial(last_modified_dt)]])


# =============================================================================
# Scheduling (bin packing LPT)
# =============================================================================

@dataclass
class Item:
    kind: str  # "region" or "structure"
    id: int
    name: str
    pages_est: int
    weight: int


def lpt_partition(items: List[Item], workers: int) -> List[List[Item]]:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    buckets: List[List[Item]] = [[] for _ in range(workers)]
    load = [0 for _ in range(workers)]

    # LPT: sort by weight desc
    items_sorted = sorted(items, key=lambda x: x.weight, reverse=True)

    for it in items_sorted:
        # pick worker with min load
        w = min(range(workers), key=lambda i: load[i])
        buckets[w].append(it)
        load[w] += it.weight

    return buckets


def calc_workers(total_weight: int, wmin: int, wmax: int, target: int) -> int:
    if total_weight <= 0:
        return wmin
    target = max(1, target)
    w = (total_weight + target - 1) // target
    return clamp(w, wmin, wmax)


# =============================================================================
# Processing / output
# =============================================================================

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


def station_name(location_id: int, stations_map: Dict[int, str], structures_map: Dict[int, str]) -> str:
    if location_id < 1_000_000_000_000:
        return stations_map.get(location_id, "")
    return structures_map.get(location_id, "")


# =============================================================================
# Global retry budget (auth) shared across structure threads
# =============================================================================

class AuthBudget:
    def __init__(self, initial: int):
        self.remaining = initial
        self.lock = threading.Lock()

    def consume_or_fail(self, reason: str, structure_id: int, page: int, char_id: Optional[int]) -> None:
        with self.lock:
            if self.remaining <= 0:
                log(
                    f"RETRY_BUDGET_EXHAUSTED reason={reason} structure_id={structure_id} page={page} char_id={char_id}"
                )
                raise RuntimeError("RETRY_BUDGET exhausted for authenticated requests (401/420).")
            self.remaining -= 1
            rem = self.remaining
        log(
            f"auth_retry={reason} structure_id={structure_id} page={page} char_id={char_id} remaining_budget={rem}"
        )


# =============================================================================
# Main runner
# =============================================================================

@dataclass
class WorkerResult:
    kind: str
    worker_idx: int
    pages_observed: Dict[int, int]  # id -> pages
    skipped_structures: set[int]    # only for structures workers
    max_expires: Optional[datetime]
    max_last_modified: Optional[datetime]
    out_files: Dict[str, Path]      # keys: buy/sell/jita_buy/jita_sell


def update_max_headers(
    cur_expires: Optional[datetime],
    cur_last_modified: Optional[datetime],
    headers: Dict[str, str],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    exp = parse_http_date(headers.get("Expires"))
    lm = parse_http_date(headers.get("Last-Modified"))
    if exp is None:
        return cur_expires, cur_last_modified
    if cur_expires is None or exp > cur_expires:
        return exp, lm
    return cur_expires, cur_last_modified


def is_end_of_pages(resp: EsiResp) -> bool:
    """
    Fin de paginación: ESI indica que page no existe.
    Dependiendo del endpoint puede ser 404/400 o 200 vacío.
    """
    if resp.status == 200 and isinstance(resp.data, list) and len(resp.data) == 0:
        return True
    if resp.status in (400, 404):
        return True
    return False


def run_regions_worker(
    worker_idx: int,
    items: List[Item],
    client: EsiClient,
    now_ref: datetime,
    types_map: Dict[int, str],
    excluded: set[int],
    stations_map: Dict[int, str],
    structures_map: Dict[int, str],
    systems_map: Dict[int, Tuple[str, str, str]],
    jita44_location_id: int,
    out_dir: Path,
) -> WorkerResult:
    pages_obs: Dict[int, int] = {}
    max_expires: Optional[datetime] = None
    max_last_modified: Optional[datetime] = None

    # local output files for this worker
    wdir = out_dir / "workers" / f"regions_w{worker_idx:02d}"
    wdir.mkdir(parents=True, exist_ok=True)
    f_buy = wdir / "buy_orders.jsonl"
    f_sell = wdir / "sell_orders.jsonl"
    f_jb = wdir / "jita44_buy_orders.jsonl"
    f_js = wdir / "jita44_sell_orders.jsonl"

    seen_order_ids: set[int] = set()

    def write_line(fp, obj: Dict[str, Any]) -> None:
        fp.write(json.dumps(obj, ensure_ascii=False) + "\n")

    log(f"regions_worker_start idx={worker_idx} items={len(items)}")

    with f_buy.open("w", encoding="utf-8") as o_buy, \
         f_sell.open("w", encoding="utf-8") as o_sell, \
         f_jb.open("w", encoding="utf-8") as o_jb, \
         f_js.open("w", encoding="utf-8") as o_js:

        for it in items:
            rid = it.id
            log(f"region_start worker={worker_idx} region_id={rid} name={it.name!r}")

            page = 1
            pages_with_data = 0

            while True:
                resp = client.region_orders_page(rid, page=page)

                max_expires, max_last_modified = update_max_headers(max_expires, max_last_modified, resp.headers)

                if is_end_of_pages(resp):
                    # page inexistente o 200 vacío = fin
                    break

                if resp.status != 200 or not isinstance(resp.data, list):
                    # error no terminal: para reducir daño, paramos esta región
                    log(f"region_stop worker={worker_idx} region_id={rid} page={page} status={resp.status}")
                    break

                # data presente
                if len(resp.data) > 0:
                    pages_with_data = page

                for o in resp.data:
                    try:
                        oid = safe_int(o.get("order_id"))
                        if oid <= 0:
                            continue
                        if oid in seen_order_ids:
                            continue
                        seen_order_ids.add(oid)

                        type_id = safe_int(o.get("type_id"))
                        if type_id in excluded:
                            continue

                        is_buy = bool(o.get("is_buy_order"))
                        issued_dt = parse_iso_utc(str(o.get("issued")))
                        duration_days = safe_int(o.get("duration"))
                        until_dt = issued_dt + timedelta(days=duration_days)
                        time_left = hh_mm_ss(until_dt - now_ref)

                        common = {
                            "type": types_map.get(type_id),
                            "orderPrice": safe_float(o.get("price")),
                            "volRemain": safe_int(o.get("volume_remain")),
                            "volTotal": safe_int(o.get("volume_total")),
                            "volMin": safe_int(o.get("min_volume")),
                            "issued": iso_utc(issued_dt),
                            "until": iso_utc(until_dt),
                            "timeLeft": time_left,
                            "orderID": oid,
                        }

                        loc_id = safe_int(o.get("location_id"))
                        sys_id = o.get("system_id")
                        solar, const, reg = systems_map.get(safe_int(sys_id), ("", "", "")) if sys_id is not None else ("", "", "")

                        if is_buy:
                            rint = range_to_int(o.get("range"))
                            row = dict(common)
                            row.update({
                                "station": station_name(loc_id, stations_map, structures_map),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                                "ordeRange": rint,
                            })
                            write_line(o_buy, row)

                            if loc_id == jita44_location_id:
                                j = {
                                    "type": common["type"],
                                    "orderPrice": common["orderPrice"],
                                    "volRemain": common["volRemain"],
                                    "volTotal": common["volTotal"],
                                    "volMin": common["volMin"],
                                    "issued": common["issued"],
                                    "until": common["until"],
                                    "timeLeft": common["timeLeft"],
                                    "orderID": common["orderID"],
                                    "ordeRange": rint,
                                }
                                write_line(o_jb, j)
                        else:
                            row = dict(common)
                            row.update({
                                "station": station_name(loc_id, stations_map, structures_map),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                            })
                            write_line(o_sell, row)

                            if loc_id == jita44_location_id:
                                j = {
                                    "type": common["type"],
                                    "orderPrice": common["orderPrice"],
                                    "volRemain": common["volRemain"],
                                    "volTotal": common["volTotal"],
                                    "volMin": common["volMin"],
                                    "issued": common["issued"],
                                    "until": common["until"],
                                    "timeLeft": common["timeLeft"],
                                    "orderID": common["orderID"],
                                }
                                write_line(o_js, j)

                    except Exception:
                        # no reventar el worker por un order malformado
                        continue

                page += 1
                # micro jitter para distribuir en el tiempo (reduce colisiones 420/429)
                if page % 5 == 0:
                    time.sleep(0.05 + random.random() * 0.05)

            pages_obs[rid] = pages_with_data
            log(f"region_done worker={worker_idx} region_id={rid} pages={pages_with_data}")

    log(f"regions_worker_end idx={worker_idx}")

    return WorkerResult(
        kind="region",
        worker_idx=worker_idx,
        pages_observed=pages_obs,
        skipped_structures=set(),
        max_expires=max_expires,
        max_last_modified=max_last_modified,
        out_files={
            "buy": f_buy,
            "sell": f_sell,
            "jita_buy": f_jb,
            "jita_sell": f_js,
        },
    )


def run_structs_worker(
    worker_idx: int,
    items: List[Item],
    client: EsiClient,
    now_ref: datetime,
    types_map: Dict[int, str],
    excluded: set[int],
    stations_map: Dict[int, str],
    structures_map: Dict[int, str],
    systems_map: Dict[int, Tuple[str, str, str]],
    jita44_location_id: int,
    out_dir: Path,
    pool: TokenPool,
    budget: AuthBudget,
) -> WorkerResult:
    pages_obs: Dict[int, int] = {}
    skipped: set[int] = set()
    max_expires: Optional[datetime] = None
    max_last_modified: Optional[datetime] = None

    wdir = out_dir / "workers" / f"structs_w{worker_idx:02d}"
    wdir.mkdir(parents=True, exist_ok=True)
    f_buy = wdir / "buy_orders.jsonl"
    f_sell = wdir / "sell_orders.jsonl"
    f_jb = wdir / "jita44_buy_orders.jsonl"
    f_js = wdir / "jita44_sell_orders.jsonl"

    seen_order_ids: set[int] = set()

    def write_line(fp, obj: Dict[str, Any]) -> None:
        fp.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def fetch_page(structure_id: int, page: int) -> EsiResp:
        """
        Política exacta:
        - 401: sleep 30s + rotate token + budget--
        - 420: sleep 30s (sin rotar necesariamente) + budget--
        """
        while True:
            cid, tok = pool.current()
            if not tok:
                return EsiResp(status=401, data=None, headers={})

            resp = client.structure_orders_page(structure_id, page=page, bearer=tok)

            if resp.status == 401:
                budget.consume_or_fail("401", structure_id, page, cid)
                time.sleep(30)
                pool.rotate()
                continue

            if resp.status == 420:
                budget.consume_or_fail("420", structure_id, page, cid)
                time.sleep(30)
                continue

            return resp

    log(f"structs_worker_start idx={worker_idx} items={len(items)}")

    with f_buy.open("w", encoding="utf-8") as o_buy, \
         f_sell.open("w", encoding="utf-8") as o_sell, \
         f_jb.open("w", encoding="utf-8") as o_jb, \
         f_js.open("w", encoding="utf-8") as o_js:

        for it in items:
            sid = it.id
            log(f"structure_start worker={worker_idx} structure_id={sid} name={it.name!r}")

            page = 1
            pages_with_data = 0

            # page=1 especial: permisos/no existe
            resp1 = fetch_page(sid, page=1)
            max_expires, max_last_modified = update_max_headers(max_expires, max_last_modified, resp1.headers)

            if resp1.status in (403, 404):
                # retry 5000ms y si vuelve a fallar => ignorar estructura y NO cachearla
                time.sleep(5.0)
                resp1b = fetch_page(sid, page=1)
                max_expires, max_last_modified = update_max_headers(max_expires, max_last_modified, resp1b.headers)

                if resp1b.status in (403, 404):
                    log(f"structure_ignore worker={worker_idx} structure_id={sid} status={resp1b.status}")
                    skipped.add(sid)
                    continue
                resp = resp1b
            else:
                resp = resp1

            # page loop
            while True:
                if page == 1:
                    cur = resp
                else:
                    cur = fetch_page(sid, page=page)

                max_expires, max_last_modified = update_max_headers(max_expires, max_last_modified, cur.headers)

                if is_end_of_pages(cur):
                    break

                if cur.status != 200 or not isinstance(cur.data, list):
                    log(f"structure_stop worker={worker_idx} structure_id={sid} page={page} status={cur.status}")
                    break

                if len(cur.data) > 0:
                    pages_with_data = page

                for o in cur.data:
                    try:
                        oid = safe_int(o.get("order_id"))
                        if oid <= 0:
                            continue
                        if oid in seen_order_ids:
                            continue
                        seen_order_ids.add(oid)

                        type_id = safe_int(o.get("type_id"))
                        if type_id in excluded:
                            continue

                        is_buy = bool(o.get("is_buy_order"))
                        issued_dt = parse_iso_utc(str(o.get("issued")))
                        duration_days = safe_int(o.get("duration"))
                        until_dt = issued_dt + timedelta(days=duration_days)
                        time_left = hh_mm_ss(until_dt - now_ref)

                        common = {
                            "type": types_map.get(type_id),
                            "orderPrice": safe_float(o.get("price")),
                            "volRemain": safe_int(o.get("volume_remain")),
                            "volTotal": safe_int(o.get("volume_total")),
                            "volMin": safe_int(o.get("min_volume")),
                            "issued": iso_utc(issued_dt),
                            "until": iso_utc(until_dt),
                            "timeLeft": time_left,
                            "orderID": oid,
                        }

                        loc_id = safe_int(o.get("location_id"))
                        sys_id = o.get("system_id")
                        solar, const, reg = systems_map.get(safe_int(sys_id), ("", "", "")) if sys_id is not None else ("", "", "")

                        if is_buy:
                            rint = range_to_int(o.get("range"))
                            row = dict(common)
                            row.update({
                                "station": station_name(loc_id, stations_map, structures_map),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                                "ordeRange": rint,
                            })
                            write_line(o_buy, row)

                            if loc_id == jita44_location_id:
                                j = {
                                    "type": common["type"],
                                    "orderPrice": common["orderPrice"],
                                    "volRemain": common["volRemain"],
                                    "volTotal": common["volTotal"],
                                    "volMin": common["volMin"],
                                    "issued": common["issued"],
                                    "until": common["until"],
                                    "timeLeft": common["timeLeft"],
                                    "orderID": common["orderID"],
                                    "ordeRange": rint,
                                }
                                write_line(o_jb, j)
                        else:
                            row = dict(common)
                            row.update({
                                "station": station_name(loc_id, stations_map, structures_map),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                            })
                            write_line(o_sell, row)

                            if loc_id == jita44_location_id:
                                j = {
                                    "type": common["type"],
                                    "orderPrice": common["orderPrice"],
                                    "volRemain": common["volRemain"],
                                    "volTotal": common["volTotal"],
                                    "volMin": common["volMin"],
                                    "issued": common["issued"],
                                    "until": common["until"],
                                    "timeLeft": common["timeLeft"],
                                    "orderID": common["orderID"],
                                }
                                write_line(o_js, j)

                    except Exception:
                        continue

                page += 1
                if page % 5 == 0:
                    time.sleep(0.05 + random.random() * 0.05)

            pages_obs[sid] = pages_with_data
            log(f"structure_done worker={worker_idx} structure_id={sid} pages={pages_with_data}")

    log(f"structs_worker_end idx={worker_idx}")

    return WorkerResult(
        kind="structure",
        worker_idx=worker_idx,
        pages_observed=pages_obs,
        skipped_structures=skipped,
        max_expires=max_expires,
        max_last_modified=max_last_modified,
        out_files={
            "buy": f_buy,
            "sell": f_sell,
            "jita_buy": f_jb,
            "jita_sell": f_js,
        },
    )


def merge_files_jsonl(inputs: List[Path], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out_path.open("w", encoding="utf-8") as w:
        for p in inputs:
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as r:
                for line in r:
                    w.write(line)
                    n += 1
    return n


def gzip_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as r, gzip.open(dst, "wb") as w:
        shutil.copyfileobj(r, w)


def main_run() -> int:
    # Env
    base_url = os.environ.get("ESI_BASE_URL", "https://esi.evetech.net")
    datasource = os.environ.get("ESI_DATASOURCE", "tranquility")

    sde_types = os.environ["SDE_TYPES_PATH"]
    sde_excluded = os.environ["SDE_EXCLUDED_TYPES_PATH"]
    sde_stations = os.environ["SDE_STATIONS_PATH"]
    sde_systems = os.environ["SDE_SOLARSYSTEMS_PATH"]
    sde_regions = os.environ["SDE_REGIONS_PATH"]
    structures_file = os.environ["STRUCTURES_FILE"]

    pages_cache_path = os.environ.get("PAGES_CACHE_PATH", "states/market_orders_pages.json")

    out_dir = Path(os.environ.get("OUT_DIR", ".tmp_eou_market_orders"))
    out_dir.mkdir(parents=True, exist_ok=True)

    jita44_location_id = int(os.environ.get("JITA44_LOCATION_ID", "60003760"))

    # Workers params
    rmin = int(os.environ.get("REGIONS_WORKERS_MIN", "2"))
    rmax = int(os.environ.get("REGIONS_WORKERS_MAX", "6"))
    smin = int(os.environ.get("STRUCTS_WORKERS_MIN", "1"))
    smax = int(os.environ.get("STRUCTS_WORKERS_MAX", "3"))
    tr = int(os.environ.get("TARGET_PAGES_PER_WORKER_REGIONS", "250"))
    ts = int(os.environ.get("TARGET_PAGES_PER_WORKER_STRUCTS", "120"))

    # Tokens
    primary_char_id = int(os.environ.get("PRIMARY_CHAR_ID", "2124070822"))
    retry_budget = int(os.environ.get("RETRY_BUDGET", "3"))

    log("loading indices...")
    types_map = load_types_map(sde_types)
    excluded = load_excluded_set(sde_excluded)
    stations_map = load_stations_map(sde_stations)
    systems_map = load_solarsystems_map(sde_systems)
    regions = load_regions_list(sde_regions)  # [(regionID, regionName)]
    structure_ids, structures_map = load_structures_market(structures_file)

    cache = load_pages_cache(pages_cache_path)
    cache_regions = {int(x.get("regionID")): int(x.get("pages", 0)) for x in cache.get("stations", []) if x.get("regionID") is not None}
    cache_structs = {int(x.get("stationID")): int(x.get("pages", 0)) for x in cache.get("structures", []) if x.get("stationID") is not None}

    log(f"regions_count={len(regions)}")
    log(f"structures_market_count={len(structure_ids)}")

    token_map = load_tokens_from_env()
    pool = TokenPool(primary_char_id=primary_char_id, token_map=token_map)
    log(f"tokens_count={pool.count()} primary_char_id={primary_char_id}")
    for cid in pool.order[: min(20, len(pool.order))]:
        raw = (pool.token_map.get(cid, "") or "").strip()
        log(f"token_shape char_id={cid} has_bearer_prefix={raw.lower().startswith('bearer ')} len={len(raw)}")

    # Build items with pages_est from cache, else 0; weight=max(1,pages_est)
    region_items: List[Item] = []
    for rid, rname in regions:
        pe = int(cache_regions.get(rid, 0))
        w = max(1, pe)
        region_items.append(Item(kind="region", id=rid, name=rname, pages_est=pe, weight=w))

    struct_items: List[Item] = []
    for sid in structure_ids:
        pe = int(cache_structs.get(sid, 0))
        w = max(1, pe)
        struct_items.append(Item(kind="structure", id=sid, name=structures_map.get(sid, ""), pages_est=pe, weight=w))

    total_r_weight = sum(i.weight for i in region_items)
    total_s_weight = sum(i.weight for i in struct_items)

    r_workers = calc_workers(total_r_weight, rmin, rmax, tr)
    s_workers = calc_workers(total_s_weight, smin, smax, ts)

    log(f"plan regions_workers={r_workers} total_weight={total_r_weight} target={tr}")
    log(f"plan struct_workers={s_workers} total_weight={total_s_weight} target={ts}")

    regions_shards = lpt_partition(region_items, r_workers)
    struct_shards = lpt_partition(struct_items, s_workers)

    # Shared client per thread? safer: per worker create its own session/client
    now_ref = utc_now()

    # Shared auth budget across structure workers
    budget = AuthBudget(retry_budget)

    # Run workers
    results: List[WorkerResult] = []
    max_expires: Optional[datetime] = None
    max_last_modified: Optional[datetime] = None

    log("phase=workers start")

    futures = []
    with ThreadPoolExecutor(max_workers=r_workers + s_workers) as ex:
        # regions
        for wi, shard in enumerate(regions_shards, start=1):
            c = EsiClient(base_url=base_url, datasource=datasource)
            futures.append(ex.submit(
                run_regions_worker,
                wi, shard, c, now_ref,
                types_map, excluded, stations_map, structures_map, systems_map,
                jita44_location_id, out_dir
            ))
        # structures
        for wi, shard in enumerate(struct_shards, start=1):
            c = EsiClient(base_url=base_url, datasource=datasource)
            futures.append(ex.submit(
                run_structs_worker,
                wi, shard, c, now_ref,
                types_map, excluded, stations_map, structures_map, systems_map,
                jita44_location_id, out_dir, pool, budget
            ))

        for fut in as_completed(futures):
            res = fut.result()  # propagates exceptions (incl retry budget exhausted)
            results.append(res)
            max_expires, max_last_modified = update_max_headers(max_expires, max_last_modified, {
                "Expires": iso_utc(res.max_expires) if res.max_expires else "",
                "Last-Modified": iso_utc(res.max_last_modified) if res.max_last_modified else "",
            })

    log("phase=workers end")

    # Aggregate pages observed
    regions_pages_observed: Dict[int, int] = {}
    structs_pages_observed: Dict[int, int] = {}
    skipped_structures_all: set[int] = set()

    for r in results:
        if r.kind == "region":
            regions_pages_observed.update(r.pages_observed)
        else:
            structs_pages_observed.update(r.pages_observed)
            skipped_structures_all |= r.skipped_structures

        # combine max headers more accurately
        if r.max_expires:
            if max_expires is None or r.max_expires > max_expires:
                max_expires = r.max_expires
                max_last_modified = r.max_last_modified

    # Merge outputs
    log("phase=merge start")
    final_dir = out_dir / "final_outputs"
    final_dir.mkdir(parents=True, exist_ok=True)

    def collect_files(key: str) -> List[Path]:
        xs: List[Path] = []
        for r in results:
            p = r.out_files.get(key)
            if p:
                xs.append(p)
        return xs

    merged_buy = final_dir / "buy_orders.jsonl"
    merged_sell = final_dir / "sell_orders.jsonl"
    merged_jb = final_dir / "jita44_buy_orders.jsonl"
    merged_js = final_dir / "jita44_sell_orders.jsonl"

    n_buy = merge_files_jsonl(collect_files("buy"), merged_buy)
    n_sell = merge_files_jsonl(collect_files("sell"), merged_sell)
    n_jb = merge_files_jsonl(collect_files("jita_buy"), merged_jb)
    n_js = merge_files_jsonl(collect_files("jita_sell"), merged_js)

    # gzip final (más práctico para artifact)
    gzip_file(merged_buy, final_dir / "buy_orders.jsonl.gz")
    gzip_file(merged_sell, final_dir / "sell_orders.jsonl.gz")
    gzip_file(merged_jb, final_dir / "jita44_buy_orders.jsonl.gz")
    gzip_file(merged_js, final_dir / "jita44_sell_orders.jsonl.gz")

    log(f"merge_counts buy={n_buy} sell={n_sell} jita_buy={n_jb} jita_sell={n_js}")
    log("phase=merge end")

    # Build pages cache output (ONLY if completed)
    stations_out = []
    for rid, rname in regions:
        stations_out.append({
            "regionID": rid,
            "region": rname,
            "pages": int(regions_pages_observed.get(rid, 0)),
        })

    structures_out = []
    for sid in structure_ids:
        if sid in skipped_structures_all:
            continue  # regla: no guardar en cache
        structures_out.append({
            "stationID": sid,
            "station": structures_map.get(sid, ""),
            "pages": int(structs_pages_observed.get(sid, 0)),
        })

    pages_cache_obj = {
        "stations": stations_out,
        "structures": structures_out,
    }

    # Si no hubo Expires, usamos now
    if max_expires is None:
        max_expires = utc_now()
        max_last_modified = None

    # Write cache
    write_pages_cache_atomic(pages_cache_path, pages_cache_obj)

    # Outputs para workflow
    log(f"max_expires_epoch={int(max_expires.timestamp())}")
    log(f"max_last_modified_epoch={int(max_last_modified.timestamp()) if max_last_modified else 0}")
    log("run_completed=1")

    return 0


def sheets_initial() -> int:
    lock_time = int(os.environ.get("LOCK_TIME", "180"))
    now = utc_now()
    next_run = now + timedelta(seconds=lock_time)
    sheets_set_workflow_cells(status="in progress", next_run_dt=next_run, last_modified_dt=None)
    log("sheets_initial_done")
    return 0


def sheets_finalize() -> int:
    """
    - Si run_completed=1 => completed:
        next_run = max_expires + 5 min
        last_modified = max_last_modified (si existe)
      Si no => failed:
        next_run = now + 5 min
        last_modified no se toca
    """
    run_completed = os.environ.get("RUN_COMPLETED", "").strip() == "1"

    now = utc_now()
    if run_completed:
        max_expires_epoch = safe_int(os.environ.get("MAX_EXPIRES_EPOCH", "0"))
        max_last_mod_epoch = safe_int(os.environ.get("MAX_LAST_MODIFIED_EPOCH", "0"))

        exp = datetime.fromtimestamp(max_expires_epoch, tz=timezone.utc) if max_expires_epoch > 0 else now
        next_run = exp + timedelta(minutes=5)

        last_mod = datetime.fromtimestamp(max_last_mod_epoch, tz=timezone.utc) if max_last_mod_epoch > 0 else None

        sheets_set_workflow_cells(status="completed", next_run_dt=next_run, last_modified_dt=last_mod)
        log("sheets_finalize_completed")
        return 0

    else:
        next_run = now + timedelta(minutes=5)
        sheets_set_workflow_cells(status="failed", next_run_dt=next_run, last_modified_dt=None)
        log("sheets_finalize_failed")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheets-initial", action="store_true")
    ap.add_argument("--sheets-finalize", action="store_true")
    args = ap.parse_args()

    if args.sheets_initial:
        return sheets_initial()

    if args.sheets_finalize:
        return sheets_finalize()

    return main_run()


if __name__ == "__main__":
    sys.exit(main())
