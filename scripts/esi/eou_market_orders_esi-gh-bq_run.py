#!/usr/bin/env python3
"""
EOU · ESI Market Orders (regions + structures) → BigQuery Sandbox

Cambios vs versión anterior:
✅ Quitado GZIP para evitar `bq load --compression` (no soportado en tu entorno).
✅ Ahora escribe NDJSON plano (*.jsonl) y lo carga con bq load estándar.

Se mantiene:
- Tokens + rotación + budget EXACTOS
- Logs por estructura
- timeLeft = until - now
- create/replace tablas y load replace
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gzip
import requests


# =============================================================================
# Utils
# =============================================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_http_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _parse_iso_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_hh_mm_ss(td: timedelta) -> str:
    total = int(td.total_seconds())
    if total < 0:
        total = 0
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _log(msg: str) -> None:
    print(msg, flush=True)


def normalize_token(tok: str) -> str:
    tok = (tok or "").strip()
    if tok.lower().startswith("bearer "):
        parts = tok.split(None, 1)
        tok = parts[1].strip() if len(parts) > 1 else ""
    return tok


# =============================================================================
# Tokens (Secrets) - EXACT policy
# =============================================================================

class TokenPool:
    def __init__(self, primary_char_id: int, token_map: Dict[int, str]):
        self.primary_char_id = int(primary_char_id)
        self.token_map = {int(k): str(v) for k, v in token_map.items() if str(v).strip()}
        self.order: List[int] = self._build_order()
        self.idx = 0

    def _build_order(self) -> List[int]:
        ids = list(self.token_map.keys())
        order: List[int] = []
        if self.primary_char_id in self.token_map:
            order.append(self.primary_char_id)
        rest = sorted([i for i in ids if i != self.primary_char_id], reverse=True)
        order.extend(rest)
        return order

    def has_any(self) -> bool:
        return len(self.order) > 0

    def count(self) -> int:
        return len(self.order)

    def current_char_id(self) -> Optional[int]:
        if not self.order:
            return None
        return self.order[self.idx]

    def current_token_raw(self) -> str:
        cid = self.current_char_id()
        if cid is None:
            return ""
        return self.token_map.get(cid, "")

    def current_token(self) -> str:
        return normalize_token(self.current_token_raw())

    def rotate(self) -> None:
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
# SDE/ESI local indices
# =============================================================================

def _read_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_types_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in _read_jsonl_gz(path):
        tid = obj.get("typeID")
        name = obj.get("type")
        if tid is None or name is None:
            continue
        out[int(tid)] = str(name)
    return out


def load_excluded_types_set(path: str) -> set[int]:
    out: set[int] = set()
    for obj in _read_jsonl_gz(path):
        tid = obj.get("typeID")
        if tid is None:
            continue
        out.add(int(tid))
    return out


def load_stations_map(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in _read_jsonl_gz(path):
        sid = obj.get("stationID")
        name = obj.get("station")
        if sid is None or name is None:
            continue
        out[int(sid)] = str(name)
    return out


def load_structures_market_map(path: str) -> Tuple[List[int], Dict[int, str]]:
    ids: List[int] = []
    names: Dict[int, str] = {}
    for obj in _read_jsonl_gz(path):
        sid = obj.get("stationID")
        if sid is None:
            continue
        if not bool(obj.get("market", False)):
            continue
        sid_i = int(sid)
        ids.append(sid_i)
        if obj.get("station") is not None:
            names[sid_i] = str(obj.get("station"))
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
    for obj in _read_jsonl_gz(path):
        sid = obj.get("solarSystemID")
        if sid is None:
            continue
        solar = str(obj.get("solarSystem") or "")
        const = str(obj.get("constellation") or "")
        region = str(obj.get("region") or "")
        out[int(sid)] = (solar, const, region)
    return out


# =============================================================================
# ESI HTTP client
# =============================================================================

@dataclass
class EsiResponse:
    status: int
    json_data: Any
    headers: Dict[str, str]


class EsiClient:
    def __init__(self, base_url: str, datasource: str):
        self.base_url = base_url.rstrip("/")
        self.datasource = datasource
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "EOU Market Orders (GitHub Actions)"})

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 6,
        retry_backoff_base: float = 1.5,
        retry_on_status: Tuple[int, ...] = (502, 503, 504, 520),
        allow_429: bool = True,
        timeout: int = 90,
    ) -> EsiResponse:
        url = f"{self.base_url}{path}"
        p = dict(params or {})
        p.setdefault("datasource", self.datasource)
        h = dict(headers or {})

        for attempt in range(max_retries + 1):
            r = self.session.request(method, url, params=p, headers=h, timeout=timeout)

            if allow_429 and r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = _safe_int(ra, default=5)
                time.sleep(max(1, sleep_s))
                continue

            if r.status_code in retry_on_status and attempt < max_retries:
                time.sleep(retry_backoff_base ** attempt)
                continue

            try:
                data = r.json()
            except Exception:
                data = None

            return EsiResponse(status=r.status_code, json_data=data, headers=dict(r.headers))

        raise RuntimeError("ESI request retry loop exhausted")

    def list_regions(self) -> List[int]:
        resp = self._request("GET", "/latest/universe/regions/")
        if resp.status != 200 or not isinstance(resp.json_data, list):
            raise RuntimeError(f"Failed to list regions: status={resp.status}")
        return [int(x) for x in resp.json_data]

    def get_region_orders(self, region_id: int, page: int) -> EsiResponse:
        return self._request(
            "GET",
            f"/latest/markets/{region_id}/orders/",
            params={"order_type": "all", "page": page},
        )

    def get_structure_orders(self, structure_id: int, page: int, bearer_token: str) -> EsiResponse:
        return self._request(
            "GET",
            f"/latest/markets/structures/{structure_id}/",
            params={"page": page},
            headers={"Authorization": f"Bearer {bearer_token}"},
        )


# =============================================================================
# BigQuery helpers
# =============================================================================

def run_cmd(cmd: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def bq_query(project_id: str, sql: str) -> None:
    cmd = [
        "bq",
        "--quiet",
        "query",
        f"--project_id={project_id}",
        "--use_legacy_sql=false",
        sql,
    ]
    cp = run_cmd(cmd, check=False)
    if cp.returncode != 0:
        raise RuntimeError(f"bq query failed: {cp.stderr.strip()}")


def bq_load_ndjson(
    project_id: str,
    dataset: str,
    table: str,
    schema_file: str,
    data_file: str,
) -> None:
    cmd = [
        "bq",
        "--quiet",
        "load",
        f"--project_id={project_id}",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--replace=true",
        f"--schema={schema_file}",
        f"{project_id}:{dataset}.{table}",
        data_file,
    ]
    cp = run_cmd(cmd, check=False)
    if cp.returncode != 0:
        raise RuntimeError(f"bq load failed ({dataset}.{table}): {cp.stderr.strip()}")


def bq_create_or_replace_empty_table(
    project_id: str,
    dataset: str,
    table: str,
    schema_file: str,
) -> None:
    schema = json.loads(open(schema_file, "r", encoding="utf-8").read())

    def ddl_type(t: str) -> str:
        t = t.upper()
        if t == "FLOAT":
            return "FLOAT64"
        if t == "INTEGER":
            return "INT64"
        if t == "TIMESTAMP":
            return "TIMESTAMP"
        if t == "STRING":
            return "STRING"
        return t

    cols = []
    for f in schema:
        name = f["name"]
        typ = ddl_type(f["type"])
        mode = f.get("mode", "NULLABLE").upper()
        not_null = " NOT NULL" if mode == "REQUIRED" else ""
        cols.append(f"`{name}` {typ}{not_null}")

    sql = f"CREATE OR REPLACE TABLE `{project_id}.{dataset}.{table}` (" + ", ".join(cols) + ")"
    bq_query(project_id, sql)


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    base_url = os.environ.get("ESI_BASE_URL", "https://esi.evetech.net")
    datasource = os.environ.get("ESI_DATASOURCE", "tranquility")
    region_ids_raw = os.environ.get("REGION_IDS", "").strip()

    primary_char_id = int(os.environ.get("PRIMARY_CHAR_ID", "2124070822"))
    retry_budget = int(os.environ.get("RETRY_BUDGET", "3"))

    sde_types = os.environ["SDE_TYPES_PATH"]
    sde_excluded = os.environ["SDE_EXCLUDED_TYPES_PATH"]
    sde_stations = os.environ["SDE_STATIONS_PATH"]
    sde_systems = os.environ["SDE_SOLARSYSTEMS_PATH"]
    structures_file = os.environ["STRUCTURES_FILE"]

    out_dir = os.environ.get("OUT_DIR", ".tmp_eou_market_orders")
    os.makedirs(out_dir, exist_ok=True)

    jita44_location_id = int(os.environ.get("JITA44_LOCATION_ID", "60003760"))

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "eou")
    table_buy = os.environ.get("BQ_TABLE_BUY", "buy_orders")
    table_sell = os.environ.get("BQ_TABLE_SELL", "sell_orders")
    table_jita_buy = os.environ.get("BQ_TABLE_JITA44_BUY", "jita44_buy_orders")
    table_jita_sell = os.environ.get("BQ_TABLE_JITA44_SELL", "jita44_sell_orders")

    schema_buy = "schemas/esi/eou_market_orders_esi-gh-bq_buy_orders.json"
    schema_sell = "schemas/esi/eou_market_orders_esi-gh-bq_sell_orders.json"
    schema_jita_buy = "schemas/esi/eou_market_orders_esi-gh-bq_jita44_buy_orders.json"
    schema_jita_sell = "schemas/esi/eou_market_orders_esi-gh-bq_jita44_sell_orders.json"

    # ✅ NDJSON plano (sin gzip)
    out_buy = os.path.join(out_dir, "buy_orders.jsonl")
    out_sell = os.path.join(out_dir, "sell_orders.jsonl")
    out_jita_buy = os.path.join(out_dir, "jita44_buy_orders.jsonl")
    out_jita_sell = os.path.join(out_dir, "jita44_sell_orders.jsonl")

    _log("loading indices...")
    types_map = load_types_map(sde_types)
    excluded = load_excluded_types_set(sde_excluded)
    stations_map = load_stations_map(sde_stations)
    structure_ids, structures_map = load_structures_market_map(structures_file)
    systems_map = load_solarsystems_map(sde_systems)

    client = EsiClient(base_url=base_url, datasource=datasource)

    if region_ids_raw:
        region_ids = [int(x.strip()) for x in region_ids_raw.split(",") if x.strip()]
    else:
        region_ids = client.list_regions()

    token_map = load_tokens_from_env()
    pool = TokenPool(primary_char_id=primary_char_id, token_map=token_map)

    _log(f"regions_count={len(region_ids)}")
    _log(f"structures_market_count={len(structure_ids)}")
    _log(f"tokens_count={pool.count()} primary_char_id={primary_char_id}")

    # Token shape logs
    if pool.has_any():
        for cid in pool.order:
            raw = (pool.token_map.get(cid, "") or "").strip()
            _log(
                f"token_shape char_id={cid} has_bearer_prefix={raw.lower().startswith('bearer ')} len={len(raw)}"
            )

    auth_retry_budget = retry_budget

    def consume_budget_or_fail(reason: str, structure_id: int, page: int, char_id: Optional[int]) -> None:
        nonlocal auth_retry_budget
        if auth_retry_budget <= 0:
            _log(
                f"RETRY_BUDGET_EXHAUSTED reason={reason} structure_id={structure_id} "
                f"page={page} char_id={char_id}"
            )
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated requests (401/420).")
        auth_retry_budget -= 1

    buy_count = sell_count = jita_buy_count = jita_sell_count = 0
    now_ref = _utc_now()

    max_expires: Optional[datetime] = None
    max_last_modified: Optional[datetime] = None

    def update_cache_headers(headers: Dict[str, str]) -> None:
        nonlocal max_expires, max_last_modified
        exp = _parse_http_date(headers.get("Expires"))
        if exp is None:
            return
        if max_expires is None or exp > max_expires:
            max_expires = exp
            max_last_modified = _parse_http_date(headers.get("Last-Modified"))

    def station_name_from_location(location_id: int) -> str:
        if location_id < 1_000_000_000_000:
            return stations_map.get(location_id, "")
        return structures_map.get(location_id, "")

    def system_info(system_id: Optional[int]) -> Tuple[str, str, str]:
        if system_id is None:
            return ("", "", "")
        return systems_map.get(int(system_id), ("", "", ""))

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
        return _safe_int(s, default=0)

    def write_row(writer, obj: Dict[str, Any]) -> None:
        writer.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def fetch_structure_page_with_policy(structure_id: int, page: int) -> Optional[EsiResponse]:
        if not pool.has_any():
            return None

        while True:
            cid = pool.current_char_id()
            tok = pool.current_token()
            if not tok:
                return None

            resp = client.get_structure_orders(structure_id, page=page, bearer_token=tok)

            if resp.status == 401:
                consume_budget_or_fail("401", structure_id, page, cid)
                _log(
                    f"auth_retry=401 structure_id={structure_id} page={page} "
                    f"char_id={cid} remaining_budget={auth_retry_budget}"
                )
                time.sleep(30)
                pool.rotate()
                continue

            if resp.status == 420:
                consume_budget_or_fail("420", structure_id, page, cid)
                _log(
                    f"auth_retry=420 structure_id={structure_id} page={page} "
                    f"char_id={cid} remaining_budget={auth_retry_budget}"
                )
                time.sleep(30)
                continue

            return resp

    struct_ok = 0
    struct_skipped_403 = 0
    struct_skipped_404 = 0
    struct_bad_status = 0
    struct_errors = 0

    with open(out_buy, "w", encoding="utf-8") as w_buy, \
         open(out_sell, "w", encoding="utf-8") as w_sell, \
         open(out_jita_buy, "w", encoding="utf-8") as w_jita_buy, \
         open(out_jita_sell, "w", encoding="utf-8") as w_jita_sell:

        # Regions (no auth)
        _log("phase=regions start")
        for rid in region_ids:
            local_420_budget = 2
            resp = client.get_region_orders(rid, page=1)
            while resp.status == 420 and local_420_budget > 0:
                local_420_budget -= 1
                time.sleep(30)
                resp = client.get_region_orders(rid, page=1)

            if resp.status != 200:
                continue

            update_cache_headers(resp.headers)

            pages = max(1, _safe_int(resp.headers.get("X-Pages"), default=1))
            for page in range(1, pages + 1):
                if page > 1:
                    local_420_budget_p = 2
                    resp = client.get_region_orders(rid, page=page)
                    while resp.status == 420 and local_420_budget_p > 0:
                        local_420_budget_p -= 1
                        time.sleep(30)
                        resp = client.get_region_orders(rid, page=page)

                if resp.status != 200 or not isinstance(resp.json_data, list):
                    continue

                update_cache_headers(resp.headers)

                for o in resp.json_data:
                    try:
                        type_id = int(o.get("type_id"))
                        if type_id in excluded:
                            continue

                        is_buy = bool(o.get("is_buy_order"))
                        issued_dt = _parse_iso_utc(str(o.get("issued")))
                        duration_days = int(o.get("duration"))
                        until_dt = issued_dt + timedelta(days=duration_days)
                        time_left = _format_hh_mm_ss(until_dt - now_ref)

                        row_common = {
                            "type": types_map.get(type_id),
                            "orderPrice": _safe_float(o.get("price")),
                            "volRemain": _safe_int(o.get("volume_remain")),
                            "volTotal": _safe_int(o.get("volume_total")),
                            "volMin": _safe_int(o.get("min_volume")),
                            "issued": _isoformat_utc(issued_dt),
                            "until": _isoformat_utc(until_dt),
                            "timeLeft": time_left,
                            "orderID": _safe_int(o.get("order_id")),
                        }

                        loc_id = _safe_int(o.get("location_id"))
                        sys_id = o.get("system_id")

                        if is_buy:
                            rint = range_to_int(o.get("range"))
                            solar, const, reg = system_info(sys_id)

                            out = dict(row_common)
                            out.update({
                                "station": station_name_from_location(loc_id),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                                "ordeRange": rint,
                            })
                            write_row(w_buy, out)
                            buy_count += 1

                            if loc_id == jita44_location_id:
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
                                write_row(w_jita_buy, j)
                                jita_buy_count += 1
                        else:
                            solar, const, reg = system_info(sys_id)

                            out = dict(row_common)
                            out.update({
                                "station": station_name_from_location(loc_id),
                                "solarSystem": solar,
                                "constellation": const,
                                "region": reg,
                            })
                            write_row(w_sell, out)
                            sell_count += 1

                            if loc_id == jita44_location_id:
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
                                write_row(w_jita_sell, j)
                                jita_sell_count += 1
                    except Exception:
                        continue

        _log("phase=regions end")

        # Structures (auth)
        _log("phase=structures start")
        if structure_ids and pool.has_any():
            total_structs = len(structure_ids)

            for idx, sid in enumerate(structure_ids, start=1):
                sname = structures_map.get(sid, "")
                label = f"{sid}" + (f' "{sname}"' if sname else "")
                _log(f"structure_start idx={idx}/{total_structs} structure_id={label}")

                try:
                    page1 = fetch_structure_page_with_policy(sid, page=1)
                except RuntimeError:
                    raise
                except Exception as e:
                    struct_errors += 1
                    _log(f"structure_error structure_id={sid} err={type(e).__name__}")
                    continue

                if page1 is None:
                    _log("structure_abort reason=no_tokens")
                    break

                st = page1.status
                xpages = page1.headers.get("X-Pages", "")
                _log(f"structure_page1 status={st} structure_id={sid} xpages={xpages}")

                if st == 403:
                    struct_skipped_403 += 1
                    _log(f"structure_skip status=403 structure_id={sid}")
                    continue
                if st == 404:
                    struct_skipped_404 += 1
                    _log(f"structure_skip status=404 structure_id={sid}")
                    continue
                if st != 200 or not isinstance(page1.json_data, list):
                    struct_bad_status += 1
                    _log(f"structure_bad status={st} structure_id={sid}")
                    continue

                update_cache_headers(page1.headers)

                pages = max(1, _safe_int(page1.headers.get("X-Pages"), default=1))

                def process_orders(resp: EsiResponse) -> None:
                    nonlocal buy_count, sell_count, jita_buy_count, jita_sell_count
                    for o in resp.json_data:
                        try:
                            type_id = int(o.get("type_id"))
                            if type_id in excluded:
                                continue

                            is_buy = bool(o.get("is_buy_order"))
                            issued_dt = _parse_iso_utc(str(o.get("issued")))
                            duration_days = int(o.get("duration"))
                            until_dt = issued_dt + timedelta(days=duration_days)
                            time_left = _format_hh_mm_ss(until_dt - now_ref)

                            row_common = {
                                "type": types_map.get(type_id),
                                "orderPrice": _safe_float(o.get("price")),
                                "volRemain": _safe_int(o.get("volume_remain")),
                                "volTotal": _safe_int(o.get("volume_total")),
                                "volMin": _safe_int(o.get("min_volume")),
                                "issued": _isoformat_utc(issued_dt),
                                "until": _isoformat_utc(until_dt),
                                "timeLeft": time_left,
                                "orderID": _safe_int(o.get("order_id")),
                            }

                            loc_id = _safe_int(o.get("location_id"))
                            sys_id = o.get("system_id")

                            if is_buy:
                                rint = range_to_int(o.get("range"))
                                solar, const, reg = system_info(sys_id)

                                out = dict(row_common)
                                out.update({
                                    "station": station_name_from_location(loc_id),
                                    "solarSystem": solar,
                                    "constellation": const,
                                    "region": reg,
                                    "ordeRange": rint,
                                })
                                write_row(w_buy, out)
                                buy_count += 1

                                if loc_id == jita44_location_id:
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
                                    write_row(w_jita_buy, j)
                                    jita_buy_count += 1
                            else:
                                solar, const, reg = system_info(sys_id)

                                out = dict(row_common)
                                out.update({
                                    "station": station_name_from_location(loc_id),
                                    "solarSystem": solar,
                                    "constellation": const,
                                    "region": reg,
                                })
                                write_row(w_sell, out)
                                sell_count += 1

                                if loc_id == jita44_location_id:
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
                                    write_row(w_jita_sell, j)
                                    jita_sell_count += 1
                        except Exception:
                            continue

                process_orders(page1)

                for page in range(2, pages + 1):
                    resp_p = fetch_structure_page_with_policy(sid, page=page)
                    if resp_p is None:
                        _log(f"structure_abort structure_id={sid} reason=no_tokens")
                        break
                    if resp_p.status in (403, 404):
                        _log(f"structure_stop structure_id={sid} page={page} status={resp_p.status}")
                        break
                    if resp_p.status != 200 or not isinstance(resp_p.json_data, list):
                        _log(f"structure_bad structure_id={sid} page={page} status={resp_p.status}")
                        continue
                    update_cache_headers(resp_p.headers)
                    process_orders(resp_p)

                struct_ok += 1
                _log(f"structure_done status=ok structure_id={sid} pages={pages}")

        _log("phase=structures end")

    # BigQuery load
    _log("phase=bq start")
    table_specs = [
        (table_buy, schema_buy, out_buy, buy_count),
        (table_sell, schema_sell, out_sell, sell_count),
        (table_jita_buy, schema_jita_buy, out_jita_buy, jita_buy_count),
        (table_jita_sell, schema_jita_sell, out_jita_sell, jita_sell_count),
    ]

    for tname, schema_file, data_file, cnt in table_specs:
        _log(f"bq_table_prepare table={dataset}.{tname} rows={cnt}")
        bq_create_or_replace_empty_table(project_id, dataset, tname, schema_file)
        if cnt > 0:
            bq_load_ndjson(project_id, dataset, tname, schema_file, data_file)

    _log("phase=bq end")

    _log(
        "summary "
        f"rows_buy={buy_count} rows_sell={sell_count} "
        f"rows_jita_buy={jita_buy_count} rows_jita_sell={jita_sell_count}"
    )
    _log(
        "structures_summary "
        f"ok={struct_ok} skipped_403={struct_skipped_403} skipped_404={struct_skipped_404} "
        f"bad_status={struct_bad_status} errors={struct_errors} "
        f"remaining_auth_budget={auth_retry_budget}"
    )

    if max_expires is None:
        max_expires = _utc_now()
        max_last_modified = None

    max_expires_epoch = int(max_expires.timestamp())
    max_last_modified_epoch = int(max_last_modified.timestamp()) if max_last_modified else 0

    print(f"max_expires_epoch={max_expires_epoch}")
    print(f"max_last_modified_epoch={max_last_modified_epoch}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
