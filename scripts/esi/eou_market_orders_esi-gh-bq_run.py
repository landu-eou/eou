#!/usr/bin/env python3
"""EOU · ESI Market Orders (regions + structures) → BigQuery Sandbox

Tokens (EXACTAMENTE como "ESI Structures"):
- NO OAuth aquí.
- Consume access tokens desde GitHub Secrets env:
  - EOU_ACCESS_TOKENS_1
  - EOU_ACCESS_TOKENS_2
- Selección: PRIMARY_CHAR_ID primero; luego resto de char_id desc
- Autenticadas (structures):
  - 401 -> sleep 30s, rota token, retry
  - 420 -> sleep 30s, retry (no necesariamente rotar)
- Presupuesto global RETRY_BUDGET=3 (401+420 autenticadas). Si se agota -> fail.

No imprime secretos.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# -------------------------------
# Utils
# -------------------------------

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


# -------------------------------
# Tokens (Secrets) - EXACT policy
# -------------------------------

class TokenPool:
    def __init__(self, primary_char_id: int, token_map: Dict[int, str]):
        self.primary_char_id = primary_char_id
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

    def current_char_id(self) -> Optional[int]:
        if not self.order:
            return None
        return self.order[self.idx]

    def current_token(self) -> str:
        cid = self.current_char_id()
        if cid is None:
            return ""
        return self.token_map.get(cid, "")

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

    d = {}
    d.update(parse(t1))
    d.update(parse(t2))
    return d


# -------------------------------
# Google Sheets (estado) - sigue igual
# -------------------------------

def sheets_get_cell(
    *,
    access_token: str,
    spreadsheet_id: str,
    tab_name: str,
    cell_a1: str,
) -> str:
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
        f"/values/{tab_name}!{cell_a1}?valueRenderOption=UNFORMATTED_VALUE"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    values = data.get("values")
    if not values or not values[0] or values[0][0] is None:
        return ""
    return str(values[0][0]).strip()


# -------------------------------
# SDE/ESI local indices
# -------------------------------

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
        is_market = bool(obj.get("market", False))
        if not is_market:
            continue
        sid_i = int(sid)
        ids.append(sid_i)
        if obj.get("station") is not None:
            names[sid_i] = str(obj.get("station"))
    seen = set()
    uniq = []
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


# -------------------------------
# ESI HTTP client
# -------------------------------

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

            # 429: respetar Retry-After
            if allow_429 and r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = _safe_int(ra, default=5)
                time.sleep(max(1, sleep_s))
                continue

            # 5xx: backoff
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


# -------------------------------
# BigQuery helpers
# -------------------------------

def run_cmd(cmd: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def bq_query(project_id: str, sql: str) -> None:
    cmd = ["bq", "--quiet", "query", f"--project_id={project_id}", "--use_legacy_sql=false", sql]
    cp = run_cmd(cmd, check=False)
    if cp.returncode != 0:
        raise RuntimeError(f"bq query failed: {cp.stderr.strip()}")


def bq_load_ndjson_gz(
    project_id: str,
    dataset: str,
    table: str,
    schema_file: str,
    data_file: str,
) -> None:
    cmd = [
        "bq", "--quiet", "load",
        f"--project_id={project_id}",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--replace=true",
        "--compression=GZIP",
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


# -------------------------------
# Main
# -------------------------------

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

    # ✅ schemas moved to schemas/esi/...
    schema_buy = "schemas/esi/eou_market_orders_esi-gh-bq_buy_orders.json"
    schema_sell = "schemas/esi/eou_market_orders_esi-gh-bq_sell_orders.json"
    schema_jita_buy = "schemas/esi/eou_market_orders_esi-gh-bq_jita44_buy_orders.json"
    schema_jita_sell = "schemas/esi/eou_market_orders_esi-gh-bq_jita44_sell_orders.json"

    out_buy = os.path.join(out_dir, "buy_orders.jsonl.gz")
    out_sell = os.path.join(out_dir, "sell_orders.jsonl.gz")
    out_jita_buy = os.path.join(out_dir, "jita44_buy_orders.jsonl.gz")
    out_jita_sell = os.path.join(out_dir, "jita44_sell_orders.jsonl.gz")

    # Load indices
    types_map = load_types_map(sde_types)
    excluded = load_excluded_types_set(sde_excluded)
    stations_map = load_stations_map(sde_stations)
    structure_ids, structures_map = load_structures_market_map(structures_file)
    systems_map = load_solarsystems_map(sde_systems)

    client = EsiClient(base_url=base_url, datasource=datasource)

    # Regions list
    if region_ids_raw:
        region_ids = [int(x.strip()) for x in region_ids_raw.split(",") if x.strip()]
    else:
        region_ids = client.list_regions()

    # Tokens pool (Secrets) - EXACT policy
    token_map = load_tokens_from_env()
    pool = TokenPool(primary_char_id=primary_char_id, token_map=token_map)

    # Global retry budget for authenticated (401+420) across entire run
    auth_retry_budget = retry_budget

    def consume_budget_or_fail() -> None:
        nonlocal auth_retry_budget
        auth_retry_budget -= 1
        if auth_retry_budget < 0:
            # presupuesto agotado => falla run
            raise RuntimeError("RETRY_BUDGET exhausted for authenticated requests (401/420).")

    # Writers + time reference
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
        if s == "station" or s == "solarsystem":
            return 0
        if s == "region":
            return 1000
        return _safe_int(s, default=0)

    def write_row(writer: gzip.GzipFile, obj: Dict[str, Any]) -> None:
        writer.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))

    # -----------------------
    # Authenticated structure fetch with EXACT retry policy
    # -----------------------
    def fetch_structure_page_with_policy(structure_id: int, page: int) -> Optional[EsiResponse]:
        """
        - 401: sleep 30s, rotate token, retry (budget--)
        - 420: sleep 30s, retry (budget--)
        - budget global across all calls
        """
        nonlocal auth_retry_budget

        if not pool.has_any():
            return None

        while True:
            tok = pool.current_token()
            if not tok:
                # no usable token
                return None

            resp = client.get_structure_orders(structure_id, page=page, bearer_token=tok)

            if resp.status == 401:
                consume_budget_or_fail()
                time.sleep(30)
                pool.rotate()
                continue

            if resp.status == 420:
                consume_budget_or_fail()
                time.sleep(30)
                # no rotation required
                continue

            return resp

    with gzip.open(out_buy, "wb", compresslevel=6) as w_buy, \
         gzip.open(out_sell, "wb", compresslevel=6) as w_sell, \
         gzip.open(out_jita_buy, "wb", compresslevel=6) as w_jita_buy, \
         gzip.open(out_jita_sell, "wb", compresslevel=6) as w_jita_sell:

        # -----------------------
        # Regions (no auth)
        # -----------------------
        for rid in region_ids:
            # tolerancia mínima a 420 sin tocar auth budget (porque esto no es auth)
            local_420_budget = 2

            resp = client.get_region_orders(rid, page=1)
            while resp.status == 420 and local_420_budget > 0:
                local_420_budget -= 1
                time.sleep(30)
                resp = client.get_region_orders(rid, page=1)

            if resp.status != 200:
                continue

            update_cache_headers(resp.headers)
            pages = _safe_int(resp.headers.get("X-Pages"), default=1)
            pages = max(1, pages)

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

                        # ✅ timeLeft = until - now
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
                            # ✅ sell tables: is_buy_order=false (ya viene del API)
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

        # -----------------------
        # Structures (auth) - with EXACT policy
        # -----------------------
        if structure_ids and pool.has_any():
            for sid in structure_ids:
                page1 = fetch_structure_page_with_policy(sid, page=1)
                if page1 is None:
                    break

                # 403/404: skip structure
                if page1.status in (403, 404):
                    continue

                if page1.status != 200 or not isinstance(page1.json_data, list):
                    continue

                update_cache_headers(page1.headers)

                pages = _safe_int(page1.headers.get("X-Pages"), default=1)
                pages = max(1, pages)

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

                            # ✅ timeLeft = until - now
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
                        # sin tokens utilizables
                        break
                    if resp_p.status in (403, 404):
                        break
                    if resp_p.status != 200 or not isinstance(resp_p.json_data, list):
                        continue

                    update_cache_headers(resp_p.headers)
                    process_orders(resp_p)

    # BigQuery: recrear/cargar tablas "de golpe"
    table_specs = [
        (table_buy, schema_buy, out_buy, buy_count),
        (table_sell, schema_sell, out_sell, sell_count),
        (table_jita_buy, schema_jita_buy, out_jita_buy, jita_buy_count),
        (table_jita_sell, schema_jita_sell, out_jita_sell, jita_sell_count),
    ]

    for tname, schema_file, data_file, cnt in table_specs:
        bq_create_or_replace_empty_table(project_id, dataset, tname, schema_file)
        if cnt > 0:
            bq_load_ndjson_gz(project_id, dataset, tname, schema_file, data_file)

    # Outputs para finalize
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
