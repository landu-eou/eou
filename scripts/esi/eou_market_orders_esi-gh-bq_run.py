#!/usr/bin/env python3
"""EOU · ESI Market Orders (regions + structures) → BigQuery Sandbox

- Descarga todas las páginas (X-Pages) de /markets/{region_id}/orders
- Descarga todas las páginas (X-Pages) de /markets/structures/{structure_id}
- Aplica filtros (excludedMarketTypes, buy/sell, Jita 4-4)
- Joins con ficheros SDE (types, stations, solarsystems) y structures.jsonl.gz
- Genera NDJSON (gzip) y carga a BigQuery en bloque (4 tablas)

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
    # ESI suele devolver RFC3339 con Z
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
# Google Sheets: leer token EVE
# -------------------------------

def sheets_get_cell(
    *,
    access_token: str,
    spreadsheet_id: str,
    tab_name: str,
    cell_a1: str,
) -> str:
    """Lee un valor (raw) de una celda A1 de Google Sheets v4."""
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
    # dedupe, preserve order
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
        self.session.headers.update({
            "User-Agent": "EOU Market Orders (GitHub Actions)"
        })

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
        timeout: int = 60,
    ) -> EsiResponse:
        url = f"{self.base_url}{path}"
        p = dict(params or {})
        p.setdefault("datasource", self.datasource)

        h = dict(headers or {})

        for attempt in range(max_retries + 1):
            r = self.session.request(method, url, params=p, headers=h, timeout=timeout)

            # Rate limiting (429)
            if allow_429 and r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = _safe_int(ra, default=5)
                time.sleep(max(1, sleep_s))
                continue

            # Error rate limiting (420)
            if r.status_code == 420:
                reset = _safe_int(r.headers.get("X-ESI-Error-Limit-Reset"), default=60)
                time.sleep(max(5, reset))
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
            max_retries=6,
            timeout=90,
        )

    def get_structure_orders(self, structure_id: int, page: int, eve_access_token: str) -> EsiResponse:
        return self._request(
            "GET",
            f"/latest/markets/structures/{structure_id}/",
            params={"page": page},
            headers={"Authorization": f"Bearer {eve_access_token}"},
            max_retries=6,
            timeout=90,
        )


# -------------------------------
# BigQuery helpers
# -------------------------------

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


def bq_load_ndjson_gz(
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

    token_sheets_access = os.environ.get("GOOGLE_SHEETS_ACCESS_TOKEN", "").strip()
    token_sheet_id = os.environ.get("TOKEN_SHEETS_ID", "").strip()
    token_tab = os.environ.get("TOKEN_TAB_NAME", "access_tokens").strip()
    token_cell = os.environ.get("TOKEN_CELL_A1", "D12").strip()

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

    schema_buy = "schemas/eou_market_orders_esi-gh-bq_buy_orders.json"
    schema_sell = "schemas/eou_market_orders_esi-gh-bq_sell_orders.json"
    schema_jita_buy = "schemas/eou_market_orders_esi-gh-bq_jita44_buy_orders.json"
    schema_jita_sell = "schemas/eou_market_orders_esi-gh-bq_jita44_sell_orders.json"

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

    # Region list
    if region_ids_raw:
        region_ids = [int(x.strip()) for x in region_ids_raw.split(",") if x.strip()]
    else:
        region_ids = client.list_regions()

    # Token provider (EVE)
    eve_token_cache: Optional[str] = None

    def get_eve_access_token(force_refresh: bool = False) -> str:
        nonlocal eve_token_cache
        if eve_token_cache and not force_refresh:
            return eve_token_cache
        if not token_sheets_access or not token_sheet_id:
            eve_token_cache = ""
            return eve_token_cache
        token = sheets_get_cell(
            access_token=token_sheets_access,
            spreadsheet_id=token_sheet_id,
            tab_name=token_tab,
            cell_a1=token_cell,
        )
        eve_token_cache = token
        return token

    # Writers
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

    with gzip.open(out_buy, "wb", compresslevel=6) as w_buy, \
         gzip.open(out_sell, "wb", compresslevel=6) as w_sell, \
         gzip.open(out_jita_buy, "wb", compresslevel=6) as w_jita_buy, \
         gzip.open(out_jita_sell, "wb", compresslevel=6) as w_jita_sell:

        # -----------------------
        # Regions
        # -----------------------
        for rid in region_ids:
            resp = client.get_region_orders(rid, page=1)
            if resp.status != 200:
                continue

            update_cache_headers(resp.headers)

            pages = _safe_int(resp.headers.get("X-Pages"), default=1)
            if pages < 1:
                pages = 1

            for page in range(1, pages + 1):
                if page > 1:
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

        # -----------------------
        # Structures
        # -----------------------
        if structure_ids:
            eve_token = get_eve_access_token(force_refresh=False)

            for sid in structure_ids:
                if not eve_token:
                    break

                page1: Optional[EsiResponse] = None
                for _ in range(10):
                    resp = client.get_structure_orders(sid, page=1, eve_access_token=eve_token)
                    if resp.status == 401:
                        time.sleep(5)
                        eve_token = get_eve_access_token(force_refresh=True)
                        continue
                    page1 = resp
                    break

                if page1 is None:
                    continue

                if page1.status in (403, 404):
                    continue

                if page1.status != 200 or not isinstance(page1.json_data, list):
                    continue

                update_cache_headers(page1.headers)

                pages = _safe_int(page1.headers.get("X-Pages"), default=1)
                if pages < 1:
                    pages = 1

                def process_structure_orders(resp: EsiResponse) -> None:
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

                process_structure_orders(page1)

                for page in range(2, pages + 1):
                    resp_p: Optional[EsiResponse] = None
                    for _ in range(10):
                        rr = client.get_structure_orders(sid, page=page, eve_access_token=eve_token)
                        if rr.status == 401:
                            time.sleep(5)
                            eve_token = get_eve_access_token(force_refresh=True)
                            continue
                        resp_p = rr
                        break

                    if resp_p is None:
                        continue

                    if resp_p.status in (403, 404):
                        break

                    if resp_p.status != 200 or not isinstance(resp_p.json_data, list):
                        continue

                    update_cache_headers(resp_p.headers)
                    process_structure_orders(resp_p)

    # BigQuery: recrear/cargar tablas "de golpe"
    table_specs = [
        (table_buy, schema_buy, out_buy, buy_count),
        (table_sell, schema_sell, out_sell, sell_count),
        (table_jita_buy, schema_jita_buy, out_jita_buy, jita_buy_count),
        (table_jita_sell, schema_jita_sell, out_jita_sell, jita_sell_count),
    ]

    for tname, schema_file, data_file, cnt in table_specs:
        # Crear o reemplazar la tabla con schema limpio (sin restos de versiones anteriores)
        bq_create_or_replace_empty_table(project_id, dataset, tname, schema_file)

        # Si hay datos, cargar en bloque (REPLACE). Si no, se queda vacía con schema correcto.
        if cnt > 0:
            bq_load_ndjson_gz(project_id, dataset, tname, schema_file, data_file)

    # Outputs para GitHub Actions (Finalize)
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
