"""
EOU · SDE Dataset (SDE → GH) — build outputs

Genera (sobrescribe) en el directorio de salida:
  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz
  - stations.jsonl.gz
  - stargates.jsonl.gz
  - corporations.jsonl.gz
  - sdesi/types.jsonl.gz   (MOVED + packagedVolume enrichment)

Regla de packagedVolume:
  - Sin ESI: packagedVolume = volume (SDE)
  - Con ESI: packagedVolume = packaged_volume (ESI) si existe, si no, fallback volume(SDE)

Regla de ETag:
  - RESET_ETAGS=true:
      * categorías != Ship/Module/Celestial: etag=null siempre
      * categorías Ship/Module/Celestial: llamar ESI y guardar etag SOLO si packagedVolume(ESI) != volume(SDE)
  - RESET_ETAGS=false:
      * llamar ESI SOLO si etag previo != null
      * + discovery de NUEVOS types (no existían antes) en Ship/Module/Celestial
      * si ESI devuelve 200 y packagedVolume(ESI) == volume(SDE), entonces etag se limpia a null

ESI caching: If-None-Match + 304 Not Modified (sin body) está recomendado por CCP. :contentReference[oaicite:8]{index=8}
ESI rate limiting: 429 + Retry-After, y bucket tokens por status code. :contentReference[oaicite:9]{index=9}

No usa dependencias externas (stdlib only).
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import zipfile

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from eou_sde_dataset_sde_to_gh_io import iter_jsonl_from_zip, read_jsonl_gz, write_jsonl_gz  # noqa: E402
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name  # noqa: E402
from eou_sde_dataset_sde_to_gh_cynodock import station_cyno, system_cyno  # noqa: E402


# -----------------------------
# ESI helpers (packaged_volume + ETag caching)
# -----------------------------

ESI_TYPE_URL = "https://esi.evetech.net/latest/universe/types/{type_id}/?datasource=tranquility"


def _http_get_json(url: str, headers: Dict[str, str], timeout: int = 30) -> Tuple[int, Dict[str, str], Optional[Dict]]:
    """
    Returns: (status_code, response_headers_lower, json_obj_or_none)
    """
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            hdrs = {k.lower(): v for (k, v) in resp.headers.items()}
            body = resp.read()
            obj = json.loads(body.decode("utf-8")) if body else None
            return status, hdrs, obj
    except HTTPError as e:
        status = int(e.code)
        hdrs = {k.lower(): v for (k, v) in (e.headers.items() if e.headers else [])}
        body = e.read() if hasattr(e, "read") else b""
        obj = None
        if body:
            try:
                obj = json.loads(body.decode("utf-8"))
            except Exception:
                obj = None
        return status, hdrs, obj
    except URLError:
        return 0, {}, None


def _to_decimal(x: Optional[float]) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def _vol_equal(a: Optional[float], b: Optional[float]) -> bool:
    """
    Comparación exacta por representación decimal (evita sorpresas de float).
    Si alguno es None => no iguales.
    """
    da = _to_decimal(a)
    db = _to_decimal(b)
    if da is None or db is None:
        return False
    return da == db


def fetch_packaged_volume(
    type_id: int,
    prev_etag: Optional[str],
    use_if_none_match: bool,
    max_attempts: int = 8,
) -> Tuple[int, Optional[float], Optional[str], Dict[str, str]]:
    """
    Returns: (status, packaged_volume_or_none, etag_or_none, headers_lower)
    Handles:
      - 429: respects Retry-After (seconds) :contentReference[oaicite:10]{index=10}
      - error-limit headers (X-ESI-Error-Limit-*) :contentReference[oaicite:11]{index=11}
      - 5xx/network: backoff retry
    """
    url = ESI_TYPE_URL.format(type_id=type_id)

    headers = {
        "Accept": "application/json",
        "User-Agent": "EOU-SDE-Dataset/1.0 (GitHub Actions)",
    }
    if use_if_none_match and prev_etag:
        headers["If-None-Match"] = prev_etag

    attempt = 0
    backoff = 1.0

    while attempt < max_attempts:
        attempt += 1
        status, hdrs, obj = _http_get_json(url, headers=headers)

        # Normalize common headers for decisions/logging
        etag = hdrs.get("etag") or (prev_etag if status == 304 else None)
        retry_after = hdrs.get("retry-after")
        err_rem = hdrs.get("x-esi-error-limit-remain")
        err_reset = hdrs.get("x-esi-error-limit-reset")

        # Control HTTP (429)
        if status == 429:
            wait_s = None
            if retry_after:
                try:
                    wait_s = float(retry_after)
                except Exception:
                    wait_s = None
            if wait_s is None:
                wait_s = backoff
                backoff = min(backoff * 2.0, 60.0)
            time.sleep(max(0.5, min(wait_s, 180.0)))
            continue

        # Error-limit triggered (legacy): often 420 elsewhere; we treat any non-2xx/3xx burst carefully.
        if status == 420:
            # If CCP sends reset seconds, respect it; else backoff.
            wait_s = None
            if err_reset:
                try:
                    wait_s = float(err_reset)
                except Exception:
                    wait_s = None
            if wait_s is None:
                wait_s = max(10.0, backoff)
                backoff = min(backoff * 2.0, 120.0)
            time.sleep(min(wait_s, 300.0))
            continue

        if status == 0:
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
            continue

        if 500 <= status <= 599:
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
            continue

        if status == 304:
            return 304, None, etag, hdrs

        if status == 200 and isinstance(obj, dict):
            pv = obj.get("packaged_volume")
            pv_f: Optional[float]
            try:
                pv_f = float(pv) if pv is not None else None
            except Exception:
                pv_f = None
            return 200, pv_f, etag, hdrs

        # Other 4xx -> do not hammer; stop.
        return status, None, etag, hdrs

    return 0, None, prev_etag, {}


def load_repo_types_cache(repo_types_path: Path) -> Dict[int, Dict]:
    """
    Load previous data from data/sdesi/types.jsonl.gz

    Stored fields of interest:
      - typeID
      - packagedVolume
      - etag
    """
    cache: Dict[int, Dict] = {}
    for row in read_jsonl_gz(repo_types_path):
        try:
            tid = int(row.get("typeID"))
        except Exception:
            continue

        pv = row.get("packagedVolume")
        et = row.get("etag")

        try:
            pv_f = float(pv) if pv is not None else None
        except Exception:
            pv_f = None

        et_s = et if isinstance(et, str) and et else None

        cache[tid] = {"packagedVolume": pv_f, "etag": et_s}
    return cache


# -----------------------------
# Helpers SDE (lecturas)
# -----------------------------

def _read_regions(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapRegions.jsonl"):
        rid = int(obj.get("_key"))
        out[rid] = safe_en_name(obj, fallback=str(rid))
    return out


def _read_constellations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapConstellations.jsonl"):
        cid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(cid))
        rid = int(obj.get("regionID"))
        out[cid] = (name, rid)
    return out


def _read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int, int]]:
    out: Dict[int, Tuple[str, int, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(sid))
        cid = int(obj.get("constellationID"))
        rid = int(obj.get("regionID"))
        out[sid] = (name, cid, rid)
    return out


def _read_planet_orbit_names(
    zf: zipfile.ZipFile,
    systems: Dict[int, Tuple[str, int, int]],
) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        pid = int(obj.get("_key"))
        solar_system_id = int(obj.get("solarSystemID"))
        ss_name = systems.get(solar_system_id, (str(solar_system_id), 0, 0))[0]
        cidx = int(obj.get("celestialIndex"))
        out[pid] = planet_name(ss_name, cidx)
    return out


def _read_moon_orbit_names(
    zf: zipfile.ZipFile,
    planet_orbits: Dict[int, str],
) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        mid = int(obj.get("_key"))
        planet_id = int(obj.get("orbitID"))
        p_orbit = planet_orbits.get(planet_id, str(planet_id))
        oidx = int(obj.get("orbitIndex"))
        out[mid] = moon_name(p_orbit, oidx)
    return out


def _read_corporations(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "npcCorporations.jsonl"):
        cid = int(obj.get("_key"))
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def _read_station_services(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}

    def normalize(s: str) -> str:
        x = s.strip().lower()
        x = re.sub(r"\s+", "-", x)
        x = x.replace("_", "-")
        x = re.sub(r"[^a-z0-9\-]", "", x)
        x = re.sub(r"-{2,}", "-", x).strip("-")
        return x

    CANONICAL = {
        "docking": "docking",
        "market": "market",
        "storage": "storage",
        "repair-facilities": "repair-facilities",
        "repair": "repair-facilities",
        "fitting": "fitting",
        "cloning": "cloning",
        "jump-clone-facility": "jump-clone-facility",
        "jump-clone": "jump-clone-facility",
    }

    for obj in iter_jsonl_from_zip(zf, "stationServices.jsonl"):
        sid = int(obj.get("_key"))
        sn = obj.get("serviceName") or {}
        en = sn.get("en") if isinstance(sn, dict) else None
        en = en if isinstance(en, str) else str(sid)
        key = normalize(en)
        out[sid] = CANONICAL.get(key, key)

    return out


def _read_station_operations(zf: zipfile.ZipFile, service_keys: Dict[int, str]) -> Dict[int, Dict]:
    ops: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "stationOperations.jsonl"):
        oid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(oid))
        use_op = bool(obj.get("useOperationName", False))

        svc_ids = obj.get("services")
        svc_set: Set[str] = set()
        if isinstance(svc_ids, list):
            for s_id in svc_ids:
                try:
                    sid_int = int(s_id)
                except Exception:
                    continue
                key = service_keys.get(sid_int)
                if key:
                    svc_set.add(key)

        ops[oid] = {"operationName": name, "useOperationName": use_op, "services": svc_set}
    return ops


def _read_type_name_map(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        tid = int(obj.get("_key"))
        out[tid] = safe_en_name(obj, fallback=str(tid))
    return out


def _read_marketgroup_names(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "marketGroups.jsonl"):
        mgid = int(obj.get("_key"))
        out[mgid] = safe_en_name(obj, fallback=str(mgid))
    return out


def _read_contraband_set(zf: zipfile.ZipFile) -> Set[int]:
    ids: Set[int] = set()
    for obj in iter_jsonl_from_zip(zf, "contrabandTypes.jsonl"):
        ids.add(int(obj.get("_key")))
    return ids


def _read_categories(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "categories.jsonl"):
        cid = int(obj.get("_key"))
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def _read_groups_meta(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "groups.jsonl"):
        gid = int(obj.get("_key"))
        gname = safe_en_name(obj, fallback=str(gid))
        cat_id = int(obj.get("categoryID"))
        out[gid] = (gname, cat_id)
    return out


def _get_int(obj: Dict, *keys: str) -> Optional[int]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return int(obj[k])
            except Exception:
                return None
    return None


def _get_bool(obj: Dict, *keys: str, default: bool = False) -> bool:
    for k in keys:
        if k in obj:
            return bool(obj[k])
    return default


def _get_float(obj: Dict, *keys: str) -> Optional[float]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return float(obj[k])
            except Exception:
                return None
    return None


# -----------------------------
# Builders (outputs)
# -----------------------------

def build_regions_out(regions: Dict[int, str]) -> List[Dict]:
    rows = [{"regionID": rid, "region": name} for rid, name in regions.items()]
    rows.sort(key=lambda r: r["regionID"])
    return rows


def build_constellations_out(consts: Dict[int, Tuple[str, int]], regions: Dict[int, str]) -> List[Dict]:
    rows: List[Dict] = []
    for cid, (cname, rid) in consts.items():
        rows.append({"constellationID": cid, "constellation": cname, "region": regions.get(rid, str(rid))})
    rows.sort(key=lambda r: r["constellationID"])
    return rows


def build_solarsystems_out(
    systems: Dict[int, Tuple[str, int, int]],
    consts: Dict[int, Tuple[str, int]],
    regions: Dict[int, str],
    system_cyno_jump: Dict[int, str],
) -> List[Dict]:
    rows: List[Dict] = []
    for sid, (sname, cid, rid) in systems.items():
        cname = consts.get(cid, (str(cid), 0))[0]
        rows.append(
            {
                "solarSystemID": sid,
                "solarSystem": sname,
                "constellation": cname,
                "region": regions.get(rid, str(rid)),
                "cynoJumpSecurity": system_cyno_jump.get(sid, "no jump"),
            }
        )
    rows.sort(key=lambda r: r["solarSystemID"])
    return rows


def build_corporations_out(corp_names: Dict[int, str]) -> List[Dict]:
    rows: List[Dict] = [{"corporationID": cid, "corporation": name} for cid, name in corp_names.items()]
    rows.sort(key=lambda r: r["corporationID"])
    return rows


def build_stations_out(
    zf: zipfile.ZipFile,
    systems: Dict[int, Tuple[str, int, int]],
    corp_names: Dict[int, str],
    operations: Dict[int, Dict],
    planet_orbits: Dict[int, str],
    moon_orbits: Dict[int, str],
    type_names: Dict[int, str],
) -> Tuple[List[Dict], Dict[int, Set[Optional[str]]]]:
    rows: List[Dict] = []
    sys_labels: Dict[int, Set[Optional[str]]] = defaultdict(set)

    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        station_id = int(obj.get("_key"))
        solar_system_id = _get_int(obj, "solarSystemID") or -1
        ss_name = systems.get(solar_system_id, (str(solar_system_id), 0, 0))[0]

        orbit_id = _get_int(obj, "orbitID")
        orbit_name = orbit_names.get(orbit_id, ss_name) if orbit_id is not None else ss_name

        owner_id = _get_int(obj, "ownerID") or -1
        owner = corp_names.get(owner_id, str(owner_id))

        op_id = _get_int(obj, "operationID")
        op_name = ""
        use_op = False
        services: Set[str] = set()
        if op_id is not None and op_id in operations:
            op = operations[op_id]
            op_name = str(op.get("operationName", "")) or str(op_id)
            use_op = bool(op.get("useOperationName", False))
            services = set(op.get("services", set()) or set())

        station_name = f"{orbit_name} - {owner}".strip()
        if use_op and op_name:
            station_name = f"{station_name} {op_name}".strip()

        st_type_id = _get_int(obj, "typeID", "stationTypeID", "stationTypeId")
        station_type = type_names.get(st_type_id, str(st_type_id)) if st_type_id is not None else ""

        docking = "docking" in services
        market = "market" in services
        storage = "storage" in services
        repair = "repair-facilities" in services
        fitting = "fitting" in services
        cloning = "cloning" in services
        jump_clone = "jump-clone-facility" in services

        _lvl, dock_label = station_cyno(station_type if station_type else None, docking)
        sys_labels[solar_system_id].add(dock_label)

        rows.append(
            {
                "_solarSystemID": solar_system_id,
                "stationID": station_id,
                "station": station_name,
                "stationType": station_type,
                "solarSystem": ss_name,
                "owner": owner,
                "cynoJumpSecurity": None,  # fill later from systems
                "cynoDockSecurity": dock_label,
                "docking": docking,
                "market": market,
                "storage": storage,
                "repair": repair,
                "fitting": fitting,
                "cloning": cloning,
                "jump-clone": jump_clone,
            }
        )

    rows.sort(key=lambda r: r["stationID"])
    return rows, dict(sys_labels)


def build_stargates_out(zf: zipfile.ZipFile, systems: Dict[int, Tuple[str, int, int]]) -> List[Dict]:
    rows: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "mapStargates.jsonl"):
        gid = int(obj.get("_key"))
        src_id = int(obj.get("solarSystemID"))
        dest = obj.get("destination") or {}
        dst_id = int(dest.get("solarSystemID"))

        src_name = systems.get(src_id, (str(src_id), 0, 0))[0]
        dst_name = systems.get(dst_id, (str(dst_id), 0, 0))[0]

        stargate_value = f"{src_name} → {dst_name}"

        lo_id, hi_id = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        lo_name = systems.get(lo_id, (str(lo_id), 0, 0))[0]
        hi_name = systems.get(hi_id, (str(hi_id), 0, 0))[0]
        stargate_group = f"{lo_name} ↔ {hi_name}"

        rows.append({"stargateID": gid, "stargate": stargate_value, "stargateGroup": stargate_group, "solarSystem": src_name})

    rows.sort(key=lambda r: r["stargateID"])
    return rows


def build_types_out_sdesi(
    zf: zipfile.ZipFile,
    repo_cache: Dict[int, Dict],
    reset_etags: bool,
) -> List[Dict]:
    """
    Output: sdesi/types.jsonl.gz

    Fields (ordered):
      - typeID
      - type
      - packagedVolume  (default volume SDE; optional ESI override)
      - group
      - category
      - marketGroup
      - is_contraband
      - is_gategank
      - etag            (only when packagedVolume != volume SDE; else null)
    """
    groups_meta = _read_groups_meta(zf)
    categories = _read_categories(zf)
    marketgroup_names = _read_marketgroup_names(zf)
    contraband = _read_contraband_set(zf)

    CANDIDATE_CATEGORIES = {"Ship", "Module", "Celestial"}

    # metrics for log (aggregate)
    m_total = 0
    m_candidates = 0
    m_calls = 0
    m_200 = 0
    m_304 = 0
    m_429 = 0
    m_420 = 0
    m_err = 0
    m_store = 0
    m_clear = 0
    m_discovery = 0
    start = time.time()

    rows: List[Dict] = []

    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not _get_bool(obj, "published", default=False):
            continue

        m_total += 1

        tid = int(obj.get("_key"))
        tname = safe_en_name(obj, fallback=str(tid))

        # SDE volume (base)
        vol_sde = _get_float(obj, "volume")
        if vol_sde is None:
            vol_sde = 0.0  # ensure non-null number<double>

        gid = _get_int(obj, "group_id", "groupID")

        gname = ""
        cname = ""
        if gid is not None and gid in groups_meta:
            gname, cat_id = groups_meta[gid]
            cname = categories.get(cat_id, str(cat_id))
        else:
            gname = str(gid) if gid is not None else ""
            cname = ""

        mgid = _get_int(obj, "marketGroupID", "market_group_id", "marketGroupId")
        mgname = marketgroup_names.get(mgid, str(mgid)) if mgid is not None else ""

        is_candidate = cname in CANDIDATE_CATEGORIES
        if is_candidate:
            m_candidates += 1

        prev = repo_cache.get(tid)
        prev_etag = prev.get("etag") if prev else None
        prev_pv = prev.get("packagedVolume") if prev else None
        is_new = prev is None

        # Default outputs without ESI:
        out_pv = vol_sde
        out_etag: Optional[str] = None

        # --- Mode decision ---
        do_call = False
        use_if_none_match = False

        if reset_etags:
            # RESET MODE: candidates -> call ESI; non-candidates never call ESI and etag forced null
            if is_candidate:
                do_call = True
                use_if_none_match = False  # reset wants a fresh 200 when possible
            else:
                do_call = False
                out_etag = None
                out_pv = vol_sde
        else:
            # NORMAL MODE:
            # 1) If prev etag exists -> refresh it (If-None-Match)
            if prev_etag:
                do_call = True
                use_if_none_match = True
            # 2) discovery: new types in candidate categories (no prev record)
            elif is_new and is_candidate:
                do_call = True
                use_if_none_match = False
                m_discovery += 1
            else:
                do_call = False

        if do_call:
            m_calls += 1
            status, pv_esi, etag_resp, hdrs = fetch_packaged_volume(
                type_id=tid,
                prev_etag=prev_etag,
                use_if_none_match=use_if_none_match,
            )

            if status == 304:
                m_304 += 1
                # Keep previous if present; otherwise fallback SDE
                out_pv = prev_pv if prev_pv is not None else vol_sde
                out_etag = prev_etag  # still non-null
            elif status == 200:
                m_200 += 1
                # ESI packaged_volume may be missing; treat as equals->clear etag
                pv_effective = pv_esi if pv_esi is not None else vol_sde

                if not _vol_equal(pv_effective, vol_sde):
                    out_pv = pv_effective
                    out_etag = etag_resp
                    if out_etag:
                        m_store += 1
                else:
                    out_pv = vol_sde
                    out_etag = None
                    if prev_etag:
                        m_clear += 1
            else:
                # classify a bit
                if status == 429:
                    m_429 += 1
                elif status == 420:
                    m_420 += 1
                elif status != 0:
                    m_err += 1
                else:
                    m_err += 1

                # On error: preserve previous "best known"
                # - If prev had etag (meaning historically differed), keep prev values
                # - else fallback to SDE volume / etag null
                if prev_etag:
                    out_etag = prev_etag
                    out_pv = prev_pv if prev_pv is not None else vol_sde
                else:
                    out_etag = None
                    out_pv = vol_sde

        rows.append(
            {
                "typeID": tid,
                "type": tname,
                "packagedVolume": out_pv,
                "group": gname,
                "category": cname,
                "marketGroup": mgname,
                "is_contraband": tid in contraband,
                "is_gategank": gname == "Smart Bomb",
                "etag": out_etag,
            }
        )

    rows.sort(key=lambda r: r["typeID"])

    elapsed = time.time() - start
    # Aggregate log (legible)
    print(
        "[TYPES:ESI] "
        f"reset_etags={reset_etags} total_published={m_total} candidates={m_candidates} "
        f"calls={m_calls} discovery_new={m_discovery} "
        f"200={m_200} 304={m_304} 429={m_429} 420={m_420} err={m_err} "
        f"etag_store={m_store} etag_clear={m_clear} elapsed_s={elapsed:.1f}"
    )

    return rows


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="Path to CCP SDE JSONL ZIP")
    ap.add_argument("--out", required=True, help="Output directory (will be created)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    reset_etags = os.environ.get("RESET_ETAGS", "").strip().lower() == "true"

    # Load repo cache (previous types) to control ESI calls in NORMAL mode
    repo_types_path = Path("data/sdesi/types.jsonl.gz")
    repo_cache = load_repo_types_cache(repo_types_path)

    with zipfile.ZipFile(args.zip) as zf:
        regions = _read_regions(zf)
        consts = _read_constellations(zf)
        systems = _read_solarsystems(zf)

        # Base geo
        write_jsonl_gz(out_dir / "regions.jsonl.gz", build_regions_out(regions))
        write_jsonl_gz(out_dir / "constellations.jsonl.gz", build_constellations_out(consts, regions))

        # Corporations
        corp_names = _read_corporations(zf)
        write_jsonl_gz(out_dir / "corporations.jsonl.gz", build_corporations_out(corp_names))

        # Stations + cyno
        service_keys = _read_station_services(zf)
        operations = _read_station_operations(zf, service_keys)
        type_names = _read_type_name_map(zf)
        planet_orbits = _read_planet_orbit_names(zf, systems)
        moon_orbits = _read_moon_orbit_names(zf, planet_orbits)

        stations_rows, sys_labels = build_stations_out(
            zf=zf,
            systems=systems,
            corp_names=corp_names,
            operations=operations,
            planet_orbits=planet_orbits,
            moon_orbits=moon_orbits,
            type_names=type_names,
        )

        # system cynoJumpSecurity computed from station dock labels
        system_cyno_jump: Dict[int, str] = {}
        for sid in systems.keys():
            labels = sys_labels.get(sid, set())
            station_count = len([1 for r in stations_rows if r.get("_solarSystemID") == sid])
            system_cyno_jump[sid] = system_cyno(labels, station_count)

        # inject cynoJumpSecurity into stations (copy from solarsystems)
        for r in stations_rows:
            sid = int(r.get("_solarSystemID", -1))
            r["cynoJumpSecurity"] = system_cyno_jump.get(sid, "no jump")
            r.pop("_solarSystemID", None)

        write_jsonl_gz(out_dir / "stations.jsonl.gz", stations_rows)

        # solar systems output (includes cynoJumpSecurity)
        write_jsonl_gz(out_dir / "solarsystems.jsonl.gz", build_solarsystems_out(systems, consts, regions, system_cyno_jump))

        # Stargates
        write_jsonl_gz(out_dir / "stargates.jsonl.gz", build_stargates_out(zf, systems))

        # Types SDE+ESI -> out/sdesi/types.jsonl.gz
        types_rows = build_types_out_sdesi(zf, repo_cache, reset_etags=reset_etags)
        (out_dir / "sdesi").mkdir(parents=True, exist_ok=True)
        write_jsonl_gz(out_dir / "sdesi/types.jsonl.gz", types_rows)

    # Sanity check
    expected = [
        "regions.jsonl.gz",
        "constellations.jsonl.gz",
        "solarsystems.jsonl.gz",
        "stations.jsonl.gz",
        "stargates.jsonl.gz",
        "corporations.jsonl.gz",
        "sdesi/types.jsonl.gz",
    ]
    for name in expected:
        p = out_dir / name
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Missing/empty output: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
