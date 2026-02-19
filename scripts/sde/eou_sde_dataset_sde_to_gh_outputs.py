"""
EOU · SDE Dataset (SDE → GH) — build outputs + packaged + marketTree + excluded

Genera (sobrescribe) en el directorio SDE de salida:

  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz           (incluye cynoJumpSecurity + conteos)
  - stations.jsonl.gz               (incluye cynoJumpSecurity copiado del sistema)
  - stargates.jsonl.gz
  - types.jsonl.gz                  (incluye volume, packaged, marketTree)
  - marketTree.jsonl.gz
  - excludedMarketTypes.jsonl.gz
  - corporations.jsonl.gz
  - marketTree.txt
  - excludeMarketTypes.state.json   (estado sha256 del config)

Y en el directorio ESI de salida:

  - packaged.jsonl.gz               (solo tipos con packaged != volume)

Solo usa:
  - ZIP oficial CCP SDE JSONL
  - ESI universe/types/{type_id} para packagedVolume (solo types nuevos para packaged.jsonl.gz)

Incluye logging de progreso para GitHub Actions.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
import urllib.request
import urllib.error

import zipfile

from eou_sde_dataset_sde_to_gh_io import (
    env_int,
    iter_jsonl_from_zip,
    read_jsonl_gz,
    sha256_file,
    write_jsonl_gz,
    write_text,
)
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name
from eou_sde_dataset_sde_to_gh_cynodock import station_cyno, system_cyno


# -----------------------------
# Logging helpers
# -----------------------------

def log(msg: str) -> None:
    # flush=True para que GH Actions muestre el output al instante
    print(msg, flush=True)


class StepTimer:
    def __init__(self, name: str):
        self.name = name
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        log(f"[START] {self.name}")
        return self

    def __exit__(self, exc_type, exc, tb):
        dt = time.time() - self.t0
        log(f"[DONE ] {self.name} ({dt:.2f}s)")


# -----------------------------
# Rounding / constants
# -----------------------------

_Q6 = Decimal("0.000001")


def round6(x: float) -> float:
    return float(Decimal(str(x)).quantize(_Q6, rounding=ROUND_HALF_UP))


def _sleep_ms(ms: int) -> None:
    time.sleep(max(0, ms) / 1000.0)


# -----------------------------
# HTTP / ESI packagedVolume
# -----------------------------

@dataclass(frozen=True)
class EsiTypeVolumes:
    volume: float
    packaged: float


def esi_get_universe_type(type_id: int, *, min_delay_ms: int, max_retries: int) -> EsiTypeVolumes:
    """
    GET https://esi.evetech.net/universe/types/{type_id}/

    Política:
    - 200: ok
    - 404: no existe → no insertar
    - 420: error-limit → dormir X-ESI-Error-Limit-Reset + 1 y reintentar
    - 429: rate limit → dormir Retry-After y reintentar
    - 5xx/timeout: backoff exponencial con jitter
    """
    url = f"https://esi.evetech.net/latest/universe/types/{type_id}/?datasource=tranquility&language=en"
    backoff = 0.5

    for attempt in range(1, max_retries + 1):
        _sleep_ms(min_delay_ms)

        req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = resp.status
                headers = {k.lower(): v for k, v in resp.headers.items()}
                body = resp.read().decode("utf-8", errors="replace")

            if status == 200:
                obj = json.loads(body)
                vol = float(obj.get("volume", 0.0))
                # ESI usa packaged_volume (snake) en muchas respuestas
                pvol = float(obj.get("packaged_volume", obj.get("packagedVolume", vol)))
                return EsiTypeVolumes(volume=vol, packaged=pvol)

            if status == 404:
                raise FileNotFoundError(f"ESI type {type_id} not found (404)")

            raise RuntimeError(f"Unexpected HTTP {status} for type {type_id}")

        except urllib.error.HTTPError as e:
            status = getattr(e, "code", None)
            hdrs = {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}

            if status == 404:
                raise FileNotFoundError(f"ESI type {type_id} not found (404)")

            if status == 420:
                reset = hdrs.get("x-esi-error-limit-reset")
                try:
                    sec = int(str(reset).strip()) if reset is not None else 60
                except Exception:
                    sec = 60
                time.sleep(sec + 1)
                continue

            if status == 429:
                ra = hdrs.get("retry-after")
                try:
                    sec = int(str(ra).strip()) if ra is not None else 5
                except Exception:
                    sec = 5
                time.sleep(sec + 1)
                continue

            jitter = random.random() * 0.25
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2.0, 20.0)
            continue

        except urllib.error.URLError:
            jitter = random.random() * 0.25
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2.0, 20.0)
            continue

    raise RuntimeError(f"ESI failed for type {type_id} after {max_retries} retries")


# -----------------------------
# SDE readers
# -----------------------------

def _read_regions(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapRegions.jsonl"):
        rid = int(obj["_key"])
        out[rid] = safe_en_name(obj, fallback=str(rid))
    return out


def _read_constellations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapConstellations.jsonl"):
        cid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(cid))
        rid = int(obj["regionID"])
        out[cid] = (name, rid)
    return out


def _read_factions(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "factions.jsonl"):
        fid = int(obj["_key"])
        out[fid] = safe_en_name(obj, fallback=str(fid))
    return out


def _read_corporations(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "npcCorporations.jsonl"):
        cid = int(obj["_key"])
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def _read_market_groups(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, Optional[int]]]:
    """
    marketGroupID -> (name_en, parentGroupID)
    """
    out: Dict[int, Tuple[str, Optional[int]]] = {}
    for obj in iter_jsonl_from_zip(zf, "marketGroups.jsonl"):
        mgid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(mgid))
        parent = obj.get("parentGroupID")
        parent_id = int(parent) if parent is not None else None
        out[mgid] = (name, parent_id)
    return out


def _read_groups_meta(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "groups.jsonl"):
        gid = int(obj["_key"])
        gname = safe_en_name(obj, fallback=str(gid))
        cat_id = int(obj["categoryID"])
        out[gid] = (gname, cat_id)
    return out


def _read_categories(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "categories.jsonl"):
        cid = int(obj["_key"])
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def _read_contraband_set(zf: zipfile.ZipFile) -> Set[int]:
    s: Set[int] = set()
    for obj in iter_jsonl_from_zip(zf, "contrabandTypes.jsonl"):
        s.add(int(obj["_key"]))
    return s


def _read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Dict]:
    systems: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(sid))
        cid = int(obj["constellationID"])
        rid = int(obj["regionID"])

        pos = obj.get("position") or {}
        position = [float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0))]

        sec = float(obj.get("securityStatus", 0.0))
        faction_id = obj.get("factionID")
        faction_id_int = int(faction_id) if faction_id is not None else None

        planet_ids = obj.get("planetIDs")
        planets = len(planet_ids) if isinstance(planet_ids, list) else 0

        stargate_ids = obj.get("stargateIDs")
        stargates = len(stargate_ids) if isinstance(stargate_ids, list) else 0

        systems[sid] = {
            "name": name,
            "constellationID": cid,
            "regionID": rid,
            "position": position,
            "securityStatus": sec,
            "factionID": faction_id_int,
            "planets": planets,
            "stargates": stargates,
        }
    return systems


def _read_planet_orbit_names(zf: zipfile.ZipFile, systems: Dict[int, Dict]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        pid = int(obj["_key"])
        sid = int(obj["solarSystemID"])
        ss = systems.get(sid, {"name": str(sid)})["name"]
        cidx = int(obj.get("celestialIndex", 0))
        out[pid] = planet_name(ss, cidx)
    return out


def _read_moon_orbit_names(zf: zipfile.ZipFile, planet_orbits: Dict[int, str]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        mid = int(obj["_key"])
        pid = int(obj["orbitID"])
        p_orbit = planet_orbits.get(pid, str(pid))
        oidx = int(obj.get("orbitIndex", 0))
        out[mid] = moon_name(p_orbit, oidx)
    return out


def _count_moons_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    c: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        sid = int(obj["solarSystemID"])
        c[sid] += 1
    return dict(c)


def _count_asteroid_belts_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    c: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        sid = int(obj["solarSystemID"])
        belts = obj.get("asteroidBeltIDs")
        if isinstance(belts, list):
            c[sid] += len(belts)
    return dict(c)


def _read_station_services(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}

    def normalize(s: str) -> str:
        x = s.strip().lower()
        x = re.sub(r"\s+", "-", x)
        x = x.replace("_", "-")
        x = re.sub(r"[^a-z0-9\-]", "", x)
        x = re.sub(r"-{2,}", "-", x).strip("-")
        return x

    CANON = {
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
        sid = int(obj["_key"])
        sn = obj.get("serviceName") or {}
        en = sn.get("en") if isinstance(sn, dict) else None
        en = en if isinstance(en, str) else str(sid)
        key = CANON.get(normalize(en), normalize(en))
        out[sid] = key

    return out


def _read_station_operations(zf: zipfile.ZipFile, service_keys: Dict[int, str]) -> Dict[int, Dict]:
    ops: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "stationOperations.jsonl"):
        oid = int(obj["_key"])
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
                k = service_keys.get(sid_int)
                if k:
                    svc_set.add(k)

        ops[oid] = {"operationName": name, "useOperationName": use_op, "services": svc_set}
    return ops


def _read_types_base(zf: zipfile.ZipFile) -> Dict[int, Dict]:
    """
    published-only:
      typeID -> {type, volume, group_id, marketGroupID}
    """
    out: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not bool(obj.get("published", False)):
            continue
        tid = int(obj["_key"])
        tname = safe_en_name(obj, fallback=str(tid))
        volume = float(obj.get("volume", 0.0))
        gid = obj.get("group_id") if "group_id" in obj else obj.get("groupID")
        gid = int(gid) if gid is not None else None
        mgid = obj.get("marketGroupID") if "marketGroupID" in obj else obj.get("market_group_id")
        mgid = int(mgid) if mgid is not None else None
        out[tid] = {"type": tname, "volume": volume, "group_id": gid, "marketGroupID": mgid}
    return out


# -----------------------------
# Builders
# -----------------------------

def build_regions_out(regions: Dict[int, str]) -> List[Dict]:
    rows = [{"regionID": rid, "region": nm} for rid, nm in regions.items()]
    rows.sort(key=lambda r: r["regionID"])
    return rows


def build_constellations_out(consts: Dict[int, Tuple[str, int]], regions: Dict[int, str]) -> List[Dict]:
    rows: List[Dict] = []
    for cid, (cname, rid) in consts.items():
        rows.append({"constellationID": cid, "constellation": cname, "region": regions.get(rid, str(rid))})
    rows.sort(key=lambda r: r["constellationID"])
    return rows


def build_corporations_out(corps: Dict[int, str]) -> List[Dict]:
    rows = [{"corporationID": cid, "corporation": nm} for cid, nm in corps.items()]
    rows.sort(key=lambda r: r["corporationID"])
    return rows


def build_stargates_out(zf: zipfile.ZipFile, systems: Dict[int, Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "mapStargates.jsonl"):
        gid = int(obj["_key"])
        src_id = int(obj["solarSystemID"])
        dst = obj.get("destination") or {}
        dst_id = int(dst["solarSystemID"])

        src = systems.get(src_id, {"name": str(src_id)})["name"]
        dstn = systems.get(dst_id, {"name": str(dst_id)})["name"]

        lo_id, hi_id = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        lo = systems.get(lo_id, {"name": str(lo_id)})["name"]
        hi = systems.get(hi_id, {"name": str(hi_id)})["name"]

        rows.append(
            {
                "stargateID": gid,
                "stargate": f"{src} → {dstn}",
                "stargateGroup": f"{lo} ↔ {hi}",
                "solarSystem": src,
            }
        )
    rows.sort(key=lambda r: r["stargateID"])
    return rows


def build_stations_out(
    zf: zipfile.ZipFile,
    systems: Dict[int, Dict],
    corp_names: Dict[int, str],
    operations: Dict[int, Dict],
    planet_orbits: Dict[int, str],
    moon_orbits: Dict[int, str],
    type_names: Dict[int, str],
) -> Tuple[List[Dict], Dict[int, Set[Optional[str]]], Dict[int, int]]:
    rows: List[Dict] = []
    system_labels: Dict[int, Set[Optional[str]]] = defaultdict(set)
    station_counts: Dict[int, int] = defaultdict(int)

    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    n = 0
    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        n += 1
        if n % 5000 == 0:
            log(f"Stations parsed: {n}")

        station_id = int(obj["_key"])
        sid = int(obj.get("solarSystemID", -1))
        station_counts[sid] += 1

        ss_name = systems.get(sid, {"name": str(sid)})["name"]
        orbit_id = obj.get("orbitID")
        orbit_name = orbit_names.get(int(orbit_id), ss_name) if orbit_id is not None else ss_name

        owner_id = int(obj.get("ownerID", -1))
        owner = corp_names.get(owner_id, str(owner_id))

        op_id = obj.get("operationID")
        services: Set[str] = set()
        use_op = False
        op_name = ""
        if op_id is not None and int(op_id) in operations:
            op = operations[int(op_id)]
            services = set(op.get("services", set()) or set())
            use_op = bool(op.get("useOperationName", False))
            op_name = str(op.get("operationName", "")) or str(op_id)

        station_name = f"{orbit_name} - {owner}".strip()
        if use_op and op_name:
            station_name = f"{station_name} {op_name}".strip()

        st_type_id = obj.get("typeID") or obj.get("stationTypeID") or obj.get("stationTypeId")
        st_type_id_int = int(st_type_id) if st_type_id is not None else None
        station_type = type_names.get(st_type_id_int, str(st_type_id_int)) if st_type_id_int is not None else ""

        docking = "docking" in services
        market = "market" in services
        storage = "storage" in services
        repair = "repair-facilities" in services
        fitting = "fitting" in services
        cloning = "cloning" in services
        jump_clone = "jump-clone-facility" in services

        _lvl, dock_label = station_cyno(station_type if station_type else None, docking)
        system_labels[sid].add(dock_label)

        rows.append(
            {
                "_solarSystemID": sid,  # internal key for later injection
                "stationID": station_id,
                "station": station_name,
                "stationType": station_type,
                "solarSystem": ss_name,
                "owner": owner,
                # cynoJumpSecurity injected later
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
    return rows, dict(system_labels), dict(station_counts)


def build_solarsystems_out(
    systems: Dict[int, Dict],
    consts: Dict[int, Tuple[str, int]],
    regions: Dict[int, str],
    factions: Dict[int, str],
    moons_count: Dict[int, int],
    belts_count: Dict[int, int],
    stations_count: Dict[int, int],
    system_cyno_jump: Dict[int, str],
) -> List[Dict]:
    rows: List[Dict] = []
    for sid, s in systems.items():
        cid = int(s["constellationID"])
        rid = int(s["regionID"])
        cname = consts.get(cid, (str(cid), 0))[0]
        rname = regions.get(rid, str(rid))

        fid = s.get("factionID")
        faction = factions.get(int(fid)) if fid is not None and int(fid) in factions else None

        rows.append(
            {
                "solarSystemID": sid,
                "solarSystem": s["name"],
                "constellation": cname,
                "region": rname,
                "position": s.get("position", [0.0, 0.0, 0.0]),
                "faction": faction,
                "securityStatus": round6(float(s.get("securityStatus", 0.0))),
                "planets": int(s.get("planets", 0)),
                "moons": int(moons_count.get(sid, 0)),
                "asteroid_belts": int(belts_count.get(sid, 0)),
                "stargates": int(s.get("stargates", 0)),
                "stations": int(stations_count.get(sid, 0)),
                "cynoJumpSecurity": system_cyno_jump.get(sid, "no jump"),
            }
        )
    rows.sort(key=lambda r: r["solarSystemID"])
    return rows


def market_tree_string(mgid: Optional[int], mg_map: Dict[int, Tuple[str, Optional[int]]]) -> Optional[str]:
    if mgid is None:
        return None
    if mgid not in mg_map:
        return None
    parts: List[str] = []
    cur: Optional[int] = mgid
    seen: Set[int] = set()
    while cur is not None and cur in mg_map and cur not in seen:
        seen.add(cur)
        name, parent = mg_map[cur]
        parts.append(name)
        cur = parent
    parts.reverse()
    return "Market → " + " → ".join(parts)


def build_types_out(
    types_base: Dict[int, Dict],
    packaged_map: Dict[int, float],
    groups_meta: Dict[int, Tuple[str, int]],
    categories: Dict[int, str],
    market_groups: Dict[int, Tuple[str, Optional[int]]],
    contraband: Set[int],
) -> List[Dict]:
    rows: List[Dict] = []
    for tid, t in types_base.items():
        volume = float(t["volume"])
        packaged = float(packaged_map.get(tid, volume))
        gid = t.get("group_id")
        gname = ""
        cname = ""
        if gid is not None and int(gid) in groups_meta:
            gname, cat_id = groups_meta[int(gid)]
            cname = categories.get(cat_id, str(cat_id))
        else:
            gname = str(gid) if gid is not None else ""
            cname = ""

        mgid = t.get("marketGroupID")
        mg_name = ""
        if mgid is not None and int(mgid) in market_groups:
            mg_name = market_groups[int(mgid)][0]

        mtree = market_tree_string(int(mgid) if mgid is not None else None, market_groups)

        rows.append(
            {
                "typeID": tid,
                "type": t["type"],
                "volume": volume,
                "packaged": packaged,
                "group": gname,
                "category": cname,
                "marketGroup": mg_name,
                "marketTree": mtree,
                "is_contraband": tid in contraband,
                "is_gategank": gname == "Smart Bomb",
            }
        )
    rows.sort(key=lambda r: r["typeID"])
    return rows


def build_market_tree_jsonl(types_rows: List[Dict]) -> List[Dict]:
    buckets: Dict[str, Set[str]] = defaultdict(set)
    for r in types_rows:
        mt = r.get("marketTree")
        if not isinstance(mt, str) or not mt:
            continue
        buckets[mt].add(str(r.get("type", "")))

    out: List[Dict] = []
    for mt, typeset in buckets.items():
        out.append({"marketTree": mt, "types": sorted(typeset)})
    out.sort(key=lambda r: r["marketTree"])
    return out


def build_market_tree_txt(
    market_groups: Dict[int, Tuple[str, Optional[int]]],
    types_base: Dict[int, Dict],
) -> str:
    children: Dict[Optional[int], List[int]] = defaultdict(list)
    for mgid, (_name, parent) in market_groups.items():
        children[parent].append(mgid)
    for k in list(children.keys()):
        children[k].sort(key=lambda mgid: market_groups[mgid][0].lower())

    group_types: Dict[int, List[str]] = defaultdict(list)
    for _tid, t in types_base.items():
        mgid = t.get("marketGroupID")
        if mgid is None:
            continue
        if int(mgid) in market_groups:
            group_types[int(mgid)].append(str(t.get("type", "")))
    for mgid in list(group_types.keys()):
        group_types[mgid].sort(key=lambda x: x.lower())

    roots = children.get(None, [])

    def icon_for_type_name(_name: str) -> str:
        n = _name.lower()
        if "blueprint" in n:
            return "📜"
        if "drone" in n:
            return "🐝"
        if "charge" in n or "ammo" in n or "missile" in n:
            return "☄️"
        if "skill" in n:
            return "🎓"
        if "implant" in n or "booster" in n:
            return "🧠"
        if "ship" in n:
            return "🚢"
        return "📦"

    lines: List[str] = []
    lines.append("Market")

    def walk_group(mgid: int, prefix: str, is_last: bool) -> None:
        name = market_groups[mgid][0]
        branch = "└── " if is_last else "├── "
        lines.append(f"{prefix}{branch}📁 {name}")

        new_prefix = prefix + ("    " if is_last else "│   ")
        kids = children.get(mgid, [])
        tps = group_types.get(mgid, [])

        for i, child in enumerate(kids):
            walk_group(child, new_prefix, i == len(kids) - 1 and len(tps) == 0)

        if tps:
            for j, tn in enumerate(tps):
                b = "└── " if j == len(tps) - 1 else "├── "
                lines.append(f"{new_prefix}{b}{icon_for_type_name(tn)} {tn}")

    for i, root in enumerate(roots):
        walk_group(root, "", i == len(roots) - 1)

    lines.append("")
    lines.append("Leyenda (puede ajustarse):")
    lines.append("Munición y Cargas: ☄️ | Drones: 🐝 | Planos: 📜 | Skills: 🎓 | Implantes/Boosters: 🧠 | Naves: 🚢 | Default: 📦")
    lines.append("")
    return "\n".join(lines)


def build_excluded_market_types(
    exclude_config_path: Path,
    types_rows: List[Dict],
    market_tree_rows: List[Dict],
) -> List[Dict]:
    type_to_tree: Dict[str, Optional[str]] = {}
    for r in types_rows:
        tname = str(r.get("type", ""))
        type_to_tree[tname] = r.get("marketTree")

    tree_to_types: Dict[str, List[str]] = {}
    for r in market_tree_rows:
        mt = str(r.get("marketTree", ""))
        tps = r.get("types") or []
        if isinstance(tps, list):
            tree_to_types[mt] = [str(x) for x in tps]

    out: List[Dict] = []
    if not exclude_config_path.exists():
        return out

    for raw in exclude_config_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.startswith("Market → "):
            if line in tree_to_types:
                for tname in tree_to_types[line]:
                    mt = type_to_tree.get(tname)
                    out.append({"excludedType": tname, "marketTree": mt})
        else:
            if line in type_to_tree:
                out.append({"excludedType": line, "marketTree": type_to_tree.get(line)})

    uniq = {(r["excludedType"], r.get("marketTree")): r for r in out}
    out2 = list(uniq.values())
    out2.sort(key=lambda r: (str(r.get("excludedType", "")).lower(), str(r.get("marketTree", "") or "").lower()))
    return out2


# -----------------------------
# packaged.jsonl.gz logic
# -----------------------------

def load_existing_packaged(existing_path: Path) -> Dict[int, float]:
    """
    Reads data/esi/packaged.jsonl.gz in a tolerant way.

    Accepts keys:
      - packaged
      - packagedVolume
      - packaged_volume
    """
    rows = read_jsonl_gz(existing_path)
    out: Dict[int, float] = {}
    for r in rows:
        try:
            tid = int(r.get("typeID"))
        except Exception:
            continue

        pv = r.get("packaged")
        if pv is None:
            pv = r.get("packagedVolume")
        if pv is None:
            pv = r.get("packaged_volume")

        if pv is None:
            continue

        try:
            out[tid] = float(pv)
        except Exception:
            continue
    return out


def build_packaged_updated(
    *,
    types_base: Dict[int, Dict],
    existing_packaged: Dict[int, float],
    min_delay_ms: int,
    max_retries: int,
    log_every: int = 250,
) -> Dict[int, float]:
    """
    Incremental updater:
      - prune ids not in current published types
      - fetch ESI for new types only
      - store only if packaged != volume
    """
    valid_ids = set(types_base.keys())

    # prune (Nota 2)
    existing_packaged = {tid: pv for tid, pv in existing_packaged.items() if tid in valid_ids}

    # missing are types never evaluated before
    missing = sorted([tid for tid in valid_ids if tid not in existing_packaged])

    log(f"Packaged: existing entries: {len(existing_packaged)}")
    log(f"Packaged: new types requiring ESI check: {len(missing)}")
    log(f"Packaged: min_delay_ms={min_delay_ms}, max_retries={max_retries}")

    start = time.time()
    esi_calls = 0
    kept = 0

    for idx, tid in enumerate(missing, 1):
        try:
            vols = esi_get_universe_type(tid, min_delay_ms=min_delay_ms, max_retries=max_retries)
        except FileNotFoundError:
            continue

        esi_calls += 1

        sde_vol = float(types_base[tid]["volume"])
        pvol = float(vols.packaged)

        if abs(pvol - sde_vol) > 0.0:
            existing_packaged[tid] = pvol
            kept += 1

        if log_every > 0 and idx % log_every == 0:
            elapsed = time.time() - start
            rate = idx / elapsed if elapsed > 0 else 0.0
            eta = (len(missing) - idx) / rate if rate > 0 else float("inf")
            log(
                f"ESI progress: {idx}/{len(missing)} | "
                f"{rate:.2f} req/s | kept={kept} | eta~{eta/60:.1f} min"
            )

    elapsed_total = time.time() - start
    log(f"ESI finished. Calls made: {esi_calls}. Kept packaged!=volume: {kept}. Time: {elapsed_total:.1f}s")

    return existing_packaged


def packaged_rows_for_write(types_base: Dict[int, Dict], packaged_map: Dict[int, float]) -> List[Dict]:
    rows: List[Dict] = []
    for tid, pv in packaged_map.items():
        tname = types_base.get(tid, {}).get("type", str(tid))
        rows.append({"typeID": tid, "type": tname, "packaged": float(pv)})
    # Nota 3: orden descendente
    rows.sort(key=lambda r: r["typeID"], reverse=True)
    return rows


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="Path to CCP SDE JSONL ZIP")
    ap.add_argument("--out-sde", required=True, help="Output dir for SDE (jsonl.gz + txt)")
    ap.add_argument("--out-esi", required=True, help="Output dir for ESI (packaged.jsonl.gz)")
    ap.add_argument("--existing-packaged", required=True, help="Existing data/esi/packaged.jsonl.gz (may not exist)")
    ap.add_argument("--exclude-config", required=True, help="config/excludeMarketTypes.txt")
    ap.add_argument("--exclude-state", required=True, help="states/excludeMarketTypes.json (may not exist)")
    args = ap.parse_args()

    out_sde = Path(args.out_sde)
    out_esi = Path(args.out_esi)
    out_sde.mkdir(parents=True, exist_ok=True)
    out_esi.mkdir(parents=True, exist_ok=True)

    existing_packaged_path = Path(args.existing_packaged)
    exclude_config = Path(args.exclude_config)
    exclude_state = Path(args.exclude_state)

    min_delay_ms = env_int("ESI_MIN_DELAY_MS", 300)
    max_retries = env_int("ESI_MAX_RETRIES", 8)
    log_every = env_int("ESI_LOG_EVERY", 250)

    t0 = time.time()
    log("=== BUILD outputs (SDE + packaged + marketTree + excluded) START ===")
    log(f"Inputs: zip={args.zip}")
    log(f"Outputs: out_sde={out_sde}, out_esi={out_esi}")
    log(f"Existing packaged path: {existing_packaged_path}")
    log(f"Exclude config: {exclude_config} | state: {exclude_state}")

    with StepTimer("Open ZIP"):
        zf = zipfile.ZipFile(args.zip)

    try:
        with zf:
            # ---- base geo / corp / factions ----
            with StepTimer("Read base SDE maps (regions/constellations/factions/corps/solarsystems)"):
                regions = _read_regions(zf)
                consts = _read_constellations(zf)
                factions = _read_factions(zf)
                corp_names = _read_corporations(zf)
                systems = _read_solarsystems(zf)

            log(f"Regions: {len(regions)} | Constellations: {len(consts)} | Systems: {len(systems)} | Corps: {len(corp_names)}")

            # ---- station operations/services ----
            with StepTimer("Read station services + operations"):
                service_keys = _read_station_services(zf)
                operations = _read_station_operations(zf, service_keys)

            log(f"Station services: {len(service_keys)} | Station operations: {len(operations)}")

            # ---- type name map for stationType ----
            with StepTimer("Build type-name map (for stationType)"):
                type_names: Dict[int, str] = {}
                n_types_total = 0
                for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
                    n_types_total += 1
                    tid = int(obj["_key"])
                    type_names[tid] = safe_en_name(obj, fallback=str(tid))
                    if n_types_total % 50000 == 0:
                        log(f"types.jsonl scanned for names: {n_types_total}")

            log(f"type_names loaded: {len(type_names)} (types.jsonl scanned: {n_types_total})")

            # ---- stations + cynoDock ----
            with StepTimer("Read planet/moon orbits"):
                planet_orbits = _read_planet_orbit_names(zf, systems)
                moon_orbits = _read_moon_orbit_names(zf, planet_orbits)

            with StepTimer("Build stations (incl. cynoDockSecurity + services)"):
                stations_rows, system_station_labels, stations_count = build_stations_out(
                    zf=zf,
                    systems=systems,
                    corp_names=corp_names,
                    operations=operations,
                    planet_orbits=planet_orbits,
                    moon_orbits=moon_orbits,
                    type_names=type_names,
                )

            log(f"Stations built: {len(stations_rows)} | Systems with stations: {len(stations_count)}")

            # ---- system cynoJump + inject into stations ----
            with StepTimer("Compute system cynoJumpSecurity + inject into stations"):
                system_cyno_jump: Dict[int, str] = {}
                for sid in systems.keys():
                    labels = system_station_labels.get(sid, set())
                    scount = int(stations_count.get(sid, 0))
                    system_cyno_jump[sid] = system_cyno(labels, scount)

                for r in stations_rows:
                    sid = int(r.pop("_solarSystemID", -1))
                    r["cynoJumpSecurity"] = system_cyno_jump.get(sid, "no jump")

            # ---- solarsystems counts ----
            with StepTimer("Count moons + asteroid belts"):
                moons_count = _count_moons_by_system(zf)
                belts_count = _count_asteroid_belts_by_system(zf)

            with StepTimer("Build solarsystems (incl. cynoJumpSecurity)"):
                sol_rows = build_solarsystems_out(
                    systems=systems,
                    consts=consts,
                    regions=regions,
                    factions=factions,
                    moons_count=moons_count,
                    belts_count=belts_count,
                    stations_count=stations_count,
                    system_cyno_jump=system_cyno_jump,
                )

            # ---- types base ----
            with StepTimer("Read types_base (published-only)"):
                types_base = _read_types_base(zf)

            log(f"Published types: {len(types_base)}")

            # ---- packaged incremental (ESI) ----
            with StepTimer("Load existing packaged map"):
                existing_packaged = load_existing_packaged(existing_packaged_path)
            log(f"Existing packaged map loaded: {len(existing_packaged)}")

            with StepTimer("Update packaged via ESI (incremental)"):
                packaged_map = build_packaged_updated(
                    types_base=types_base,
                    existing_packaged=existing_packaged,
                    min_delay_ms=min_delay_ms,
                    max_retries=max_retries,
                    log_every=log_every,
                )

            with StepTimer("Write packaged.jsonl.gz"):
                write_jsonl_gz(out_esi / "packaged.jsonl.gz", packaged_rows_for_write(types_base, packaged_map))

            log(f"Packaged final entries: {len(packaged_map)}")

            # ---- types final + marketTree ----
            with StepTimer("Read marketGroups + groups + categories + contraband"):
                market_groups = _read_market_groups(zf)
                groups_meta = _read_groups_meta(zf)
                categories = _read_categories(zf)
                contraband = _read_contraband_set(zf)

            with StepTimer("Build types (volume + packaged + marketTree)"):
                types_rows = build_types_out(
                    types_base=types_base,
                    packaged_map=packaged_map,
                    groups_meta=groups_meta,
                    categories=categories,
                    market_groups=market_groups,
                    contraband=contraband,
                )
            log(f"Types built: {len(types_rows)}")

            with StepTimer("Build marketTree.jsonl + marketTree.txt"):
                market_tree_rows = build_market_tree_jsonl(types_rows)
                market_tree_txt = build_market_tree_txt(market_groups, types_base)

            log(f"MarketTree rows: {len(market_tree_rows)}")

            # ---- exclusions ----
            with StepTimer("Compute exclude config etag + rebuild excludedMarketTypes"):
                cfg_etag = sha256_file(exclude_config) if exclude_config.exists() else ""
                prev_etag = ""
                if exclude_state.exists():
                    try:
                        prev_etag = json.loads(exclude_state.read_text(encoding="utf-8")).get("etag", "") or ""
                    except Exception:
                        prev_etag = ""

                excluded_rows = build_excluded_market_types(exclude_config, types_rows, market_tree_rows)
                state_payload = {"etag": cfg_etag}

                # Siempre reescribimos excludedMarketTypes determinista (es barato) y emitimos state
                write_text(out_sde / "excludeMarketTypes.state.json", json.dumps(state_payload, ensure_ascii=False, indent=2) + "\n")

            log(f"Excluded rows: {len(excluded_rows)} | exclude_etag_changed={cfg_etag != prev_etag}")

            # ---- write SDE outputs ----
            with StepTimer("Write SDE outputs (jsonl.gz + txt)"):
                write_jsonl_gz(out_sde / "regions.jsonl.gz", build_regions_out(regions))
                write_jsonl_gz(out_sde / "constellations.jsonl.gz", build_constellations_out(consts, regions))
                write_jsonl_gz(out_sde / "solarsystems.jsonl.gz", sol_rows)
                write_jsonl_gz(out_sde / "stations.jsonl.gz", stations_rows)
                write_jsonl_gz(out_sde / "stargates.jsonl.gz", build_stargates_out(zf, systems))
                write_jsonl_gz(out_sde / "types.jsonl.gz", types_rows)
                write_jsonl_gz(out_sde / "corporations.jsonl.gz", build_corporations_out(corp_names))
                write_jsonl_gz(out_sde / "marketTree.jsonl.gz", market_tree_rows)
                write_text(out_sde / "marketTree.txt", market_tree_txt)
                write_jsonl_gz(out_sde / "excludedMarketTypes.jsonl.gz", excluded_rows)

    except Exception as e:
        log(f"!!! BUILD FAILED: {type(e).__name__}: {e}")
        raise

    # ---- sanity checks ----
    with StepTimer("Sanity checks"):
        expected_sde = [
            "regions.jsonl.gz",
            "constellations.jsonl.gz",
            "solarsystems.jsonl.gz",
            "stations.jsonl.gz",
            "stargates.jsonl.gz",
            "types.jsonl.gz",
            "corporations.jsonl.gz",
            "marketTree.jsonl.gz",
            "excludedMarketTypes.jsonl.gz",
            "marketTree.txt",
            "excludeMarketTypes.state.json",
        ]
        for name in expected_sde:
            p = out_sde / name
            if not p.exists() or p.stat().st_size == 0:
                raise RuntimeError(f"Missing/empty output: {p}")

        p = out_esi / "packaged.jsonl.gz"
        if not p.exists():
            raise RuntimeError(f"Missing output: {p}")

    total = time.time() - t0
    log(f"=== BUILD COMPLETE ({total/60:.2f} min) ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
