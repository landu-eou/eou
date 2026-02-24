"""
EOU · SDE Dataset (SDE → GH) — build outputs (SDE + packaged + marketTree + excluded)

Outputs (out_sde):
  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz
  - stations.jsonl.gz
  - stargates.jsonl.gz
  - types.jsonl.gz
  - corporations.jsonl.gz
  - marketTree.jsonl.gz
  - excludedMarketTypes.jsonl.gz
  - marketTree.txt
  - excludeMarketTypes.state.json

Outputs (out_esi):
  - packaged.jsonl.gz   (exceptions only; packaged != volume)

Key packaged strategy:
  - baseline types1 = committed data/sde/types.jsonl.gz
  - new types2 = SDE published types from ZIP
  - types3 = types2 - types1  (ONLY these call ESI)

Exclude rules:
  - "Market → ...": exact match only
  - "Market → ... → ...": exact + any descendant trees prefixed by "Market → ..."

types.jsonl.gz:
  - marketGroup must be null if no marketGroup (not "")

marketTree.txt icons:
  - folders: 📁
  - types: ☠️ if contraband else 📦
  - no other icons

excludedMarketTypes.jsonl.gz (UPDATED):
  - {"typeID":..., "type":..., "marketTree":...}

stargates.jsonl.gz (UPDATED):
  - adds "stargateDistance": null | list[{"stargate": "...", "distanceAU": double}]
    * computed against other stargates in same solar system (excluding itself)
    * if only one stargate in the system => null
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import math
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

from eou_sde_dataset_sde_to_gh_io import (  # noqa: E402
    env_int,
    iter_jsonl_from_zip,
    read_jsonl_gz,
    sha256_file,
    write_jsonl_gz,
    write_text,
)
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name  # noqa: E402
from eou_sde_dataset_sde_to_gh_cynodock import station_cyno, system_cyno  # noqa: E402

AU_METERS = 149_597_870_700.0


# -----------------------------
# small helpers
# -----------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def _get_int(obj: Dict, *keys: str) -> Optional[int]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return int(obj[k])
            except Exception:
                return None
    return None


def _get_float(obj: Dict, *keys: str) -> Optional[float]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return float(obj[k])
            except Exception:
                return None
    return None


def _get_bool(obj: Dict, *keys: str, default: bool = False) -> bool:
    for k in keys:
        if k in obj:
            return bool(obj[k])
    return default


def _sleep_ms(ms: int) -> None:
    time.sleep(max(0, ms) / 1000.0)


# -----------------------------
# baseline (types1) typeIDs
# -----------------------------

def load_baseline_typeids(baseline_types_path: str) -> Set[int]:
    p = Path(baseline_types_path)
    if not baseline_types_path or not p.exists():
        return set()
    rows = read_jsonl_gz(p)
    out: Set[int] = set()
    for r in rows:
        tid = _get_int(r, "typeID")
        if tid is not None:
            out.add(tid)
    return out


# -----------------------------
# packaged exceptions (repo file)
# -----------------------------

def load_existing_packaged(existing_path: str) -> Dict[int, Dict]:
    p = Path(existing_path)
    if not existing_path or not p.exists():
        return {}
    rows = read_jsonl_gz(p)
    out: Dict[int, Dict] = {}
    for r in rows:
        tid = _get_int(r, "typeID")
        if tid is None:
            continue
        pv = r.get("packaged")
        if pv is None:
            pv = r.get("packagedVolume")
        if pv is None:
            pv = r.get("packaged_volume")
        try:
            pv_f = float(pv)
        except Exception:
            continue
        out[tid] = {"typeID": tid, "type": str(r.get("type") or ""), "packaged": pv_f}
    return out


def write_packaged_exceptions(path: Path, packaged_map: Dict[int, Dict]) -> None:
    rows = list(packaged_map.values())
    rows.sort(key=lambda r: int(r["typeID"]), reverse=True)
    write_jsonl_gz(path, rows)


# -----------------------------
# ESI fetch for packagedVolume (only for types3)
# -----------------------------

def esi_get_type(type_id: int) -> Tuple[int, Optional[Dict]]:
    url = f"https://esi.evetech.net/latest/universe/types/{type_id}/?datasource=tranquility&language=en"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "EOU-packaged/1.0"})
    try:
        with urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read().decode("utf-8", errors="replace")
            return status, json.loads(data)
    except HTTPError as e:
        return int(e.code), None
    except URLError:
        return 0, None


def refresh_packaged_for_new_types(
    *,
    new_type_ids: List[int],
    type_name_by_id: Dict[int, str],
    sde_volume_by_id: Dict[int, float],
    packaged_map: Dict[int, Dict],
) -> None:
    """
    Update packaged_map in-place for ONLY new types (types3).
    Store exception ONLY when packaged_volume != volume.
    """
    if not new_type_ids:
        log("[PACKAGED] No new types to evaluate (types3 empty).")
        return

    min_delay_ms = env_int("ESI_MIN_DELAY_MS", 300)
    max_retries = env_int("ESI_MAX_RETRIES", 8)
    log_every = env_int("ESI_LOG_EVERY", 50)

    log(f"[PACKAGED] Evaluating {len(new_type_ids)} new typeIDs via ESI (types3).")

    for idx, tid in enumerate(new_type_ids, start=1):
        vol = float(sde_volume_by_id.get(tid, 0.0))

        attempt = 0
        while True:
            attempt += 1
            _sleep_ms(min_delay_ms)
            status, payload = esi_get_type(tid)

            if status == 200 and isinstance(payload, dict):
                pkg = payload.get("packaged_volume")
                if pkg is None:
                    packaged_map.pop(tid, None)
                    break

                try:
                    pkg_f = float(pkg)
                except Exception:
                    packaged_map.pop(tid, None)
                    break

                if pkg_f != vol:
                    packaged_map[tid] = {
                        "typeID": tid,
                        "type": type_name_by_id.get(tid, str(tid)),
                        "packaged": pkg_f,
                    }
                else:
                    packaged_map.pop(tid, None)
                break

            if status == 404:
                packaged_map.pop(tid, None)
                break

            retryable = status in {0, 420, 429, 500, 502, 503, 504}
            if retryable and attempt < max_retries:
                _sleep_ms(min_delay_ms * attempt)
                continue

            log(f"[WARN] ESI type {tid} failed (status={status}) after {attempt} attempts; skipping.")
            break

        if idx % log_every == 0 or idx == len(new_type_ids):
            log(f"[PACKAGED] Progress {idx}/{len(new_type_ids)} (last typeID={tid}).")


# -----------------------------
# SDE readers
# -----------------------------

def read_regions(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapRegions.jsonl"):
        rid = int(obj["_key"])
        out[rid] = safe_en_name(obj, fallback=str(rid))
    return out


def read_constellations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapConstellations.jsonl"):
        cid = int(obj["_key"])
        out[cid] = (safe_en_name(obj, fallback=str(cid)), int(obj["regionID"]))
    return out


def read_factions(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "factions.jsonl"):
        fid = int(obj["_key"])
        out[fid] = safe_en_name(obj, fallback=str(fid))
    return out


def read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(sid))
        pos = obj.get("position") or {}
        position = [float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0))]
        sec = float(obj.get("securityStatus", 0.0))
        fid = obj.get("factionID")
        faction_id = int(fid) if fid is not None else None
        planets = len(obj.get("planetIDs", []) or []) if isinstance(obj.get("planetIDs"), list) else 0
        stargates = len(obj.get("stargateIDs", []) or []) if isinstance(obj.get("stargateIDs"), list) else 0

        out[sid] = {
            "name": name,
            "constellationID": int(obj["constellationID"]),
            "regionID": int(obj["regionID"]),
            "position": position,
            "factionID": faction_id,
            "securityStatus": sec,
            "planets": planets,
            "stargates": stargates,
        }
    return out


def read_planet_orbits(zf: zipfile.ZipFile, systems: Dict[int, Dict]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        pid = int(obj["_key"])
        sid = int(obj["solarSystemID"])
        ss_name = systems.get(sid, {"name": str(sid)})["name"]
        cidx = int(obj.get("celestialIndex", 0))
        out[pid] = planet_name(ss_name, cidx)
    return out


def read_moon_orbits(zf: zipfile.ZipFile, planet_orbits: Dict[int, str]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        mid = int(obj["_key"])
        pid = int(obj["orbitID"])
        p_orbit = planet_orbits.get(pid, str(pid))
        oidx = int(obj.get("orbitIndex", 0))
        out[mid] = moon_name(p_orbit, oidx)
    return out


def count_moons_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    c: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        sid = int(obj["solarSystemID"])
        c[sid] += 1
    return dict(c)


def count_asteroid_belts_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    c: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        sid = int(obj["solarSystemID"])
        belts = obj.get("asteroidBeltIDs")
        if isinstance(belts, list):
            c[sid] += len(belts)
    return dict(c)


def read_corporations(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "npcCorporations.jsonl"):
        cid = int(obj["_key"])
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def read_station_services(zf: zipfile.ZipFile) -> Dict[int, str]:
    def norm(s: str) -> str:
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

    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "stationServices.jsonl"):
        sid = int(obj["_key"])
        sn = obj.get("serviceName") or {}
        en = sn.get("en") if isinstance(sn, dict) else None
        en = en if isinstance(en, str) and en else str(sid)
        out[sid] = CANON.get(norm(en), norm(en))
    return out


def read_station_operations(zf: zipfile.ZipFile, service_keys: Dict[int, str]) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "stationOperations.jsonl"):
        oid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(oid))
        use_op = bool(obj.get("useOperationName", False))
        svc_ids = obj.get("services") or []
        svc_set: Set[str] = set()
        if isinstance(svc_ids, list):
            for s in svc_ids:
                try:
                    sid = int(s)
                except Exception:
                    continue
                k = service_keys.get(sid)
                if k:
                    svc_set.add(k)
        out[oid] = {"operationName": name, "useOperationName": use_op, "services": svc_set}
    return out


def read_type_name_map(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        tid = int(obj["_key"])
        out[tid] = safe_en_name(obj, fallback=str(tid))
    return out


def read_market_groups(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, Optional[int]]]:
    out: Dict[int, Tuple[str, Optional[int]]] = {}
    for obj in iter_jsonl_from_zip(zf, "marketGroups.jsonl"):
        mgid = int(obj["_key"])
        name = safe_en_name(obj, fallback=str(mgid))
        parent = obj.get("parentGroupID")
        parent_id = int(parent) if parent is not None else None
        out[mgid] = (name, parent_id)
    return out


def read_groups_meta(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    out: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "groups.jsonl"):
        gid = int(obj["_key"])
        out[gid] = (safe_en_name(obj, fallback=str(gid)), int(obj["categoryID"]))
    return out


def read_categories(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "categories.jsonl"):
        cid = int(obj["_key"])
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def read_contraband_set(zf: zipfile.ZipFile) -> Set[int]:
    s: Set[int] = set()
    for obj in iter_jsonl_from_zip(zf, "contrabandTypes.jsonl"):
        s.add(int(obj["_key"]))
    return s


def read_published_types_base(zf: zipfile.ZipFile) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not _get_bool(obj, "published", default=False):
            continue
        tid = int(obj["_key"])
        out[tid] = {
            "type": safe_en_name(obj, fallback=str(tid)),
            "volume": float(obj.get("volume", 0.0)),
            "group_id": _get_int(obj, "group_id", "groupID"),
            "marketGroupID": _get_int(obj, "marketGroupID", "market_group_id", "marketGroupId"),
        }
    return out


# -----------------------------
# builders
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


def _dist_au(p1: Tuple[float, float, float], p2: Tuple[float, float, float]) -> float:
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    dz = p2[2] - p1[2]
    meters = math.sqrt(dx * dx + dy * dy + dz * dz)
    return meters / AU_METERS


def build_stargates_out(zf: zipfile.ZipFile, systems: Dict[int, Dict]) -> List[Dict]:
    """
    stargates.jsonl.gz:
      - stargateID
      - stargate           "<srcSystemName> → <dstSystemName>"
      - stargateGroup      "<minSystemIDName> ↔ <maxSystemIDName>" by systemID
      - solarSystem        source solar system name
      - stargateDistance   null if only 1 gate in system else list of other gates in same system:
                           [{"stargate": "<src → otherDst>", "distanceAU": <double>}...]
                           where distanceAU is computed from positions of the gates within the system.
    """
    # First pass: parse all gates, keep position and grouping by source system.
    gates: List[Dict] = []
    by_system: Dict[int, List[Dict]] = defaultdict(list)

    parsed = 0
    for obj in iter_jsonl_from_zip(zf, "mapStargates.jsonl"):
        parsed += 1
        if parsed % 20000 == 0:
            log(f"[STARGATES] parsed {parsed}")

        gid = int(obj["_key"])
        src_id = int(obj["solarSystemID"])
        dest = obj.get("destination") or {}
        dst_id = int(dest["solarSystemID"])

        src_name = systems.get(src_id, {"name": str(src_id)})["name"]
        dst_name = systems.get(dst_id, {"name": str(dst_id)})["name"]

        lo_id, hi_id = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        lo_name = systems.get(lo_id, {"name": str(lo_id)})["name"]
        hi_name = systems.get(hi_id, {"name": str(hi_id)})["name"]

        pos = obj.get("position") or {}
        p = (float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0)))

        stargate_str = f"{src_name} → {dst_name}"
        group_str = f"{lo_name} ↔ {hi_name}"

        rec = {
            "stargateID": gid,
            "stargate": stargate_str,
            "stargateGroup": group_str,
            "solarSystem": src_name,
            "_srcSystemID": src_id,
            "_pos": p,
        }
        gates.append(rec)
        by_system[src_id].append(rec)

    # Second pass: compute distances within each system
    for sid, lst in by_system.items():
        if len(lst) <= 1:
            lst[0]["stargateDistance"] = None
            continue

        for g in lst:
            p0 = g["_pos"]
            distances: List[Dict] = []
            for h in lst:
                if h is g:
                    continue  # exclude itself (distance 0)
                au = _dist_au(p0, h["_pos"])
                # keep numeric double; rounding is not required by spec
                distances.append({"stargate": h["stargate"], "distanceAU": au})

            # Deterministic order: by distance asc, then stargateID asc
            distances.sort(key=lambda r: (float(r["distanceAU"]), str(r["stargate"])))
            g["stargateDistance"] = distances

    # Cleanup internal fields + final sort
    for r in gates:
        r.pop("_srcSystemID", None)
        r.pop("_pos", None)

    gates.sort(key=lambda r: r["stargateID"])
    return gates


def market_tree_string(mgid: Optional[int], mg_map: Dict[int, Tuple[str, Optional[int]]]) -> Optional[str]:
    if mgid is None or mgid not in mg_map:
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
    packaged_map: Dict[int, Dict],
    groups_meta: Dict[int, Tuple[str, int]],
    categories: Dict[int, str],
    market_groups: Dict[int, Tuple[str, Optional[int]]],
    contraband: Set[int],
) -> List[Dict]:
    rows: List[Dict] = []
    for tid, t in types_base.items():
        volume = float(t["volume"])
        pkg = packaged_map.get(tid, {}).get("packaged")
        packaged = float(pkg) if pkg is not None else volume

        gid = t.get("group_id")
        gname = ""
        cname = ""
        if gid is not None and int(gid) in groups_meta:
            gname, cat_id = groups_meta[int(gid)]
            cname = categories.get(cat_id, str(cat_id))

        mgid = t.get("marketGroupID")

        mgname: Optional[str] = None
        if mgid is not None and int(mgid) in market_groups:
            mgname = market_groups[int(mgid)][0]

        mtree = market_tree_string(int(mgid) if mgid is not None else None, market_groups)

        rows.append(
            {
                "typeID": tid,
                "type": t["type"],
                "volume": volume,
                "packaged": packaged,
                "group": gname,
                "category": cname,
                "marketGroup": mgname,   # None -> null
                "marketTree": mtree,     # None -> null
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
        if isinstance(mt, str) and mt:
            buckets[mt].add(str(r.get("type", "")))
    out: List[Dict] = []
    for mt, tset in buckets.items():
        out.append({"marketTree": mt, "types": sorted(tset)})
    out.sort(key=lambda r: r["marketTree"])
    return out


def build_market_tree_txt(
    market_groups: Dict[int, Tuple[str, Optional[int]]],
    types_base: Dict[int, Dict],
    contraband: Set[int],
) -> str:
    children: Dict[Optional[int], List[int]] = defaultdict(list)
    for mgid, (_name, parent) in market_groups.items():
        children[parent].append(mgid)
    for k in list(children.keys()):
        children[k].sort(key=lambda mgid: market_groups[mgid][0].lower())

    group_types: Dict[int, List[Tuple[int, str]]] = defaultdict(list)
    for tid, t in types_base.items():
        mgid = t.get("marketGroupID")
        if mgid is None:
            continue
        mgid = int(mgid)
        if mgid in market_groups:
            group_types[mgid].append((int(tid), str(t.get("type", ""))))
    for mgid in list(group_types.keys()):
        group_types[mgid].sort(key=lambda p: p[1].lower())

    roots = children.get(None, [])

    def type_icon(tid: int) -> str:
        return "☠️" if tid in contraband else "📦"

    lines: List[str] = ["Market"]

    def walk(mgid: int, prefix: str, is_last: bool) -> None:
        name = market_groups[mgid][0]
        branch = "└── " if is_last else "├── "
        lines.append(f"{prefix}{branch}📁 {name}")
        new_prefix = prefix + ("    " if is_last else "│   ")

        kids = children.get(mgid, [])
        tps = group_types.get(mgid, [])

        for i, child in enumerate(kids):
            child_is_last = (i == len(kids) - 1) and (len(tps) == 0)
            walk(child, new_prefix, child_is_last)

        for j, (tid, tname) in enumerate(tps):
            b = "└── " if j == len(tps) - 1 else "├── "
            lines.append(f"{new_prefix}{b}{type_icon(tid)} {tname}")

    for i, root in enumerate(roots):
        walk(root, "", i == len(roots) - 1)

    lines.append("")
    lines.append("Leyenda: ☠️ contraband | 📦 normal")
    lines.append("")
    return "\n".join(lines)


def build_excluded_market_types(
    exclude_config: Path,
    types_rows: List[Dict],
    market_tree_rows: List[Dict],
) -> List[Dict]:
    # typeName -> (typeID, marketTree)
    type_index: Dict[str, Tuple[int, Optional[str]]] = {}
    for r in types_rows:
        tname = str(r.get("type", ""))
        tid = _get_int(r, "typeID")
        if not tname or tid is None:
            continue
        type_index[tname] = (tid, r.get("marketTree"))

    # marketTree -> [typeName...]
    tree_to_types: Dict[str, List[str]] = {}
    for r in market_tree_rows:
        mt = str(r.get("marketTree", ""))
        tps = r.get("types") or []
        if isinstance(tps, list):
            tree_to_types[mt] = [str(x) for x in tps]

    out: List[Dict] = []
    if not exclude_config.exists():
        return out

    for raw in exclude_config.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.startswith("Market → "):
            wants_prefix = line.endswith(" → ...")
            base = line[:-6] if wants_prefix else line  # remove trailing " → ..."

            if wants_prefix:
                prefix = base + " → "
                matched_trees = [mt for mt in tree_to_types.keys() if mt == base or mt.startswith(prefix)]
            else:
                matched_trees = [base] if base in tree_to_types else []

            for mt in matched_trees:
                for tname in tree_to_types.get(mt, []):
                    if tname in type_index:
                        tid, mtree = type_index[tname]
                        out.append({"typeID": tid, "type": tname, "marketTree": mtree})

        else:
            if line in type_index:
                tid, mtree = type_index[line]
                out.append({"typeID": tid, "type": line, "marketTree": mtree})

    uniq: Dict[int, Dict] = {}
    for r in out:
        uniq[int(r["typeID"])] = r
    out2 = list(uniq.values())
    out2.sort(key=lambda r: int(r["typeID"]))
    return out2


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
    stations_count: Dict[int, int] = defaultdict(int)

    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    n = 0
    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        n += 1
        if n % 5000 == 0:
            log(f"[STATIONS] parsed {n}")

        station_id = int(obj["_key"])
        sid = int(obj.get("solarSystemID", -1))
        stations_count[sid] += 1

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
                "_solarSystemID": sid,  # internal
                "stationID": station_id,
                "station": station_name,
                "stationType": station_type,
                "solarSystem": ss_name,
                "owner": owner,
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
    return rows, dict(system_labels), dict(stations_count)


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

        sec = float(s.get("securityStatus", 0.0))
        sec6 = float(f"{sec:.6f}")  # hard requirement

        rows.append(
            {
                "solarSystemID": sid,
                "solarSystem": s["name"],
                "constellation": cname,
                "region": rname,
                "position": s.get("position", [0.0, 0.0, 0.0]),
                "faction": faction,
                "securityStatus": sec6,
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


# -----------------------------
# main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="Path to CCP SDE JSONL ZIP")
    ap.add_argument("--out-sde", required=True, help="Output directory for SDE")
    ap.add_argument("--out-esi", required=True, help="Output directory for ESI")
    ap.add_argument("--baseline-types", required=False, default="", help="Committed types.jsonl.gz (types1)")
    ap.add_argument("--existing-packaged", required=False, default="", help="Committed packaged.jsonl.gz (exceptions)")
    ap.add_argument("--exclude-config", required=False, default="", help="config/excludeMarketTypes.txt")
    ap.add_argument("--exclude-state", required=False, default="", help="states/excludeMarketTypes.json (for comparison only)")
    args = ap.parse_args()

    out_sde = Path(args.out_sde)
    out_esi = Path(args.out_esi)
    out_sde.mkdir(parents=True, exist_ok=True)
    out_esi.mkdir(parents=True, exist_ok=True)

    baseline_ids = load_baseline_typeids(args.baseline_types) if args.baseline_types else set()
    log(f"[DELTA] baseline types1 size: {len(baseline_ids)}")

    packaged_map = load_existing_packaged(args.existing_packaged) if args.existing_packaged else {}
    log(f"[PACKAGED] existing exceptions in repo: {len(packaged_map)}")

    exclude_config = Path(args.exclude_config) if args.exclude_config else Path("config/excludeMarketTypes.txt")
    exclude_state = Path(args.exclude_state) if args.exclude_state else Path("states/excludeMarketTypes.json")

    with zipfile.ZipFile(args.zip) as zf:
        regions = read_regions(zf)
        consts = read_constellations(zf)
        factions = read_factions(zf)
        systems = read_solarsystems(zf)
        corp_names = read_corporations(zf)

        log(f"[SDE] regions={len(regions)} constellations={len(consts)} systems={len(systems)} corps={len(corp_names)}")

        types_base = read_published_types_base(zf)
        types2_ids = set(types_base.keys())
        log(f"[DELTA] new SDE types2 size: {len(types2_ids)}")

        types3_ids = sorted([tid for tid in types2_ids if tid not in baseline_ids])
        log(f"[DELTA] types3 (types2 - types1) size: {len(types3_ids)}")

        before = len(packaged_map)
        packaged_map = {tid: row for tid, row in packaged_map.items() if tid in types2_ids}
        pruned = before - len(packaged_map)
        if pruned:
            log(f"[PACKAGED] pruned {pruned} exceptions (types no longer in SDE).")

        type_name_by_id = {tid: t["type"] for tid, t in types_base.items()}
        sde_volume_by_id = {tid: float(t["volume"]) for tid, t in types_base.items()}

        refresh_packaged_for_new_types(
            new_type_ids=types3_ids,
            type_name_by_id=type_name_by_id,
            sde_volume_by_id=sde_volume_by_id,
            packaged_map=packaged_map,
        )

        write_packaged_exceptions(out_esi / "packaged.jsonl.gz", packaged_map)
        log(f"[PACKAGED] wrote exceptions: {len(packaged_map)} -> {out_esi/'packaged.jsonl.gz'}")

        market_groups = read_market_groups(zf)
        groups_meta = read_groups_meta(zf)
        categories = read_categories(zf)
        contraband = read_contraband_set(zf)

        types_rows = build_types_out(
            types_base=types_base,
            packaged_map=packaged_map,
            groups_meta=groups_meta,
            categories=categories,
            market_groups=market_groups,
            contraband=contraband,
        )

        market_tree_rows = build_market_tree_jsonl(types_rows)
        market_tree_txt = build_market_tree_txt(market_groups, types_base, contraband)

        type_names = read_type_name_map(zf)
        planet_orbits = read_planet_orbits(zf, systems)
        moon_orbits = read_moon_orbits(zf, planet_orbits)
        service_keys = read_station_services(zf)
        operations = read_station_operations(zf, service_keys)

        stations_rows, system_station_labels, stations_count = build_stations_out(
            zf=zf,
            systems=systems,
            corp_names=corp_names,
            operations=operations,
            planet_orbits=planet_orbits,
            moon_orbits=moon_orbits,
            type_names=type_names,
        )

        system_cyno_jump: Dict[int, str] = {}
        for sid in systems.keys():
            labels = system_station_labels.get(sid, set())
            scount = int(stations_count.get(sid, 0))
            system_cyno_jump[sid] = system_cyno(labels, scount)

        for r in stations_rows:
            sid = int(r.pop("_solarSystemID", -1))
            r["cynoJumpSecurity"] = system_cyno_jump.get(sid, "no jump")

        moons_count = count_moons_by_system(zf)
        belts_count = count_asteroid_belts_by_system(zf)

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

        excluded_rows = build_excluded_market_types(exclude_config, types_rows, market_tree_rows)

        cfg_etag = sha256_file(exclude_config)
        write_text(out_sde / "excludeMarketTypes.state.json", json.dumps({"etag": cfg_etag}, ensure_ascii=False, indent=2) + "\n")

        write_jsonl_gz(out_sde / "regions.jsonl.gz", build_regions_out(regions))
        write_jsonl_gz(out_sde / "constellations.jsonl.gz", build_constellations_out(consts, regions))
        write_jsonl_gz(out_sde / "solarsystems.jsonl.gz", sol_rows)
        write_jsonl_gz(out_sde / "stations.jsonl.gz", stations_rows)
        write_jsonl_gz(out_sde / "stargates.jsonl.gz", build_stargates_out(zf, systems))
        write_jsonl_gz(out_sde / "types.jsonl.gz", types_rows)
        write_jsonl_gz(out_sde / "corporations.jsonl.gz", build_corporations_out(corp_names))
        write_jsonl_gz(out_sde / "marketTree.jsonl.gz", market_tree_rows)
        write_jsonl_gz(out_sde / "excludedMarketTypes.jsonl.gz", excluded_rows)
        write_text(out_sde / "marketTree.txt", market_tree_txt)

    expected = [
        out_sde / "regions.jsonl.gz",
        out_sde / "constellations.jsonl.gz",
        out_sde / "solarsystems.jsonl.gz",
        out_sde / "stations.jsonl.gz",
        out_sde / "stargates.jsonl.gz",
        out_sde / "types.jsonl.gz",
        out_sde / "corporations.jsonl.gz",
        out_sde / "marketTree.jsonl.gz",
        out_sde / "excludedMarketTypes.jsonl.gz",
        out_sde / "marketTree.txt",
        out_sde / "excludeMarketTypes.state.json",
        out_esi / "packaged.jsonl.gz",
    ]
    for p in expected:
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Missing/empty output: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
