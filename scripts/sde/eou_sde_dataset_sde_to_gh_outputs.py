"""
EOU · SDE Dataset (SDE → GH) — build outputs

Genera (sobrescribe) en el directorio de salida:

  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz   (NOW with: position, faction, securityStatus(rounded), counts, cynoJumpSecurity)
  - stations.jsonl.gz       (NOW with: owner, cynoDockSecurity, service booleans)
  - stargates.jsonl.gz
  - types.jsonl.gz          (incluye group, category, marketGroup; published-only)
  - corporations.jsonl.gz   (npcCorporations)

Solo usa el ZIP oficial CCP SDE JSONL (no ESI, no "extended").
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

import zipfile

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from eou_sde_dataset_sde_to_gh_io import iter_jsonl_from_zip, write_jsonl_gz  # noqa: E402
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name  # noqa: E402
from eou_sde_dataset_sde_to_gh_cynodock import station_cyno, system_cyno  # noqa: E402


# -----------------------------
# Numeric helpers
# -----------------------------

_Q6 = Decimal("0.000001")


def round6(x: float) -> float:
    """Round to 6 decimals deterministically (Decimal, HALF_UP)."""
    return float(Decimal(str(x)).quantize(_Q6, rounding=ROUND_HALF_UP))


# -----------------------------
# Helpers de lectura (SDE)
# -----------------------------

def _read_regions(zf: zipfile.ZipFile) -> Dict[int, str]:
    regions: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapRegions.jsonl"):
        rid = int(obj.get("_key"))
        regions[rid] = safe_en_name(obj, fallback=str(rid))
    return regions


def _read_constellations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    # constellationID -> (constellationNameEn, regionID)
    consts: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapConstellations.jsonl"):
        cid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(cid))
        region_id = int(obj.get("regionID"))
        consts[cid] = (name, region_id)
    return consts


def _read_factions(zf: zipfile.ZipFile) -> Dict[int, str]:
    # factionID -> factionNameEn
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "factions.jsonl"):
        fid = int(obj.get("_key"))
        out[fid] = safe_en_name(obj, fallback=str(fid))
    return out


def _read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Dict]:
    """
    solarSystemID -> dict with:
      - name: str
      - constellationID: int
      - regionID: int
      - position: {"x": float, "y": float, "z": float}
      - securityStatus: float
      - factionID: Optional[int]
      - planets: int (len(planetIDs) or 0)
      - stargates: int (len(stargateIDs) or 0)
    """
    systems: Dict[int, Dict] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(sid))
        constellation_id = int(obj.get("constellationID"))
        region_id = int(obj.get("regionID"))

        pos = obj.get("position") or {}
        # Keep as object {x,y,z} to match SDE structure.
        position = {
            "x": float(pos.get("x", 0.0)),
            "y": float(pos.get("y", 0.0)),
            "z": float(pos.get("z", 0.0)),
        }

        sec = obj.get("securityStatus")
        security_status = float(sec) if sec is not None else 0.0  # required in schema; fallback just in case

        faction_id = obj.get("factionID")
        faction_id_int: Optional[int] = int(faction_id) if faction_id is not None else None

        planet_ids = obj.get("planetIDs")
        planets = len(planet_ids) if isinstance(planet_ids, list) else 0

        stargate_ids = obj.get("stargateIDs")
        stargates = len(stargate_ids) if isinstance(stargate_ids, list) else 0

        systems[sid] = {
            "name": name,
            "constellationID": constellation_id,
            "regionID": region_id,
            "position": position,
            "securityStatus": security_status,
            "factionID": faction_id_int,
            "planets": planets,
            "stargates": stargates,
        }
    return systems


def _read_planet_orbit_names(
    zf: zipfile.ZipFile,
    systems: Dict[int, Dict],
) -> Dict[int, str]:
    # planetID -> orbitName (e.g. "Jita IV")
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        pid = int(obj.get("_key"))
        solar_system_id = int(obj.get("solarSystemID"))
        ss_name = systems.get(solar_system_id, {"name": str(solar_system_id)}).get("name", str(solar_system_id))
        cidx = int(obj.get("celestialIndex"))
        out[pid] = planet_name(ss_name, cidx)
    return out


def _read_moon_orbit_names(
    zf: zipfile.ZipFile,
    planet_orbits: Dict[int, str],
) -> Dict[int, str]:
    # moonID -> orbitName (e.g. "Jita IV - Moon 4")
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        mid = int(obj.get("_key"))
        planet_id = int(obj.get("orbitID"))
        p_orbit = planet_orbits.get(planet_id, str(planet_id))
        oidx = int(obj.get("orbitIndex"))
        out[mid] = moon_name(p_orbit, oidx)
    return out


def _count_moons_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    counts: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        sid = int(obj.get("solarSystemID"))
        counts[sid] += 1
    return dict(counts)


def _count_asteroid_belts_by_system(zf: zipfile.ZipFile) -> Dict[int, int]:
    counts: Dict[int, int] = defaultdict(int)
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        sid = int(obj.get("solarSystemID"))
        belts = obj.get("asteroidBeltIDs")
        if isinstance(belts, list):
            counts[sid] += len(belts)
    return dict(counts)


def _read_corporations(zf: zipfile.ZipFile) -> Dict[int, str]:
    corps: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "npcCorporations.jsonl"):
        cid = int(obj.get("_key"))
        corps[cid] = safe_en_name(obj, fallback=str(cid))
    return corps


def _read_station_services(zf: zipfile.ZipFile) -> Dict[int, str]:
    """
    stationServices.jsonl:
      _key: serviceID
      serviceName.en: human name of the service

    Return: serviceID -> normalized service key (kebab-case, lowercase),
    with a conservative canonicalization for the flags we need.
    """
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
        "repairfacility": "repair-facilities",
        "fitting": "fitting",
        "cloning": "cloning",
        "jump-clone-facility": "jump-clone-facility",
        "jump-clone": "jump-clone-facility",
        "jumpclone": "jump-clone-facility",
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
    """
    stationOperations.jsonl:
      _key: operationID
      operationName.* : localized name
      useOperationName: bool
      services: [serviceID, ...]

    Return:
      operationID -> {
        "operationName": str,
        "useOperationName": bool,
        "services": set[str]   # normalized/canonical service keys
      }
    """
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
    # typeID -> typeNameEn
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        tid = int(obj.get("_key"))
        out[tid] = safe_en_name(obj, fallback=str(tid))
    return out


def _read_marketgroup_names(zf: zipfile.ZipFile) -> Dict[int, str]:
    # marketGroupID -> marketGroupNameEn
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
    # categoryID -> categoryNameEn
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "categories.jsonl"):
        cid = int(obj.get("_key"))
        out[cid] = safe_en_name(obj, fallback=str(cid))
    return out


def _read_groups_meta(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    # groupID -> (groupNameEn, categoryID)
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


# -----------------------------
# Builders de outputs
# -----------------------------

def build_regions_out(regions: Dict[int, str]) -> List[Dict]:
    rows = [{"regionID": rid, "region": name} for rid, name in regions.items()]
    rows.sort(key=lambda r: r["regionID"])
    return rows


def build_constellations_out(
    consts: Dict[int, Tuple[str, int]],
    regions: Dict[int, str],
) -> List[Dict]:
    rows: List[Dict] = []
    for cid, (cname, rid) in consts.items():
        rows.append(
            {
                "constellationID": cid,
                "constellation": cname,
                "region": regions.get(rid, str(rid)),
            }
        )
    rows.sort(key=lambda r: r["constellationID"])
    return rows


def build_corporations_out(corp_names: Dict[int, str]) -> List[Dict]:
    """
    corporations.jsonl.gz:
      - corporationID  (npcCorporations._key)
      - corporation    (npcCorporations.name.en)
    """
    rows: List[Dict] = [{"corporationID": cid, "corporation": name} for cid, name in corp_names.items()]
    rows.sort(key=lambda r: r["corporationID"])
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
    """
    stations.jsonl.gz (updated):
      - stationID
      - station         (SDE-only constructed name)
      - stationType     (types.name.en via station.typeID)
      - solarSystem     (mapSolarSystems.name.en)
      - owner           (npcCorporations.name.en via ownerID)
      - cynoDockSecurity (derived; null if docking=false)
      - docking, market, storage, repair, fitting, cloning, jump-clone  (bool, no null)

    Returns:
      rows,
      system_station_cyno_labels: solarSystemID -> set(labels)  (labels may include None, but we store Optional[str])
      stations_count: solarSystemID -> count of npcStations
    """
    rows: List[Dict] = []
    system_cyno_labels: Dict[int, Set[Optional[str]]] = defaultdict(set)
    stations_count: Dict[int, int] = defaultdict(int)

    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        station_id = int(obj.get("_key"))

        solar_system_id = _get_int(obj, "solarSystemID") or -1
        stations_count[solar_system_id] += 1
        ss_name = systems.get(solar_system_id, {"name": str(solar_system_id)}).get("name", str(solar_system_id))

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

        # Cyno dock security (null if docking is false)
        _level, dock_label = station_cyno(station_type if station_type else None, docking)

        # Accumulate per-system labels for later cynoJumpSecurity aggregation
        system_cyno_labels[solar_system_id].add(dock_label)

        rows.append(
            {
                "stationID": station_id,
                "station": station_name,
                "stationType": station_type,
                "solarSystem": ss_name,
                "owner": owner,
                "cynoDockSecurity": dock_label,  # may be None
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
    return rows, dict(system_cyno_labels), dict(stations_count)


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
    """
    solarsystems.jsonl.gz (updated):
      - solarSystemID
      - solarSystem
      - constellation
      - region
      - position                  (object {x,y,z} as in SDE)
      - faction                   (name.en or null)
      - securityStatus            (rounded to 6 decimals, never null)
      - planets, moons, asteroid_belts, stargates, stations  (int, never null)
      - cynoJumpSecurity          ("safe"|"risky"|"unsafe"|"no jump")
    """
    rows: List[Dict] = []
    for sid, s in systems.items():
        sname = s["name"]
        cid = int(s["constellationID"])
        rid = int(s["regionID"])
        cname = consts.get(cid, (str(cid), 0))[0]
        rname = regions.get(rid, str(rid))

        fid = s.get("factionID")
        faction = factions.get(int(fid)) if fid is not None and int(fid) in factions else None

        sec = round6(float(s.get("securityStatus", 0.0)))
        planets = int(s.get("planets", 0))
        stargates = int(s.get("stargates", 0))
        moons = int(moons_count.get(sid, 0))
        asteroid_belts = int(belts_count.get(sid, 0))
        stations = int(stations_count.get(sid, 0))
        cyno_jump = system_cyno_jump.get(sid, "no jump")

        rows.append(
            {
                "solarSystemID": sid,
                "solarSystem": sname,
                "constellation": cname,
                "region": rname,
                "position": s.get("position", {"x": 0.0, "y": 0.0, "z": 0.0}),
                "faction": faction,
                "securityStatus": sec,
                "planets": planets,
                "moons": moons,
                "asteroid_belts": asteroid_belts,
                "stargates": stargates,
                "stations": stations,
                "cynoJumpSecurity": cyno_jump,
            }
        )

    rows.sort(key=lambda r: r["solarSystemID"])
    return rows


def build_stargates_out(zf: zipfile.ZipFile, systems: Dict[int, Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "mapStargates.jsonl"):
        gid = int(obj.get("_key"))
        src_id = int(obj.get("solarSystemID"))
        dest = obj.get("destination") or {}
        dst_id = int(dest.get("solarSystemID"))

        src_name = systems.get(src_id, {"name": str(src_id)}).get("name", str(src_id))
        dst_name = systems.get(dst_id, {"name": str(dst_id)}).get("name", str(dst_id))

        stargate_value = f"{src_name} → {dst_name}"

        lo_id, hi_id = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        lo_name = systems.get(lo_id, {"name": str(lo_id)}).get("name", str(lo_id))
        hi_name = systems.get(hi_id, {"name": str(hi_id)}).get("name", str(hi_id))
        stargate_group = f"{lo_name} ↔ {hi_name}"

        rows.append(
            {
                "stargateID": gid,
                "stargate": stargate_value,
                "stargateGroup": stargate_group,
                "solarSystem": src_name,
            }
        )

    rows.sort(key=lambda r: r["stargateID"])
    return rows


def build_types_out(zf: zipfile.ZipFile) -> List[Dict]:
    """
    types.jsonl.gz:
      - typeID
      - type
      - group          (name.en)
      - category       (categories.jsonl.name.en via groups.jsonl.categoryID)
      - marketGroup    (name.en)
      - is_contraband
      - is_gategank    (group name == "Smart Bomb")

    IMPORTANTE:
      - solo published == true
    """
    groups_meta = _read_groups_meta(zf)
    categories = _read_categories(zf)
    marketgroup_names = _read_marketgroup_names(zf)
    contraband = _read_contraband_set(zf)

    rows: List[Dict] = []

    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not _get_bool(obj, "published", default=False):
            continue

        tid = int(obj.get("_key"))
        tname = safe_en_name(obj, fallback=str(tid))

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

        rows.append(
            {
                "typeID": tid,
                "type": tname,
                "group": gname,
                "category": cname,
                "marketGroup": mgname,
                "is_contraband": tid in contraband,
                "is_gategank": gname == "Smart Bomb",
            }
        )

    rows.sort(key=lambda r: r["typeID"])
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

    with zipfile.ZipFile(args.zip) as zf:
        # Base geo dictionaries
        regions = _read_regions(zf)
        consts = _read_constellations(zf)
        factions = _read_factions(zf)
        systems = _read_solarsystems(zf)

        # These outputs don't depend on cyno logic
        write_jsonl_gz(out_dir / "regions.jsonl.gz", build_regions_out(regions))
        write_jsonl_gz(out_dir / "constellations.jsonl.gz", build_constellations_out(consts, regions))

        # Corporations
        corp_names = _read_corporations(zf)
        write_jsonl_gz(out_dir / "corporations.jsonl.gz", build_corporations_out(corp_names))

        # Station/service dictionaries
        type_names = _read_type_name_map(zf)
        service_keys = _read_station_services(zf)
        operations = _read_station_operations(zf, service_keys)

        # Orbit naming helpers for station name construction
        planet_orbits = _read_planet_orbit_names(zf, systems)
        moon_orbits = _read_moon_orbit_names(zf, planet_orbits)

        # Build stations first so we can derive cynoJumpSecurity per solar system
        stations_rows, system_station_labels, stations_count = build_stations_out(
            zf=zf,
            systems=systems,
            corp_names=corp_names,
            operations=operations,
            planet_orbits=planet_orbits,
            moon_orbits=moon_orbits,
            type_names=type_names,
        )
        write_jsonl_gz(out_dir / "stations.jsonl.gz", stations_rows)

        # Counts for solarsystems
        moons_count = _count_moons_by_system(zf)
        belts_count = _count_asteroid_belts_by_system(zf)

        # Derive cynoJumpSecurity per system following the user-defined priority
        system_cyno_jump: Dict[int, str] = {}
        for sid in systems.keys():
            labels = system_station_labels.get(sid, set())
            scount = int(stations_count.get(sid, 0))
            system_cyno_jump[sid] = system_cyno(labels, scount)

        # Now we can build solarsystems consistently (includes cynoJumpSecurity)
        solarsystems_rows = build_solarsystems_out(
            systems=systems,
            consts=consts,
            regions=regions,
            factions=factions,
            moons_count=moons_count,
            belts_count=belts_count,
            stations_count=stations_count,
            system_cyno_jump=system_cyno_jump,
        )
        write_jsonl_gz(out_dir / "solarsystems.jsonl.gz", solarsystems_rows)

        # Stargates
        write_jsonl_gz(out_dir / "stargates.jsonl.gz", build_stargates_out(zf, systems))

        # Types
        write_jsonl_gz(out_dir / "types.jsonl.gz", build_types_out(zf))

    # Sanity check: ficheros existen y no vacíos
    expected = [
        "regions.jsonl.gz",
        "constellations.jsonl.gz",
        "solarsystems.jsonl.gz",
        "stations.jsonl.gz",
        "stargates.jsonl.gz",
        "types.jsonl.gz",
        "corporations.jsonl.gz",
    ]
    for name in expected:
        p = out_dir / name
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Missing/empty output: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
