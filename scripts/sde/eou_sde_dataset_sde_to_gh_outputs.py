"""
EOU · SDE Dataset (SDE → GH) — build outputs

Genera (sobrescribe) en el directorio de salida:

  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz
  - stations.jsonl.gz
  - stargates.jsonl.gz
  - types.jsonl.gz   (incluye group, category, marketGroup; published-only)

Solo usa el ZIP oficial CCP SDE JSONL (no ESI, no "extended").
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

import zipfile

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from eou_sde_dataset_sde_to_gh_io import iter_jsonl_from_zip, write_jsonl_gz  # noqa: E402
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name  # noqa: E402


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


def _read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int, int]]:
    # solarSystemID -> (solarSystemNameEn, constellationID, regionID)
    systems: Dict[int, Tuple[str, int, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(sid))
        constellation_id = int(obj.get("constellationID"))
        region_id = int(obj.get("regionID"))
        systems[sid] = (name, constellation_id, region_id)
    return systems


def _read_planet_orbit_names(
    zf: zipfile.ZipFile,
    systems: Dict[int, Tuple[str, int, int]],
) -> Dict[int, str]:
    # planetID -> orbitName (e.g. "Jita IV")
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
    # moonID -> orbitName (e.g. "Jita IV - Moon 4")
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapMoons.jsonl"):
        mid = int(obj.get("_key"))
        planet_id = int(obj.get("orbitID"))
        p_orbit = planet_orbits.get(planet_id, str(planet_id))
        oidx = int(obj.get("orbitIndex"))
        out[mid] = moon_name(p_orbit, oidx)
    return out


def _read_corporations(zf: zipfile.ZipFile) -> Dict[int, str]:
    corps: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "npcCorporations.jsonl"):
        cid = int(obj.get("_key"))
        corps[cid] = safe_en_name(obj, fallback=str(cid))
    return corps


def _read_station_operations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, bool]]:
    # operationID -> (operationName, useOperationName)
    ops: Dict[int, Tuple[str, bool]] = {}
    for obj in iter_jsonl_from_zip(zf, "stationOperations.jsonl"):
        oid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(oid))
        use_op = bool(obj.get("useOperationName", False))
        ops[oid] = (name, use_op)
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
    # En el SDE oficial suele ser marketGroups.jsonl
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


def build_solarsystems_out(
    systems: Dict[int, Tuple[str, int, int]],
    consts: Dict[int, Tuple[str, int]],
    regions: Dict[int, str],
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
            }
        )
    rows.sort(key=lambda r: r["solarSystemID"])
    return rows


def build_stations_out(
    zf: zipfile.ZipFile,
    systems: Dict[int, Tuple[str, int, int]],
    corp_names: Dict[int, str],
    operations: Dict[int, Tuple[str, bool]],
    planet_orbits: Dict[int, str],
    moon_orbits: Dict[int, str],
    type_names: Dict[int, str],
) -> List[Dict]:
    """
    stations.jsonl.gz:
      - stationID
      - station      (nombre construido con SDE)
      - stationType  (nombre del type en inglés, via typeID->types.jsonl.name.en)
      - solarSystem
    """
    rows: List[Dict] = []

    # orbitID puede apuntar a planeta o luna; fallback: solar system name.
    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        station_id = int(obj.get("_key"))

        solar_system_id = _get_int(obj, "solarSystemID")
        if solar_system_id is None:
            solar_system_id = -1
        ss_name = systems.get(solar_system_id, (str(solar_system_id), 0, 0))[0]

        orbit_id = _get_int(obj, "orbitID")
        orbit_name = orbit_names.get(orbit_id, ss_name) if orbit_id is not None else ss_name

        owner_id = _get_int(obj, "ownerID") or -1
        corp = corp_names.get(owner_id, str(owner_id))

        op_id = _get_int(obj, "operationID")
        op_name = ""
        use_op = False
        if op_id is not None:
            op_name, use_op = operations.get(op_id, (str(op_id), False))

        # CCP naming rule:
        #  - if useOperationName: <orbitName> - <corporationName> <operationName>
        #  - else:              <orbitName> - <corporationName>
        station_name = f"{orbit_name} - {corp}".strip()
        if use_op and op_name:
            station_name = f"{station_name} {op_name}".strip()

        # stationType: resolver typeID del station usando types.jsonl.name.en
        st_type_id = _get_int(obj, "typeID", "stationTypeID", "stationTypeId")
        st_type_name = type_names.get(st_type_id, str(st_type_id)) if st_type_id is not None else ""

        rows.append(
            {
                "stationID": station_id,
                "station": station_name,
                "stationType": st_type_name,
                "solarSystem": ss_name,
            }
        )

    rows.sort(key=lambda r: r["stationID"])
    return rows


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
        regions = _read_regions(zf)
        consts = _read_constellations(zf)
        systems = _read_solarsystems(zf)

        # Base geo
        write_jsonl_gz(out_dir / "regions.jsonl.gz", build_regions_out(regions))
        write_jsonl_gz(out_dir / "constellations.jsonl.gz", build_constellations_out(consts, regions))
        write_jsonl_gz(out_dir / "solarsystems.jsonl.gz", build_solarsystems_out(systems, consts, regions))

        # Stations
        planet_orbits = _read_planet_orbit_names(zf, systems)
        moon_orbits = _read_moon_orbit_names(zf, planet_orbits)
        corp_names = _read_corporations(zf)
        operations = _read_station_operations(zf)
        type_names = _read_type_name_map(zf)

        write_jsonl_gz(
            out_dir / "stations.jsonl.gz",
            build_stations_out(zf, systems, corp_names, operations, planet_orbits, moon_orbits, type_names),
        )

        # Stargates
        write_jsonl_gz(out_dir / "stargates.jsonl.gz", build_stargates_out(zf, systems))

        # Types (incluye category)
        write_jsonl_gz(out_dir / "types.jsonl.gz", build_types_out(zf))

    # Sanity check: ficheros existen y no vacíos
    expected = [
        "regions.jsonl.gz",
        "constellations.jsonl.gz",
        "solarsystems.jsonl.gz",
        "stations.jsonl.gz",
        "stargates.jsonl.gz",
        "types.jsonl.gz",
    ]
    for name in expected:
        p = out_dir / name
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Missing/empty output: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
