"""EOU · SDE Dataset (SDE → GH) — build outputs.

Generates (overwrites) the following files in the provided output directory:

  - regions.jsonl.gz
  - constellations.jsonl.gz
  - solarsystems.jsonl.gz
  - stations.jsonl.gz
  - stargates.jsonl.gz
  - types.jsonl.gz

Only uses the official CCP SDE JSONL ZIP.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Dict, List, Set, Tuple

import zipfile

# Allow running this file directly ("python scripts/sde/...") without packaging.
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from eou_sde_dataset_sde_to_gh_io import iter_jsonl_from_zip, write_jsonl_gz  # noqa: E402
from eou_sde_dataset_sde_to_gh_names import moon_name, planet_name, safe_en_name  # noqa: E402


def _read_regions(zf: zipfile.ZipFile) -> Dict[int, str]:
    regions: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapRegions.jsonl"):
        rid = int(obj.get("_key"))
        regions[rid] = safe_en_name(obj, fallback=str(rid))
    return regions


def _read_constellations(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int]]:
    consts: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapConstellations.jsonl"):
        cid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(cid))
        region_id = int(obj.get("regionID"))
        consts[cid] = (name, region_id)
    return consts


def _read_solarsystems(zf: zipfile.ZipFile) -> Dict[int, Tuple[str, int, int]]:
    # solarSystemID -> (name_en, constellationID, regionID)
    systems: Dict[int, Tuple[str, int, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "mapSolarSystems.jsonl"):
        sid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(sid))
        constellation_id = int(obj.get("constellationID"))
        region_id = int(obj.get("regionID"))
        systems[sid] = (name, constellation_id, region_id)
    return systems


def _read_planet_orbit_names(zf: zipfile.ZipFile, systems: Dict[int, Tuple[str, int, int]]) -> Dict[int, str]:
    # planetID -> orbitName (e.g., "Jita IV")
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "mapPlanets.jsonl"):
        pid = int(obj.get("_key"))
        solar_system_id = int(obj.get("solarSystemID"))
        ss_name = systems.get(solar_system_id, (str(solar_system_id), 0, 0))[0]
        cidx = int(obj.get("celestialIndex"))
        out[pid] = planet_name(ss_name, cidx)
    return out


def _read_moon_orbit_names(zf: zipfile.ZipFile, planet_orbits: Dict[int, str]) -> Dict[int, str]:
    # moonID -> orbitName (e.g., "Jita IV - Moon 4")
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
    ops: Dict[int, Tuple[str, bool]] = {}
    for obj in iter_jsonl_from_zip(zf, "stationOperations.jsonl"):
        oid = int(obj.get("_key"))
        name = safe_en_name(obj, fallback=str(oid))
        use_op = bool(obj.get("useOperationName", False))
        ops[oid] = (name, use_op)
    return ops


def build_regions_out(regions: Dict[int, str]) -> List[Dict]:
    rows = [{"regionID": rid, "region": name} for rid, name in regions.items()]
    rows.sort(key=lambda r: r["regionID"])
    return rows


def build_constellations_out(
    consts: Dict[int, Tuple[str, int]],
    regions: Dict[int, str],
) -> List[Dict]:
    rows = []
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
    rows = []
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
) -> List[Dict]:
    rows = []

    # orbitID can point to a planet or moon. Fall back to solar system name.
    orbit_names: Dict[int, str] = {}
    orbit_names.update(planet_orbits)
    orbit_names.update(moon_orbits)

    for obj in iter_jsonl_from_zip(zf, "npcStations.jsonl"):
        station_id = int(obj.get("_key"))
        solar_system_id = int(obj.get("solarSystemID"))
        ss_name = systems.get(solar_system_id, (str(solar_system_id), 0, 0))[0]

        orbit_id = obj.get("orbitID")
        orbit_name = orbit_names.get(int(orbit_id), ss_name) if orbit_id is not None else ss_name

        owner_id = int(obj.get("ownerID"))
        corp = corp_names.get(owner_id, str(owner_id))

        op_id_raw = obj.get("operationID")
        op_name = ""
        use_op = False
        if op_id_raw is not None:
            op_id = int(op_id_raw)
            op_name, use_op = operations.get(op_id, (str(op_id), False))

        # CCP naming rule:
        #  - if useOperationName: <orbitName> - <corporationName> <operationName>
        #  - else:              <orbitName> - <corporationName>
        station_name = f"{orbit_name} - {corp}".strip()
        if use_op and op_name:
            station_name = f"{station_name} {op_name}".strip()

        rows.append(
            {
                "stationID": station_id,
                "stationName": station_name,
                "solarSystem": ss_name,
            }
        )

    rows.sort(key=lambda r: r["stationID"])
    return rows


def build_stargates_out(zf: zipfile.ZipFile, systems: Dict[int, Tuple[str, int, int]]) -> List[Dict]:
    rows = []
    for obj in iter_jsonl_from_zip(zf, "mapStargates.jsonl"):
        gid = int(obj.get("_key"))
        src_id = int(obj.get("solarSystemID"))
        dest = obj.get("destination") or {}
        dst_id = int(dest.get("solarSystemID"))

        src_name = systems.get(src_id, (str(src_id), 0, 0))[0]
        dst_name = systems.get(dst_id, (str(dst_id), 0, 0))[0]

        stargate_name = f"{src_name} → {dst_name}"

        lo_id, hi_id = (src_id, dst_id) if src_id <= dst_id else (dst_id, src_id)
        lo_name = systems.get(lo_id, (str(lo_id), 0, 0))[0]
        hi_name = systems.get(hi_id, (str(hi_id), 0, 0))[0]
        stargate_group = f"{lo_name} ↔ {hi_name}"

        rows.append(
            {
                "stargateID": gid,
                "stargateName": stargate_name,
                "stargateGroup": stargate_group,
                "solarSystem": src_name,
            }
        )

    rows.sort(key=lambda r: r["stargateID"])
    return rows


def _read_group_names(zf: zipfile.ZipFile) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "groups.jsonl"):
        gid = int(obj.get("_key"))
        out[gid] = safe_en_name(obj, fallback=str(gid))
    return out


def _read_contraband_set(zf: zipfile.ZipFile) -> Set[int]:
    ids: Set[int] = set()
    for obj in iter_jsonl_from_zip(zf, "contrabandTypes.jsonl"):
        ids.add(int(obj.get("_key")))
    return ids


def build_types_out(zf: zipfile.ZipFile) -> List[Dict]:
    group_names = _read_group_names(zf)
    contraband = _read_contraband_set(zf)

    rows: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        tid = int(obj.get("_key"))
        tname = safe_en_name(obj, fallback=str(tid))

        # SDE uses group_id in JSONL (per schema references) but be tolerant.
        group_id = obj.get("group_id")
        if group_id is None:
            group_id = obj.get("groupID")
        gid = int(group_id) if group_id is not None else -1
        gname = group_names.get(gid, "")

        rows.append(
            {
                "typeID": tid,
                "type": tname,
                "is_contraband": tid in contraband,
                "is_gategank": gname == "Smart Bomb",
            }
        )

    rows.sort(key=lambda r: r["typeID"])
    return rows


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

        # Stations: requires orbits + corp/ops
        planet_orbits = _read_planet_orbit_names(zf, systems)
        moon_orbits = _read_moon_orbit_names(zf, planet_orbits)
        corp_names = _read_corporations(zf)
        operations = _read_station_operations(zf)

        write_jsonl_gz(
            out_dir / "stations.jsonl.gz",
            build_stations_out(zf, systems, corp_names, operations, planet_orbits, moon_orbits),
        )

        # Stargates
        write_jsonl_gz(out_dir / "stargates.jsonl.gz", build_stargates_out(zf, systems))

        # Types
        write_jsonl_gz(out_dir / "types.jsonl.gz", build_types_out(zf))

    # Minimal sanity check: ensure files exist and are non-empty.
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
