#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integrated runner for:
- build routes.jsonl.gz
- compute/write etags + schedule outputs
- update Google Sheets (via WebApp URL)

Subcommands:
  meta        -> compute etags, compare with states/routes.json, write GitHub outputs (GITHUB_OUTPUT)
  write-state -> compute etags and write states/routes.json
  sheets-update -> update Sheets row (status, next_run, optional last_modified)
  build       -> generate data/sde/routes.jsonl.gz

Notes:
- No external deps (std-lib only).
- Output schema includes new "solarSystemClass" between "solarSystem" and "routeType".
- routExpanded excludes the origin solarSystem for non-destination rows, per requirement.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import heapq
import io
import json
import math
import os
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# -----------------------
# Shared constants
# -----------------------

LOWSEC_THRESHOLD = 0.45

MAX_CYNO_DIST_M = 94_600_000_000_000_000  # m

# (Kept as in current build script you provided)
ISO_NUM = 2350
ISO_DEN = 9_460_000_000_000_000

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"

# Fixed destination station in Jita (as in your current script)
DEST_STATION_ID = 60003760
DEST_STATION_NAME = "Jita IV - Moon 4 - Caldari Navy"

# Sheets serial epoch (Google Sheets date serial)
SHEETS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


# -----------------------
# Utilities: GitHub Outputs
# -----------------------

def _set_github_output(key: str, value: str) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        return
    with open(out_path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def _sheets_serial(dt: datetime) -> str:
    delta = dt - SHEETS_EPOCH
    return str(delta.total_seconds() / 86400.0)


# -----------------------
# Utilities: Etags (SHA256)
# -----------------------

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_state_etags(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    with open(path, "rt", encoding="utf-8") as f:
        obj = json.load(f)
    et = obj.get("etag", {})
    if not isinstance(et, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in et.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def _write_state_etags(path: str, etags: Dict[str, str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    obj = {"etag": etags}
    tmp = path + ".tmp"
    with open(tmp, "wt", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


# -----------------------
# Utilities: Sheets update (WebApp)
# -----------------------

def _post_json(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "gh-actions-sde-routes/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Sheets update failed HTTP {resp.status}: {body}")


# -----------------------
# SDE loading (jsonl.gz + txt)
# -----------------------

@dataclass(frozen=True)
class System:
    system_id: int
    name: str
    sec: float
    x: float
    y: float
    z: float
    faction: Optional[str]
    cyno_jump_security: str
    region: str
    system_type: Optional[str]  # optional (wormhole detection best-effort)


@dataclass(frozen=True)
class Station:
    station_id: int
    name: str
    system_name: str
    cyno_dock_security: str


CYNO_GRADE_RANK: Dict[str, int] = {
    "no jump": 0,
    "unsafe": 1,
    "risky": 2,
    "safe": 3,
}
CYNO_RANK_GRADE = {v: k for k, v in CYNO_GRADE_RANK.items()}


def norm_cyno_grade(x: str) -> str:
    x = (x or "").strip().lower()
    if x in CYNO_GRADE_RANK:
        return x
    if x in ("nojump", "no_jump", "none"):
        return "no jump"
    return "no jump"


def read_jsonl_gz(path: str) -> Iterable[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_systems(solarsystems_gz: str) -> Tuple[Dict[int, System], Dict[str, int]]:
    by_id: Dict[int, System] = {}
    name_to_id: Dict[str, int] = {}

    for row in read_jsonl_gz(solarsystems_gz):
        sid = int(row["solarSystemID"])
        name = str(row["solarSystem"]).strip()
        sec = float(row["securityStatus"])
        pos = row.get("position") or {}
        x = float(pos.get("x", 0.0))
        y = float(pos.get("y", 0.0))
        z = float(pos.get("z", 0.0))
        faction = row.get("faction", None)
        cyno = norm_cyno_grade(str(row.get("cynoJumpSecurity", "no jump")))
        region = str(row.get("region", "") or "").strip()

        # Best-effort wormhole detection: depends on your dataset fields.
        system_type = None
        for k in ("solarSystemType", "systemType", "type"):
            if isinstance(row.get(k), str) and row.get(k).strip():
                system_type = row.get(k).strip()
                break
        # also support boolean-like "isWormhole"
        if system_type is None and row.get("isWormhole") is True:
            system_type = "wormhole"

        s = System(
            system_id=sid,
            name=name,
            sec=sec,
            x=x,
            y=y,
            z=z,
            faction=faction if faction is not None else None,
            cyno_jump_security=cyno,
            region=region,
            system_type=system_type,
        )
        by_id[sid] = s
        name_to_id[name] = sid

    return by_id, name_to_id


def load_stations(stations_gz: str) -> Tuple[Dict[str, List[Station]], Dict[int, Station]]:
    by_sys: Dict[str, List[Station]] = {}
    by_id: Dict[int, Station] = {}

    for row in read_jsonl_gz(stations_gz):
        sid = int(row["stationID"])
        name = str(row["station"]).strip()
        sysname = str(row["solarSystem"]).strip()
        cds = norm_cyno_grade(str(row.get("cynoDockSecurity", "no jump")))
        st = Station(station_id=sid, name=name, system_name=sysname, cyno_dock_security=cds)

        by_sys.setdefault(sysname, []).append(st)
        by_id[sid] = st

    for sysname in by_sys:
        by_sys[sysname].sort(key=lambda s: s.station_id)

    return by_sys, by_id


def compute_system_cyno_from_stations(
    systems_by_id: Dict[int, System],
    stations_by_sysname: Dict[str, List[Station]],
) -> Dict[int, str]:
    """
    cynoJumpSecurity del sistema = máximo grade entre sus estaciones.
    Si no hay estaciones, se conserva el grade del sistema.
    """
    out: Dict[int, str] = {}
    for sid, s in systems_by_id.items():
        stations = stations_by_sysname.get(s.name, [])
        if not stations:
            out[sid] = s.cyno_jump_security
            continue

        best_rank = 0
        for st in stations:
            r = CYNO_GRADE_RANK[norm_cyno_grade(st.cyno_dock_security)]
            if r > best_rank:
                best_rank = r

        out[sid] = CYNO_RANK_GRADE.get(best_rank, "no jump")

    return out


def choose_station_for_system(
    system_name: str,
    system_cyno_grade: str,
    stations_by_sysname: Dict[str, List[Station]],
) -> str:
    """
    Escoge estación waypoint para un sistema:
    - la que tenga cynoDockSecurity == cynoJumpSecurity del sistema,
    - empate => menor stationID,
    - si no hay estaciones => fallback al nombre del sistema.
    """
    sts = stations_by_sysname.get(system_name, [])
    if not sts:
        return system_name

    target = norm_cyno_grade(system_cyno_grade)
    for st in sts:
        if norm_cyno_grade(st.cyno_dock_security) == target:
            return st.name

    return sts[0].name


def precompute_station_name_by_system_id(
    systems: Dict[int, System],
    system_cyno_grade: Dict[int, str],
    stations_by_sysname: Dict[str, List[Station]],
    dest_system_id: int,
    dest_station_name: str,
) -> Dict[int, str]:
    """
    Precalcula el nombre de estación (waypoint) para cada system_id,
    forzando el destino final a la estación Caldari Navy.
    """
    out: Dict[int, str] = {}
    for sid, s in systems.items():
        if sid == dest_system_id:
            out[sid] = dest_station_name
        else:
            grade = system_cyno_grade.get(sid, s.cyno_jump_security)
            out[sid] = choose_station_for_system(s.name, grade, stations_by_sysname)
    return out


def load_ganksystems_ids_txt(ganksystems_txt: str, name_to_id: Dict[str, int]) -> Set[int]:
    out: Set[int] = set()
    with open(ganksystems_txt, "rt", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                sid = name_to_id.get(s)
                if sid is not None:
                    out.add(sid)
    return out


# -----------------------
# Stargates graph
# -----------------------

def _split_stargate_group(group: str) -> Optional[Tuple[str, str]]:
    if not group:
        return None
    if "↔" in group:
        parts = [p.strip() for p in group.split("↔")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    if "<->" in group:
        parts = [p.strip() for p in group.split("<->")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    return None


def _split_stargate_arrow(name: str) -> Optional[Tuple[str, str]]:
    if not name:
        return None
    if "→" in name:
        parts = [p.strip() for p in name.split("→")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    if "->" in name:
        parts = [p.strip() for p in name.split("->")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    return None


def load_stargates_graph(stargates_gz: str, name_to_id: Dict[str, int]) -> Dict[int, List[int]]:
    """
    Construye grafo no dirigido de stargates.
    """
    adj: Dict[int, Set[int]] = {}
    for row in read_jsonl_gz(stargates_gz):
        group = str(row.get("stargateGroup", "")).strip()
        pair = _split_stargate_group(group)
        if pair is None:
            sname = str(row.get("stargate", "")).strip()
            pair = _split_stargate_arrow(sname)
        if pair is None:
            continue

        a_name, b_name = pair
        a_id = name_to_id.get(a_name)
        b_id = name_to_id.get(b_name)
        if a_id is None or b_id is None:
            continue

        adj.setdefault(a_id, set()).add(b_id)
        adj.setdefault(b_id, set()).add(a_id)

    return {k: sorted(vs) for k, vs in adj.items()}


# -----------------------
# routeSDEsafe100 (Safer+100) base flags
# -----------------------

def safer_cost(sec_to: float, penalty_cost: float) -> float:
    if sec_to <= 0.0:
        return 2.0 * penalty_cost
    if sec_to < LOWSEC_THRESHOLD:
        return penalty_cost
    return 0.90


def dijkstra_route_sde_safer100(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    dest_id: int,
) -> Dict[int, int]:
    penalty_cost = math.exp(0.15 * 100.0)

    INF = float("inf")
    dist: Dict[int, Tuple[float, int]] = {dest_id: (0.0, 0)}
    next_hop: Dict[int, int] = {}

    heap: List[Tuple[float, int, int]] = [(0.0, 0, dest_id)]
    heapq.heapify(heap)

    while heap:
        cost_u, gates_u, u = heapq.heappop(heap)
        cur = dist.get(u)
        if cur is None or cur[0] != cost_u or cur[1] != gates_u:
            continue

        for p in gate_adj.get(u, []):
            inc = safer_cost(systems[u].sec, penalty_cost)
            cand = (cost_u + inc, gates_u + 1)
            old = dist.get(p, (INF, 10**18))
            if cand < old or (cand == old and u < next_hop.get(p, 2**31 - 1)):
                dist[p] = cand
                next_hop[p] = u
                heapq.heappush(heap, (cand[0], cand[1], p))

    return next_hop


def compute_base_flags(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
) -> Tuple[Dict[int, bool], Dict[int, int]]:
    """
    Para cada origen:
    - base_has_lowsec: si la ruta Safer+100 pasa por algún sistema sec<0.45 (excl. origen/dest)
    - base_min_id: min solarSystemID de la ruta base (excl. origen/dest), INF si no hay
    Iterativo (sin recursión).
    """
    INF_ID = 2**31 - 1
    has_low: Dict[int, bool] = {}
    min_id: Dict[int, int] = {}

    for start in systems.keys():
        if start in has_low:
            continue

        path: List[int] = []
        cur = start
        seen_local: Set[int] = set()

        while True:
            if cur == dest_id:
                has_low[cur] = False
                min_id[cur] = INF_ID
                break
            if cur in has_low:
                break
            if cur in seen_local:
                has_low[cur] = False
                min_id[cur] = INF_ID
                break

            seen_local.add(cur)
            path.append(cur)

            nxt = base_next.get(cur)
            if nxt is None:
                has_low[cur] = False
                min_id[cur] = INF_ID
                break

            cur = nxt

        for n in reversed(path):
            if n in has_low:
                continue
            nxt = base_next.get(n)
            if nxt is None or nxt not in has_low:
                has_low[n] = False
                min_id[n] = INF_ID
                continue

            low_here = has_low[nxt]
            min_here = min_id[nxt]

            if nxt != dest_id:
                if systems[nxt].sec < LOWSEC_THRESHOLD:
                    low_here = True
                if nxt < min_here:
                    min_here = nxt

            has_low[n] = low_here
            min_id[n] = min_here

    for sid in systems.keys():
        has_low.setdefault(sid, False)
        min_id.setdefault(sid, INF_ID)

    return has_low, min_id


# -----------------------
# Type sets for stargate rules (your current logic)
# -----------------------

def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def is_nl(sec: float) -> bool:
    return sec < LOWSEC_THRESHOLD


def build_type_sets(
    systems: Dict[int, System],
    system_cyno_grade: Dict[int, str],
    gate_adj: Dict[int, List[int]],
    gank_ids: Set[int],
    base_has_lowsec: Dict[int, bool],
) -> Dict[str, Set[int]]:
    has_gate: Set[int] = set(gate_adj.keys())

    S: Set[int] = set()
    NL: Set[int] = set()
    Hg: Set[int] = set()
    Lg: Set[int] = set()
    LD: Set[int] = set()
    LDg: Set[int] = set()
    I: Set[int] = set()

    for sid, s in systems.items():
        if sid in has_gate and s.sec <= 1.0:
            S.add(sid)

        if is_nl(s.sec):
            NL.add(sid)

        if sid in has_gate and s.sec >= LOWSEC_THRESHOLD and sid not in gank_ids:
            Hg.add(sid)

        if is_lowsec(s.sec) and sid not in gank_ids:
            Lg.add(sid)

        cj = norm_cyno_grade(system_cyno_grade.get(sid, s.cyno_jump_security))
        if is_lowsec(s.sec) and cj in ("safe", "risky"):
            LD.add(sid)
            if sid not in gank_ids:
                LDg.add(sid)

        if s.sec >= LOWSEC_THRESHOLD and base_has_lowsec.get(sid, False):
            I.add(sid)

    return {
        "has_gate": has_gate,
        "S": S,
        "NL": NL,
        "Hg": Hg,
        "Lg": Lg,
        "LD": LD,
        "LDg": LDg,
        "I": I,
    }


# -----------------------
# Cyno reverse edges (grid), as your current script
# -----------------------

def fuel_for_distance_m(dist_m: float) -> int:
    f = int(math.ceil(dist_m * ISO_NUM / ISO_DEN))
    return 1 if f < 1 else f


def build_reverse_cyno_edges_grid_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
    system_cyno_grade: Dict[int, str],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    rev[dest] = [(origin, fuel, dest_is_risky), ...]
    Optimizado con grid cúbico de tamaño MAX_CYNO_DIST_M para reducir comparaciones.
    Regla fuerte: origen de cynoJump debe tener sec < 0.45. (Se mantiene tal como tu código actual.)
    """
    dests: List[Tuple[int, float, float, float, bool]] = []
    for did in LD:
        d = systems[did]
        dest_grade = norm_cyno_grade(system_cyno_grade.get(did, d.cyno_jump_security))
        dests.append((did, d.x, d.y, d.z, dest_grade == "risky"))

    cell = float(MAX_CYNO_DIST_M)
    r2 = cell * cell

    def cell_key(x: float, y: float, z: float) -> Tuple[int, int, int]:
        return (int(math.floor(x / cell)), int(math.floor(y / cell)), int(math.floor(z / cell)))

    grid: Dict[Tuple[int, int, int], List[Tuple[int, float, float, float, bool]]] = {}
    for did, x, y, z, is_risky in dests:
        grid.setdefault(cell_key(x, y, z), []).append((did, x, y, z, is_risky))

    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in dests}

    for o in systems.values():
        if o.sec >= LOWSEC_THRESHOLD:
            continue

        ok = cell_key(o.x, o.y, o.z)
        ox, oy, oz = o.x, o.y, o.z
        oid = o.system_id

        cx, cy, cz = ok
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    bucket = grid.get((cx + dx, cy + dy, cz + dz))
                    if not bucket:
                        continue
                    for did, tx, ty, tz, dest_is_risky in bucket:
                        if did == oid:
                            continue
                        ddx = ox - tx
                        ddy = oy - ty
                        ddz = oz - tz
                        d2 = ddx * ddx + ddy * ddy + ddz * ddz
                        if d2 > r2:
                            continue
                        fuel = fuel_for_distance_m(math.sqrt(d2))
                        rev[did].append((oid, fuel, dest_is_risky))

    for did in rev:
        rev[did].sort(key=lambda t: (t[0], t[1], t[2]))
    return rev


# -----------------------
# Dijkstra final multi-criterio (your current ordering)
# -----------------------

Cost = Tuple[int, int, int, int, int, float, int, int, int]
# (cynoJumps, fuel, risky_present, low2high, gank_hi_entries, neg_minGateSec, stargates, intermediates, base_min_id)


def dijkstra_final_routes(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    rev_cyno: Dict[int, List[Tuple[int, int, bool]]],
    dest_id: int,
    type_sets: Dict[str, Set[int]],
    gank_ids: Set[int],
    base_min_id: Dict[int, int],
) -> Tuple[Dict[int, Cost], Dict[int, Tuple[int, str, int]]]:
    has_gate = type_sets["has_gate"]
    S = type_sets["S"]
    NL = type_sets["NL"]
    I = type_sets["I"]
    LDg = type_sets["LDg"]
    Lg = type_sets["Lg"]
    Hg = type_sets["Hg"]

    INF_INT = 10**18
    INF_FLOAT = float("inf")
    INF_ID = 2**31 - 1

    def inf_cost() -> Cost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT, INF_ID)

    def gate_allowed(p: int, u: int) -> bool:
        if p not in has_gate or u not in has_gate:
            return False

        # S -> NL allowed only if p in I and u in LDg
        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        # NL -> S allowed only if p in Lg and u in Hg
        if (p in NL) and (u in S):
            return (p in Lg) and (u in Hg)

        su = systems[u]
        if su.sec <= 0.0:
            return False
        if su.sec < LOWSEC_THRESHOLD and u in gank_ids:
            return False

        return True

    best: Dict[int, Cost] = {}
    nxt_step: Dict[int, Tuple[int, str, int]] = {}

    start: Cost = (0, 0, 0, 0, 0, -1.0, 0, 0, base_min_id.get(dest_id, INF_ID))
    best[dest_id] = start

    heap: List[Tuple[Cost, int]] = [(start, dest_id)]
    heapq.heapify(heap)

    while heap:
        cost_u, u = heapq.heappop(heap)
        if best.get(u) != cost_u:
            continue

        # Stargate predecessors (p -> u)
        for p in gate_adj.get(u, []):
            if not gate_allowed(p, u):
                continue

            sec_p = systems[p].sec
            sec_u = systems[u].sec

            cyno_j, fuel, risky_present, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u

            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            low2high2 = low2high + (1 if (sec_p < LOWSEC_THRESHOLD and sec_u >= LOWSEC_THRESHOLD) else 0)
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (sec_u >= LOWSEC_THRESHOLD)) else 0)

            neg_min2 = max(neg_min, -round(sec_u, 6))
            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (
                cyno_j,
                fuel,
                risky_present,
                low2high2,
                gank_hi2,
                neg_min2,
                gates2,
                inter2,
                bm_p,
            )
            old = best.get(p, inf_cost())
            if cand < old or (cand == old and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # Cyno predecessors (p -> u)
        for (p, fuel_edge, dest_is_risky) in rev_cyno.get(u, []):
            cyno_j, fuel, risky_present, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u
            bm_p = base_min_id.get(p, INF_ID)

            risky_present2 = 1 if (risky_present == 1 or dest_is_risky) else 0

            cand: Cost = (
                cyno_j + 1,
                fuel + fuel_edge,
                risky_present2,
                low2high,
                gank_hi,
                neg_min,
                gates,
                inter + 1,
                bm_p,
            )
            old = best.get(p, inf_cost())
            if cand < old or (cand == old and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_CYNO, fuel_edge)
                heapq.heappush(heap, (cand, p))

    return best, nxt_step


# -----------------------
# Route reconstruction + compact + expanded
# -----------------------

def reconstruct_raw_edges(
    dest_id: int,
    origin_id: int,
    nxt_step: Dict[int, Tuple[int, str, int]],
) -> List[Tuple[str, int, int]]:
    if origin_id == dest_id:
        return []
    raw: List[Tuple[str, int, int]] = []
    cur = origin_id
    seen: Set[int] = set()
    while cur != dest_id:
        if cur in seen:
            return []
        seen.add(cur)
        ns = nxt_step.get(cur)
        if ns is None:
            return []
        nxt, etype, meta = ns
        raw.append((etype, nxt, meta))
        cur = nxt
    return raw


def make_compact_route_with_stations(
    station_name_by_system_id: Dict[int, str],
    raw_edges: List[Tuple[str, int, int]],
) -> List[List[Any]]:
    out: List[List[Any]] = []
    gate_count = 0
    gate_last: Optional[int] = None

    def flush_gates() -> None:
        nonlocal gate_count, gate_last
        if gate_count > 0 and gate_last is not None:
            out.append([EDGE_STARGATE, station_name_by_system_id[gate_last], gate_count])
        gate_count = 0
        gate_last = None

    for etype, nxt, meta in raw_edges:
        if etype == EDGE_STARGATE:
            gate_count += meta
            gate_last = nxt
        else:
            flush_gates()
            out.append([EDGE_CYNO, station_name_by_system_id[nxt], meta])

    flush_gates()
    return out


def make_route_expanded(
    systems: Dict[int, System],
    station_name_by_system_id: Dict[int, str],
    origin_id: int,
    raw_edges: List[Tuple[str, int, int]],
) -> List[str]:
    """
    Antes: empezaba por el sistema de origen.
    Ahora: NO incluye el sistema de origen (salvo el caso "destino=Jita" que ya se gestiona aparte).
    """
    expanded: List[str] = [systems[origin_id].name]  # construir igual que antes y luego cortar [1:]

    i = 0
    while i < len(raw_edges):
        etype, nxt, _meta = raw_edges[i]

        if etype == EDGE_CYNO:
            expanded.append(station_name_by_system_id[nxt])
            i += 1
            continue

        j = i
        run_nodes: List[int] = []
        while j < len(raw_edges) and raw_edges[j][0] == EDGE_STARGATE:
            run_nodes.append(raw_edges[j][1])
            j += 1

        if run_nodes:
            for k, sid in enumerate(run_nodes):
                if k < len(run_nodes) - 1:
                    expanded.append(systems[sid].name)
                else:
                    expanded.append(station_name_by_system_id[sid])

        i = j

    # drop origin
    return expanded[1:]


# -----------------------
# routeType helpers (unchanged)
# -----------------------

_ROMAN_MAP = [
    (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
    (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
    (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
]


def to_roman(n: int) -> str:
    out = []
    x = n
    for v, sym in _ROMAN_MAP:
        while x >= v:
            out.append(sym)
            x -= v
    return "".join(out)


def cyno_run_signature(compact_route: List[List[Any]]) -> str:
    runs: List[int] = []
    i = 0
    while i < len(compact_route):
        if compact_route[i][0] != EDGE_CYNO:
            i += 1
            continue
        j = i
        while j < len(compact_route) and compact_route[j][0] == EDGE_CYNO:
            j += 1
        runs.append(j - i)
        i = j
    if not runs:
        return ""
    return "-".join(to_roman(r) for r in runs)


def normalize_roman_for_route_type(roman: str) -> str:
    return "" if roman == "I" else roman


def build_route_type(
    *,
    has_route: bool,
    origin_is_jita: bool,
    compact_route: List[List[Any]],
    cyno_risky_count: int,
    stargates_lowsec_count: int,
    stargates_ganksec_count: int,
    stargates_total: int,
) -> str:
    if not has_route:
        return "no route"
    if origin_is_jita:
        return "highway 0"

    has_cyno = any(step[0] == EDGE_CYNO for step in compact_route)

    if not has_cyno:
        base = "highway"
        roman = ""
    else:
        first = compact_route[0][0] if compact_route else EDGE_STARGATE
        base = "spaceport" if first == EDGE_CYNO else "island"
        roman = normalize_roman_for_route_type(cyno_run_signature(compact_route))

    prefixes: List[str] = []
    if has_cyno and cyno_risky_count > 0:
        prefixes.append("risky")

    if stargates_lowsec_count > 0:
        prefixes.append("red")
    elif stargates_ganksec_count > 0:
        prefixes.append("yellow")

    parts: List[str] = []
    parts.extend(prefixes)
    parts.append(base)
    if roman:
        parts.append(roman)
    parts.append(str(int(stargates_total)))
    return " ".join(parts)


# -----------------------
# solarSystemClass (NEW)
# -----------------------

def compute_solar_system_class(
    s: System,
    *,
    in_gank: bool,
    has_gate: bool,
    base_has_lowsec: bool,
) -> str:
    # Order matters (most specific first)

    # Region-based special cases
    if s.region == "Pochven":
        return "pochven"
    if s.region == "Yasna Zakh":
        return "zarzakh"
    if s.region in ("A821-A", "J7HZ-F", "UUA-F4"):
        return "jove"

    # Wormhole best-effort
    if (s.system_type or "").strip().lower() == "wormhole":
        return "wormhole"

    # Campsec: sec < 0.45 AND gank-listed
    if s.sec < LOWSEC_THRESHOLD and in_gank:
        return "campsec"

    # Ganksec: sec >= 0.45 AND gank-listed AND in stargate graph ("en la ruta de stargates" => has_gate)
    if s.sec >= LOWSEC_THRESHOLD and in_gank and has_gate:
        return "ganksec"

    # Nullsec / <= 0 (non-gank)
    if s.sec <= 0.0 and not in_gank:
        if s.faction is not None:
            return "npcnull"
        return "sovnull"

    # Lowsec (non-gank)
    if 0.0 < s.sec < LOWSEC_THRESHOLD and not in_gank:
        return "lowsec"

    # hisland / midsland (non-gank) depend on routeSDEsafe100 including lowsec
    if s.sec >= 0.65 and not in_gank and base_has_lowsec:
        return "hisland"
    if (LOWSEC_THRESHOLD <= s.sec < 0.65) and not in_gank and base_has_lowsec:
        return "midsland"

    # hisec / midsec (non-gank)
    if s.sec >= 0.65 and not in_gank:
        return "hisec"
    if (LOWSEC_THRESHOLD <= s.sec < 0.65) and not in_gank:
        return "midsec"

    return "unknown"


# -----------------------
# Writer gzip determinista
# -----------------------

def write_jsonl_gz_atomic(path: str, rows: Iterable[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
    except OSError:
        pass

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="routes.jsonl", fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


# -----------------------
# Command implementations
# -----------------------

def cmd_meta(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    lock_dt = now + timedelta(seconds=int(args.lock_seconds))
    plus_6h = now + timedelta(hours=6)
    plus_10m = now + timedelta(minutes=10)

    etags: Dict[str, str] = {}
    for p in args.inputs:
        if not os.path.exists(p):
            raise SystemExit(f"Missing input file: {p}")
        etags[p] = _sha256_file(p)

    old = _load_state_etags(args.state_file)
    inputs_changed = (set(old.keys()) != set(etags.keys())) or any(old.get(k) != v for k, v in etags.items())

    _set_github_output("inputs_changed", "true" if inputs_changed else "false")
    _set_github_output("now_serial", _sheets_serial(now))
    _set_github_output("next_run_lock_serial", _sheets_serial(lock_dt))
    _set_github_output("next_run_6h_serial", _sheets_serial(plus_6h))
    _set_github_output("next_run_10m_serial", _sheets_serial(plus_10m))
    return 0


def cmd_write_state(args: argparse.Namespace) -> int:
    etags: Dict[str, str] = {}
    for p in args.inputs:
        if not os.path.exists(p):
            raise SystemExit(f"Missing input file: {p}")
        etags[p] = _sha256_file(p)
    _write_state_etags(args.state_file, etags)
    return 0


def cmd_sheets_update(args: argparse.Namespace) -> int:
    payload = {
        "token": args.token,
        "sheets_id": args.sheets_id,
        "tab": args.tab,
        "row": int(args.row),
        "status": args.status,
        "next_run": float(args.next_run_serial),
        "last_modified": (float(args.last_modified_serial) if args.last_modified_serial is not None else None),
    }
    _post_json(args.webapp_url, payload)
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    systems, name_to_id = load_systems(args.solarsystems)
    stations_by_sysname, stations_by_id = load_stations(args.stations)
    system_cyno_grade = compute_system_cyno_from_stations(systems, stations_by_sysname)

    dest_station = stations_by_id.get(DEST_STATION_ID)
    if dest_station is None:
        raise SystemExit(f"Destination stationID {DEST_STATION_ID} not found in stations input.")

    dest_station_name = dest_station.name
    dest_system_name = dest_station.system_name

    dest_id = name_to_id.get(dest_system_name)
    if dest_id is None:
        raise SystemExit(f"Destination system '{dest_system_name}' not found in solarsystems input.")

    station_name_by_system_id = precompute_station_name_by_system_id(
        systems=systems,
        system_cyno_grade=system_cyno_grade,
        stations_by_sysname=stations_by_sysname,
        dest_system_id=dest_id,
        dest_station_name=dest_station_name,
    )

    gate_adj = load_stargates_graph(args.stargates, name_to_id)
    origin_ids = sorted(gate_adj.keys())
    has_gate_set = set(gate_adj.keys())

    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)

    base_next = dijkstra_route_sde_safer100(systems, gate_adj, dest_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, dest_id)

    type_sets = build_type_sets(
        systems=systems,
        system_cyno_grade=system_cyno_grade,
        gate_adj=gate_adj,
        gank_ids=gank_ids,
        base_has_lowsec=base_has_lowsec,
    )

    rev_cyno = build_reverse_cyno_edges_grid_LD_only(
        systems=systems,
        LD=type_sets["LD"],
        system_cyno_grade=system_cyno_grade,
    )

    best, nxt = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=dest_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
    )

    def compute_counts_from_raw_edges(raw_edges: List[Tuple[str, int, int]]) -> Tuple[int, int, int, int, int, int]:
        safe_c = 0
        risky_c = 0
        hisec = midsec = ganksec = lowsec = 0

        for etype, did, _meta in raw_edges:
            if etype == EDGE_CYNO:
                grade = norm_cyno_grade(system_cyno_grade.get(did, systems[did].cyno_jump_security))
                if grade == "risky":
                    risky_c += 1
                elif grade == "safe":
                    safe_c += 1
                continue

            s2 = systems[did]
            if 0.0 < s2.sec < LOWSEC_THRESHOLD:
                if did not in gank_ids:
                    lowsec += 1
            elif s2.sec >= LOWSEC_THRESHOLD:
                if did in gank_ids:
                    ganksec += 1
                else:
                    if s2.sec >= 0.65:
                        hisec += 1
                    else:
                        midsec += 1

        return safe_c, risky_c, hisec, midsec, ganksec, lowsec

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]

        solar_class = compute_solar_system_class(
            o,
            in_gank=(oid in gank_ids),
            has_gate=(oid in has_gate_set),
            base_has_lowsec=base_has_lowsec.get(oid, False),
        )

        if oid == dest_id:
            # Jita row remains as you consider correct: routExpanded only station
            return {
                "solarSystem": o.name,
                "solarSystemClass": solar_class,
                "routeType": "highway 0",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [dest_station_name],
            }

        if oid not in best:
            return {
                "solarSystem": o.name,
                "solarSystemClass": solar_class,
                "routeType": "no route",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [],
            }

        raw_edges = reconstruct_raw_edges(dest_id=dest_id, origin_id=oid, nxt_step=nxt)

        compact_route = make_compact_route_with_stations(
            station_name_by_system_id=station_name_by_system_id,
            raw_edges=raw_edges,
        )

        expanded = make_route_expanded(
            systems=systems,
            station_name_by_system_id=station_name_by_system_id,
            origin_id=oid,
            raw_edges=raw_edges,
        )

        cyno_count, fuel, _risky_present, _low2high, _gank_hi, _neg_min, st_total, _inter, _bm = best[oid]

        # jumpFuel se dobla globalmente; fuel por cynoJump en "route" se mantiene real
        jump_fuel = int(fuel) * 2

        safe_c, risky_c, hisec, midsec, ganksec, lowsec = compute_counts_from_raw_edges(raw_edges)

        st_obj = {
            "count": int(st_total),
            "hisec": int(hisec),
            "midsec": int(midsec),
            "ganksec": int(ganksec),
            "lowsec": int(lowsec),
        }
        cj_obj = {
            "count": int(cyno_count),
            "safe": int(safe_c),
            "risky": int(risky_c),
        }

        rt = build_route_type(
            has_route=True,
            origin_is_jita=False,
            compact_route=compact_route,
            cyno_risky_count=risky_c,
            stargates_lowsec_count=lowsec,
            stargates_ganksec_count=ganksec,
            stargates_total=int(st_total),
        )

        return {
            "solarSystem": o.name,
            "solarSystemClass": solar_class,
            "routeType": rt,
            "jumpFuel": int(jump_fuel),
            "cynoJumps": cj_obj,
            "stargates": st_obj,
            "route": compact_route,
            "routExpanded": expanded,
        }

    out_path = args.out
    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except OSError:
        pass

    write_jsonl_gz_atomic(out_path, (row_for_origin(oid) for oid in origin_ids))
    return 0


# -----------------------
# CLI
# -----------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="eou_sde_routes_sde-gh.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_meta = sub.add_parser("meta")
    p_meta.add_argument("--state-file", required=True)
    p_meta.add_argument("--lock-seconds", required=True, type=int)
    p_meta.add_argument("--inputs", nargs="+", required=True)
    p_meta.set_defaults(func=cmd_meta)

    p_ws = sub.add_parser("write-state")
    p_ws.add_argument("--state-file", required=True)
    p_ws.add_argument("--inputs", nargs="+", required=True)
    p_ws.set_defaults(func=cmd_write_state)

    p_su = sub.add_parser("sheets-update")
    p_su.add_argument("--webapp-url", required=True)
    p_su.add_argument("--token", required=True)
    p_su.add_argument("--sheets-id", required=True)
    p_su.add_argument("--tab", required=True)
    p_su.add_argument("--row", required=True)
    p_su.add_argument("--status", required=True)
    p_su.add_argument("--next-run-serial", required=True)
    p_su.add_argument("--last-modified-serial", default=None)
    p_su.set_defaults(func=cmd_sheets_update)

    p_b = sub.add_parser("build")
    p_b.add_argument("--solarsystems", required=True)
    p_b.add_argument("--stargates", required=True)
    p_b.add_argument("--stations", required=True)
    p_b.add_argument("--ganksystems", required=True)
    p_b.add_argument("--out", required=True)
    p_b.set_defaults(func=cmd_build)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
