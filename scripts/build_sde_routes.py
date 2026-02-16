#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import os
import sys
import heapq
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable, Any, Set

LOWSEC_THRESHOLD = 0.45

MAX_CYNO_DIST_M = 94_600_000_000_000_000  # 94600000000000000 m

ISO_NUM = 2350
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"

# Security grade order for cynoDockSecurity / cynoJumpSecurity (max wins)
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


@dataclass(frozen=True)
class Station:
    station_id: int
    name: str
    system_name: str
    cyno_dock_security: str


def read_jsonl_gz(path: str) -> Iterable[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
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

        sysobj = System(
            system_id=sid,
            name=name,
            sec=sec,
            x=x,
            y=y,
            z=z,
            faction=faction if faction is not None else None,
            cyno_jump_security=cyno,
        )
        by_id[sid] = sysobj
        name_to_id[name] = sid

    return by_id, name_to_id


def load_stations(stations_gz: str) -> Dict[str, List[Station]]:
    out: Dict[str, List[Station]] = {}
    for row in read_jsonl_gz(stations_gz):
        sid = int(row["stationID"])
        name = str(row["station"]).strip()
        sysname = str(row["solarSystem"]).strip()
        cds = norm_cyno_grade(str(row.get("cynoDockSecurity", "no jump")))
        st = Station(station_id=sid, name=name, system_name=sysname, cyno_dock_security=cds)
        out.setdefault(sysname, []).append(st)

    for sysname in out:
        out[sysname].sort(key=lambda s: s.station_id)
    return out


def compute_system_cyno_from_stations(
    systems_by_id: Dict[int, System],
    stations_by_sysname: Dict[str, List[Station]],
) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for sid, s in systems_by_id.items():
        stations = stations_by_sysname.get(s.name, [])
        if not stations:
            out[sid] = s.cyno_jump_security
            continue
        best_rank = -1
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
    sts = stations_by_sysname.get(system_name, [])
    if not sts:
        return system_name  # fallback if no stations

    target = norm_cyno_grade(system_cyno_grade)
    for st in sts:
        if norm_cyno_grade(st.cyno_dock_security) == target:
            return st.name

    return sts[0].name


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


def load_ganksystems_ids_txt(ganksystems_txt: str, name_to_id: Dict[str, int]) -> Set[int]:
    out: Set[int] = set()
    with open(ganksystems_txt, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            sid = name_to_id.get(line)
            if sid is not None:
                out.add(sid)
    return out


# -----------------------------
# routeSDEsafe100 (stargate-only) - Safer imitation + security_penalty=100
# -----------------------------

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
    dist: Dict[int, Tuple[float, int]] = {dest_id: (0.0, 0)}  # (cost, gates)
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
    INF_ID = 2**31 - 1
    memo_low: Dict[int, bool] = {}
    memo_min: Dict[int, int] = {}
    sys.setrecursionlimit(20000)

    def dfs(n: int) -> Tuple[bool, int]:
        if n == dest_id:
            return (False, INF_ID)
        if n in memo_low:
            return (memo_low[n], memo_min[n])

        nxt = base_next.get(n)
        if nxt is None:
            memo_low[n] = False
            memo_min[n] = INF_ID
            return (False, INF_ID)

        child_low, child_min = dfs(nxt)
        low_here = child_low
        min_here = child_min

        if nxt != dest_id:
            if systems[nxt].sec < LOWSEC_THRESHOLD:
                low_here = True
            if nxt < min_here:
                min_here = nxt

        memo_low[n] = low_here
        memo_min[n] = min_here
        return (low_here, min_here)

    for sid in systems.keys():
        dfs(sid)

    return memo_low, memo_min


# -----------------------------
# Type sets (your gate rules)
# -----------------------------

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


# -----------------------------
# Final routing (your priority 1..9)
# -----------------------------
Cost = Tuple[int, int, int, int, int, float, int, int, int]
# (cynoJumps, fuel, risky_present, low2high, gank_hi_entries, neg_minGateSec, stargates, intermediates, base_min_id)


def build_reverse_cyno_edges_bruteforce_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
    system_cyno_grade: Dict[int, str],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    eligible = []
    for did in LD:
        d = systems[did]
        dest_grade = norm_cyno_grade(system_cyno_grade.get(did, d.cyno_jump_security))
        eligible.append((did, d.x, d.y, d.z, dest_grade == "risky"))

    r2 = float(MAX_CYNO_DIST_M) * float(MAX_CYNO_DIST_M)
    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in eligible}

    # STRONG RULE: origin sec must be < 0.45
    origins = [s for s in systems.values() if s.sec < LOWSEC_THRESHOLD]

    for origin in origins:
        ox, oy, oz = origin.x, origin.y, origin.z
        oid = origin.system_id

        for did, dx, dy, dz, dest_is_risky in eligible:
            if did == oid:
                continue
            ddx = ox - dx
            ddy = oy - dy
            ddz = oz - dz
            d2 = ddx * ddx + ddy * ddy + ddz * ddz
            if d2 > r2:
                continue

            dist_m = math.sqrt(d2)
            fuel = int(math.ceil(dist_m * ISO_NUM / ISO_DEN))
            if fuel < 1:
                fuel = 1

            rev[did].append((oid, fuel, dest_is_risky))

    for did, items in rev.items():
        items.sort(key=lambda t: (t[0], t[1], t[2]))
    return rev


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

        # [S] Ø [NL]  (unless [I] -> [LDg])
        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        # [NL] Ø [S]  (unless [Lg] -> [Hg])
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


# -----------------------------
# Path reconstruction + stations
# -----------------------------

def reconstruct_raw_edges(
    dest_id: int,
    origin_id: int,
    nxt_step: Dict[int, Tuple[int, str, int]],
) -> List[Tuple[str, int, int]]:
    if origin_id == dest_id:
        return []
    raw: List[Tuple[str, int, int]] = []
    cur = origin_id
    seen = set()
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
    systems: Dict[int, System],
    system_cyno_grade: Dict[int, str],
    stations_by_sysname: Dict[str, List[Station]],
    raw_edges: List[Tuple[str, int, int]],
) -> List[List[Any]]:
    out: List[List[Any]] = []
    gate_count = 0
    gate_last: Optional[int] = None

    def station_name_for_system_id(sid: int) -> str:
        sysname = systems[sid].name
        grade = system_cyno_grade.get(sid, systems[sid].cyno_jump_security)
        return choose_station_for_system(sysname, grade, stations_by_sysname)

    def flush_gates() -> None:
        nonlocal gate_count, gate_last
        if gate_count > 0 and gate_last is not None:
            out.append([EDGE_STARGATE, station_name_for_system_id(gate_last), gate_count])
        gate_count = 0
        gate_last = None

    for etype, nxt, meta in raw_edges:
        if etype == EDGE_STARGATE:
            gate_count += meta
            gate_last = nxt
        else:
            flush_gates()
            out.append([EDGE_CYNO, station_name_for_system_id(nxt), meta])

    flush_gates()
    return out


def make_route_expanded(
    systems: Dict[int, System],
    system_cyno_grade: Dict[int, str],
    stations_by_sysname: Dict[str, List[Station]],
    origin_id: int,
    raw_edges: List[Tuple[str, int, int]],
) -> List[str]:
    def station_name_for_system_id(sid: int) -> str:
        sysname = systems[sid].name
        grade = system_cyno_grade.get(sid, systems[sid].cyno_jump_security)
        return choose_station_for_system(sysname, grade, stations_by_sysname)

    expanded: List[str] = [systems[origin_id].name]

    i = 0
    while i < len(raw_edges):
        etype, nxt, _meta = raw_edges[i]
        if etype == EDGE_CYNO:
            expanded.append(station_name_for_system_id(nxt))
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
                    expanded.append(station_name_for_system_id(sid))

        i = j

    return expanded


# -----------------------------
# routeType naming (with "I" suppression)
# -----------------------------

_ROMAN_MAP = [
    (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
    (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
    (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
]


def to_roman(n: int) -> str:
    if n <= 0:
        raise ValueError("roman requires positive integer")
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


def write_jsonl_gz_atomic(path: str, rows: Iterable[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="routes.jsonl", fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solarsystems", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--stations", required=True)
    ap.add_argument("--ganksystems", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jita-name", default="Jita")
    ap.add_argument("--out-cat", default=None, help="DEPRECATED: ignored.")
    args = ap.parse_args()

    if args.out_cat is not None:
        print("WARNING: --out-cat is deprecated and ignored; routesCat is no longer generated.", file=sys.stderr)

    systems, name_to_id = load_systems(args.solarsystems)
    stations_by_sysname = load_stations(args.stations)
    system_cyno_grade = compute_system_cyno_from_stations(systems, stations_by_sysname)

    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: Destination system '{args.jita_name}' not found.", file=sys.stderr)
        return 2

    origin_ids = sorted(gate_adj.keys())
    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)

    base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, jita_id)

    type_sets = build_type_sets(
        systems=systems,
        system_cyno_grade=system_cyno_grade,
        gate_adj=gate_adj,
        gank_ids=gank_ids,
        base_has_lowsec=base_has_lowsec,
    )

    rev_cyno = build_reverse_cyno_edges_bruteforce_LD_only(
        systems=systems,
        LD=type_sets["LD"],
        system_cyno_grade=system_cyno_grade,
    )

    best, nxt = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
    )

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]

        if oid == jita_id:
            return {
                "solarSystem": o.name,
                "routeType": "highway 0",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [o.name],
            }

        if oid not in best:
            # CHANGE REQUESTED: routExpanded must be []
            return {
                "solarSystem": o.name,
                "routeType": "no route",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [],
            }

        raw_edges = reconstruct_raw_edges(dest_id=jita_id, origin_id=oid, nxt_step=nxt)
        compact_route = make_compact_route_with_stations(
            systems=systems,
            system_cyno_grade=system_cyno_grade,
            stations_by_sysname=stations_by_sysname,
            raw_edges=raw_edges,
        )
        expanded = make_route_expanded(
            systems=systems,
            system_cyno_grade=system_cyno_grade,
            stations_by_sysname=stations_by_sysname,
            origin_id=oid,
            raw_edges=raw_edges,
        )

        cyno_count, fuel, _risky_present, _low2high, _gank_hi, _neg_min, st_total, _inter, _bm = best[oid]
        jump_fuel = int(fuel) * 2

        safe_c = 0
        risky_c = 0
        for etype, did, _meta in raw_edges:
            if etype == EDGE_CYNO:
                grade = norm_cyno_grade(system_cyno_grade.get(did, systems[did].cyno_jump_security))
                if grade == "risky":
                    risky_c += 1
                elif grade == "safe":
                    safe_c += 1

        hisec = midsec = ganksec = lowsec = 0
        cur = oid
        seen: Set[int] = set()
        while cur != jita_id:
            if cur in seen:
                break
            seen.add(cur)
            ns = nxt.get(cur)
            if ns is None:
                break
            nxt_id, etype, _meta = ns
            if etype == EDGE_STARGATE:
                s2 = systems[nxt_id]
                if 0.0 < s2.sec < LOWSEC_THRESHOLD:
                    if nxt_id not in gank_ids:
                        lowsec += 1
                elif s2.sec >= LOWSEC_THRESHOLD:
                    if nxt_id in gank_ids:
                        ganksec += 1
                    else:
                        if s2.sec >= 0.65:
                            hisec += 1
                        else:
                            midsec += 1
            cur = nxt_id

        st_obj = {"count": int(st_total), "hisec": int(hisec), "midsec": int(midsec), "ganksec": int(ganksec), "lowsec": int(lowsec)}
        cj_obj = {"count": int(cyno_count), "safe": int(safe_c), "risky": int(risky_c)}

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
            "routeType": rt,
            "jumpFuel": int(jump_fuel),
            "cynoJumps": cj_obj,
            "stargates": st_obj,
            "route": compact_route,
            "routExpanded": expanded,
        }

    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)

    write_jsonl_gz_atomic(out_path, (row_for_origin(oid) for oid in origin_ids))
    print(f"OK: wrote {out_path} for {len(origin_ids)} origin systems (systems with stargates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
