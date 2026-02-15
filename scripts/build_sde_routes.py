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

# Cyno constraints (user-provided)
MAX_CYNO_DIST_M = 94_600_000_000_000_000  # 94600000000000000 m

# Fuel constants (user-provided)
ISO_NUM = 2350
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000 (fuel per meter denominator)

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"


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
        cyno = str(row.get("cynoJumpSecurity", "no jump"))

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
# routeSDEsafe100 (stargate-only) - CCP Safer imitation
# -----------------------------

def safer_cost(sec_to: float, penalty_cost: float) -> float:
    # CCP Route Calculation guide (Safer):
    # sec <= 0.0 -> 2*penalty_cost
    # sec < 0.45 -> penalty_cost
    # else -> 0.90
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
    """
    Reverse Dijkstra from dest_id across stargate graph.
    Returns next_hop[node] = next system towards dest in the best Safer+100 path.
    """
    penalty_cost = math.exp(0.15 * 100.0)  # security_penalty=100

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
    """
    base_has_lowsec[n] = True if base route (excluding origin and dest) includes any system with sec < 0.45.
    base_min_id[n]     = min solarSystemID among systems in base route excluding origin and dest, else INF.
    """
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
# Type sets (gate rules)
# -----------------------------

def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def is_nl(sec: float) -> bool:
    return sec < LOWSEC_THRESHOLD


def build_type_sets(
    systems: Dict[int, System],
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

        if is_lowsec(s.sec) and s.cyno_jump_security in ("safe", "risky"):
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
# Final routing (cyno + gate)
# -----------------------------

# Base internal cost tuple:
# fuel, cyno_count, risky_cyno_count, low2high_gate, gank_hi_entries, neg_minGateSec, stargate_count, intermediate_count, base_min_id
BaseCost = Tuple[int, int, int, int, int, float, int, int, int]


def build_reverse_cyno_edges_bruteforce_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    eligible = []
    for did in LD:
        d = systems[did]
        eligible.append((did, d.x, d.y, d.z, d.cyno_jump_security == "risky"))

    r2 = float(MAX_CYNO_DIST_M) ** 2
    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in eligible}

    all_systems = list(systems.values())
    for origin in all_systems:
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
    allow_risky_cyno: bool,
) -> Tuple[Dict[int, BaseCost], Dict[int, Tuple[int, str, int]]]:
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

    def inf_cost() -> BaseCost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT, INF_ID)

    # Your stargate norms (latest):
    # - S Ø NL  unless I -> LDg
    # - NL Ø S  unless Lg -> Hg
    def gate_allowed(p: int, u: int) -> bool:
        if p not in has_gate or u not in has_gate:
            return False

        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        if (p in NL) and (u in S):
            return (p in Lg) and (u in Hg)

        return True

    # Comparison order (your latest “Opción A” variant for the final route selection):
    # 1) fewer cynoJumps
    # 2) lower fuel
    # 3) fewer risky cynoJumps
    # 4) fewer low->high gates
    # 5) fewer gank highsec intermediate nodes
    # 6) maximize min stargate sec  (stored as neg; we want larger neg_min => higher min sec)
    # 7) fewer stargates
    # 8) fewer intermediate systems
    # 9) lower base_min_id
    def key(c: BaseCost) -> Tuple[Any, ...]:
        fuel, cyno_j, risky_j, low2high, gank_hi, neg_min, gates, inter, bm = c
        return (cyno_j, fuel, risky_j, low2high, gank_hi, neg_min, gates, inter, bm)

    best: Dict[int, BaseCost] = {}
    nxt_step: Dict[int, Tuple[int, str, int]] = {}

    start: BaseCost = (0, 0, 0, 0, 0, -1.0, 0, 0, base_min_id.get(dest_id, INF_ID))
    best[dest_id] = start

    heap: List[Tuple[Tuple[Any, ...], int, BaseCost]] = []
    heapq.heappush(heap, (key(start), dest_id, start))

    while heap:
        _k, u, cost_u = heapq.heappop(heap)
        if best.get(u) != cost_u:
            continue

        # Stargate predecessors (p -> u)
        for p in gate_adj.get(u, []):
            if not gate_allowed(p, u):
                continue

            sec_p = systems[p].sec
            sec_u = systems[u].sec

            fuel, cyno_j, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u

            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            low2high2 = low2high + (1 if (sec_p < LOWSEC_THRESHOLD and sec_u >= LOWSEC_THRESHOLD) else 0)
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (sec_u >= LOWSEC_THRESHOLD)) else 0)

            neg_min2 = max(neg_min, -round(sec_u, 6))
            bm_p = base_min_id.get(p, INF_ID)

            cand: BaseCost = (fuel, cyno_j, risky_j, low2high2, gank_hi2, neg_min2, gates2, inter2, bm_p)
            old = best.get(p, inf_cost())

            if key(cand) < key(old) or (key(cand) == key(old) and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (key(cand), p, cand))

        # Cyno predecessors (p -> u)
        for (p, fuel_edge, dest_is_risky) in rev_cyno.get(u, []):
            if dest_is_risky and not allow_risky_cyno:
                continue

            fuel, cyno_j, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u
            bm_p = base_min_id.get(p, INF_ID)

            cand: BaseCost = (
                fuel + fuel_edge,
                cyno_j + 1,
                risky_j + (1 if dest_is_risky else 0),
                low2high,
                gank_hi,
                neg_min,
                gates,
                inter + 1,
                bm_p,
            )
            old = best.get(p, inf_cost())

            if key(cand) < key(old) or (key(cand) == key(old) and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_CYNO, fuel_edge)
                heapq.heappush(heap, (key(cand), p, cand))

    return best, nxt_step


def reconstruct_steps(
    systems: Dict[int, System],
    dest_id: int,
    origin_id: int,
    nxt_step: Dict[int, Tuple[int, str, int]],
) -> List[List[Any]]:
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

    out: List[List[Any]] = []
    gate_count = 0
    gate_last: Optional[int] = None

    def flush_gates() -> None:
        nonlocal gate_count, gate_last
        if gate_count > 0 and gate_last is not None:
            out.append([EDGE_STARGATE, systems[gate_last].name, gate_count])
        gate_count = 0
        gate_last = None

    for etype, nxt, meta in raw:
        if etype == EDGE_STARGATE:
            gate_count += meta
            gate_last = nxt
        else:
            flush_gates()
            out.append([EDGE_CYNO, systems[nxt].name, meta])

    flush_gates()
    return out


# -----------------------------
# routeType naming with roman suppression for "I"
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


def cyno_run_signature(route_steps: List[List[Any]]) -> str:
    runs: List[int] = []
    i = 0
    while i < len(route_steps):
        if route_steps[i][0] != EDGE_CYNO:
            i += 1
            continue
        j = i
        while j < len(route_steps) and route_steps[j][0] == EDGE_CYNO:
            j += 1
        runs.append(j - i)
        i = j
    if not runs:
        return ""
    return "-".join(to_roman(r) for r in runs)


def normalize_roman_for_route_type(roman: str) -> str:
    # user rule: omit exactly "I", keep "II", "III", ..., and keep compound like "I-I"
    return "" if roman == "I" else roman


def build_route_type(
    *,
    has_route: bool,
    origin_is_jita: bool,
    route_steps: List[List[Any]],
    cyno_risky_count: int,
    stargates_lowsec_count: int,
    stargates_ganksec_count: int,
    stargates_total: int,
) -> str:
    if not has_route:
        return "no route"

    if origin_is_jita:
        return "highway 0"

    has_cyno = any(step[0] == EDGE_CYNO for step in route_steps)

    if not has_cyno:
        base = "highway"
        roman = ""
    else:
        first = route_steps[0][0] if route_steps else EDGE_STARGATE
        base = "spaceport" if first == EDGE_CYNO else "island"
        roman = normalize_roman_for_route_type(cyno_run_signature(route_steps))

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


# -----------------------------
# GZip writer (atomic) with internal filename control
# -----------------------------

def write_jsonl_gz_atomic(path: str, rows: Iterable[dict], gz_filename: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(filename=gz_filename, fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


# -----------------------------
# Crossroads (routeSDEsafe100-only), Option B (intermediate-only)
# -----------------------------

def compute_crossroads_intermediate_only(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
    origin_ids: List[int],
) -> Dict[int, int]:
    """
    Count how many routeSDEsafe100 (stargate-only) paths include node as INTERMEDIATE:
      - excludes origin itself
      - excludes destination (Jita)
    Only returns nodes with sec >= 0.45 and count > 0.
    """
    origins: Set[int] = set(origin_ids)

    # Build children list from next_hop: child -> parent (toward dest) so reverse is parent -> children
    children: Dict[int, List[int]] = {}
    for child, parent in base_next.items():
        children.setdefault(parent, []).append(child)

    # Compute nodes reachable in the base tree
    nodes: Set[int] = set(children.keys()) | set(base_next.keys()) | {dest_id}

    # Depth memo to topologically process far->near
    sys.setrecursionlimit(20000)
    depth_memo: Dict[int, int] = {}

    def depth(n: int) -> int:
        if n == dest_id:
            return 0
        if n in depth_memo:
            return depth_memo[n]
        nxt = base_next.get(n)
        if nxt is None:
            depth_memo[n] = -1
            return -1
        d = depth(nxt)
        if d < 0:
            depth_memo[n] = -1
            return -1
        depth_memo[n] = d + 1
        return depth_memo[n]

    order: List[Tuple[int, int]] = []
    for n in nodes:
        d = depth(n)
        if d >= 0:
            order.append((d, n))
    order.sort(reverse=True)  # farthest first

    # subtree[n] = number of origins whose path (including the origin node) passes through n
    subtree: Dict[int, int] = {}
    for _d, n in order:
        c = 1 if n in origins else 0
        for ch in children.get(n, []):
            c += subtree.get(ch, 0)
        subtree[n] = c

    # intermediate-only pass count: subtree[n] minus routes whose origin==n (and exclude dest)
    out: Dict[int, int] = {}
    for n, c in subtree.items():
        if n == dest_id:
            continue
        inter = c - (1 if n in origins else 0)
        if inter > 0 and systems[n].sec >= LOWSEC_THRESHOLD:
            out[n] = inter

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solarsystems", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--ganksystems", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--out-crossroads", default=None)
    ap.add_argument("--jita-name", default="Jita")

    # Backward compat: ignore removed output
    ap.add_argument("--out-cat", default=None, help="DEPRECATED: ignored (routesCat removed).")

    args = ap.parse_args()

    if args.out_cat is not None:
        print("WARNING: --out-cat is deprecated and ignored; routesCat is no longer generated.", file=sys.stderr)

    systems, name_to_id = load_systems(args.solarsystems)
    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: Destination system '{args.jita_name}' not found.", file=sys.stderr)
        return 2

    origin_ids = sorted(gate_adj.keys())
    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)

    # routeSDEsafe100 precompute
    base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, jita_id)

    type_sets = build_type_sets(systems, gate_adj, gank_ids, base_has_lowsec)
    rev_cyno = build_reverse_cyno_edges_bruteforce_LD_only(systems, type_sets["LD"])

    best_safe, next_safe = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
        allow_risky_cyno=False,
    )
    best_risky, next_risky = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
        allow_risky_cyno=True,
    )

    def replay_full_path_nodes(origin: int, nxt_map: Dict[int, Tuple[int, str, int]]) -> List[int]:
        nodes: List[int] = []
        cur = origin
        seen = set()
        while cur != jita_id:
            if cur in seen:
                return []
            seen.add(cur)
            ns = nxt_map.get(cur)
            if ns is None:
                return []
            nxt, _etype, _meta = ns
            nodes.append(nxt)
            cur = nxt
        return nodes

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
            }

        chosen_cost: Optional[BaseCost] = None
        chosen_next: Optional[Dict[int, Tuple[int, str, int]]] = None

        if oid in best_safe:
            chosen_cost = best_safe[oid]
            chosen_next = next_safe
        elif oid in best_risky:
            chosen_cost = best_risky[oid]
            chosen_next = next_risky

        if chosen_cost is None or chosen_next is None:
            return {
                "solarSystem": o.name,
                "routeType": "no route",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
            }

        steps = reconstruct_steps(systems, jita_id, oid, chosen_next)
        path_nodes = replay_full_path_nodes(oid, chosen_next)

        cyno_count = 0
        cyno_safe = 0
        cyno_risky = 0
        st_count = 0
        st_hisec = 0
        st_midsec = 0
        st_ganksec = 0
        st_lowsec = 0

        cur = oid
        for nxt in path_nodes:
            etype = chosen_next[cur][1]
            if etype == EDGE_CYNO:
                cyno_count += 1
                if systems[nxt].cyno_jump_security == "risky":
                    cyno_risky += 1
                elif systems[nxt].cyno_jump_security == "safe":
                    cyno_safe += 1
            else:
                st_count += 1
                sec = systems[nxt].sec
                if 0.0 < sec < LOWSEC_THRESHOLD and nxt not in gank_ids:
                    st_lowsec += 1
                elif sec >= LOWSEC_THRESHOLD and nxt in gank_ids:
                    st_ganksec += 1
                elif LOWSEC_THRESHOLD <= sec < 0.65 and nxt not in gank_ids:
                    st_midsec += 1
                elif sec >= 0.65 and nxt not in gank_ids:
                    st_hisec += 1
            cur = nxt

        fuel_real, _cj, _rj, _l2h, _gankhi, _negmin, _gates, _inter, _bm = chosen_cost
        jump_fuel_out = int(fuel_real) * 2  # your rule: doubled output fuel, per-step meta unchanged

        route_type = build_route_type(
            has_route=True,
            origin_is_jita=False,
            route_steps=steps,
            cyno_risky_count=cyno_risky,
            stargates_lowsec_count=st_lowsec,
            stargates_ganksec_count=st_ganksec,
            stargates_total=st_count,
        )

        return {
            "solarSystem": o.name,
            "routeType": route_type,
            "jumpFuel": int(jump_fuel_out),
            "cynoJumps": {"count": int(cyno_count), "safe": int(cyno_safe), "risky": int(cyno_risky)},
            "stargates": {"count": int(st_count), "hisec": int(st_hisec), "midsec": int(st_midsec), "ganksec": int(st_ganksec), "lowsec": int(st_lowsec)},
            "route": steps,
        }

    # Write routes.jsonl.gz
    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)
    write_jsonl_gz_atomic(out_path, (row_for_origin(oid) for oid in origin_ids), gz_filename="routes.jsonl")
    print(f"OK: wrote {out_path} for {len(origin_ids)} origin systems (systems with stargates).")

    # Write crossroads.jsonl.gz ALWAYS
    out_cross = args.out_crossroads
    if not out_cross:
        out_cross = os.path.join(os.path.dirname(out_path), "crossroads.jsonl.gz")

    cross_counts = compute_crossroads_intermediate_only(
        systems=systems,
        base_next=base_next,
        dest_id=jita_id,
        origin_ids=origin_ids,
    )

    # Deterministic sort: routes desc, then name asc
    sorted_cross = sorted(cross_counts.items(), key=lambda kv: (-kv[1], systems[kv[0]].name))

    def cross_rows() -> Iterable[dict]:
        for sid, cnt in sorted_cross:
            yield {"crossroad": systems[sid].name, "routes": int(cnt)}

    if os.path.exists(out_cross):
        os.remove(out_cross)
    write_jsonl_gz_atomic(out_cross, cross_rows(), gz_filename="crossroads.jsonl")
    print(f"OK: wrote {out_cross} with {len(sorted_cross)} crossroads (sec>=0.45, intermediate-only).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
