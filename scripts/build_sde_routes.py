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

# Fuel constants
ISO_NUM = 2350
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000 (fuel per meter denominator)


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

    out: Dict[int, List[int]] = {}
    for k, vs in adj.items():
        out[k] = sorted(vs)
    return out


def load_ganksystems_ids_txt(ganksystems_txt: str, name_to_id: Dict[str, int]) -> Set[int]:
    """
    One system name per line. Blank lines and # comments allowed.
    """
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
    # As per Route Calculation guide for "Safer":
    # if sec <= 0.0 -> 2*penalty_cost
    # elif sec < 0.45 -> penalty_cost
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

    heap: List[Tuple[float, int, int]] = []
    heapq.heappush(heap, (0.0, 0, dest_id))

    while heap:
        cost_u, gates_u, u = heapq.heappop(heap)
        cur = dist.get(u)
        if cur is None or cur[0] != cost_u or cur[1] != gates_u:
            continue

        for p in gate_adj.get(u, []):
            inc = safer_cost(systems[u].sec, penalty_cost)
            new_cost = cost_u + inc
            new_gates = gates_u + 1
            cand = (new_cost, new_gates)

            old = dist.get(p, (INF, 10**18))
            better = cand < old
            equal = cand == old
            if better or (equal and u < next_hop.get(p, 2**31 - 1)):
                dist[p] = cand
                next_hop[p] = u
                heapq.heappush(heap, (new_cost, new_gates, p))

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
# Types (per your rules)
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
    HG: Set[int] = set()
    Lg: Set[int] = set()
    LD: Set[int] = set()
    LDg: Set[int] = set()
    I: Set[int] = set()

    for sid, s in systems.items():
        # [Tipo S]: has at least one stargate and sec<=1
        if sid in has_gate and s.sec <= 1.0:
            S.add(sid)

        if is_nl(s.sec):
            NL.add(sid)

        # Highsec candidates with gates
        if sid in has_gate and s.sec >= LOWSEC_THRESHOLD:
            if sid in gank_ids:
                HG.add(sid)
            else:
                Hg.add(sid)

        # Lowsec non-gank
        if is_lowsec(s.sec) and sid not in gank_ids:
            Lg.add(sid)

        # Cyno destinations allowed: 0<sec<0.45 and cyno safe/risky
        if is_lowsec(s.sec) and s.cyno_jump_security in ("safe", "risky"):
            LD.add(sid)
            if sid not in gank_ids:
                LDg.add(sid)

        # I: highsec with base route that includes lowsec somewhere
        if s.sec >= LOWSEC_THRESHOLD and base_has_lowsec.get(sid, False):
            I.add(sid)

    return {
        "has_gate": has_gate,
        "S": S,
        "NL": NL,
        "Hg": Hg,
        "HG": HG,
        "Lg": Lg,
        "LD": LD,
        "LDg": LDg,
        "I": I,
    }


# -----------------------------
# Final routing
# -----------------------------

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"

# Selection order (your latest preference):
# 1) fewer cynoJumps
# 2) lower total jumpFuel (note: output shows 2x fuel, but the optimiser must use real fuel)
# 3) fewer risky cynoJumps
# 4) low->high stargate crossings
# 5) gank highsec nodes (intermediate nodes count)
# 6) max(minStargateSecurityStatus)
# 7) fewer stargates
# 8) fewer intermediate systems
# 9) min base_min_id
Cost = Tuple[int, int, int, int, int, float, int, int, int]


def build_reverse_cyno_edges_bruteforce_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    Reverse cyno adjacency for destinations only in [LD].
    dest -> list of (origin, fuel, dest_is_risky)
    """
    eligible = []
    for did in LD:
        d = systems[did]
        eligible.append((did, d.x, d.y, d.z, d.cyno_jump_security == "risky"))

    r2 = float(MAX_CYNO_DIST_M) * float(MAX_CYNO_DIST_M)
    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in eligible}

    all_systems = list(systems.values())
    for origin in all_systems:
        ox, oy, oz = origin.x, origin.y, origin.z
        oid = origin.system_id

        # Cyno restriction you stated earlier is applied by LD set:
        # 0<sec<0.45 AND cynoJumpSecurity in (safe,risky)
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
        # Forward gate p -> u

        if p not in has_gate or u not in has_gate:
            return False

        # [S] Ø [NL]  (unless [I] -> [LDg])
        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        # [NL] Ø [S]  (unless [Lg] -> [Hg])
        if (p in NL) and (u in S):
            return (p in Lg) and (u in Hg)

        # Otherwise allowed
        return True

    best: Dict[int, Cost] = {}
    nxt_step: Dict[int, Tuple[int, str, int]] = {}

    # minStargateSecurityStatus uses negative(max) trick; start at -1.0
    start: Cost = (0, 0, 0, 0, 0, -1.0, 0, 0, base_min_id.get(dest_id, INF_ID))
    best[dest_id] = start

    heap: List[Tuple[Cost, int]] = []
    heapq.heappush(heap, (start, dest_id))

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

            fuel, cyno_j, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u

            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            low2high2 = low2high + (1 if (sec_p < LOWSEC_THRESHOLD and sec_u >= LOWSEC_THRESHOLD) else 0)

            # count intermediate gank-highsec nodes
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (sec_u >= LOWSEC_THRESHOLD)) else 0)

            # update min stargate security (among visited-by-gate nodes)
            neg_min2 = max(neg_min, -round(sec_u, 6))

            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (
                fuel,          # (2nd criterion)
                cyno_j,        # (1st criterion)
                risky_j,       # (3rd)
                low2high2,     # (4th)
                gank_hi2,      # (5th)
                neg_min2,      # (6th)
                gates2,        # (7th)
                inter2,        # (8th)
                bm_p,          # (9th)
            )

            # reorder tuple to match criteria:
            # 1 cyno_jumps, 2 fuel, 3 risky, ... 9
            cand_reordered: Cost = (
                cand[1], cand[0], cand[2], cand[3], cand[4], cand[5], cand[6], cand[7], cand[8]
            )
            old = best.get(p, inf_cost())
            old_reordered: Cost = (old[1], old[0], old[2], old[3], old[4], old[5], old[6], old[7], old[8])

            better = cand_reordered < old_reordered
            equal = cand_reordered == old_reordered
            if better or (equal and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # Cyno predecessors (p -> u)
        for (p, fuel_edge, dest_is_risky) in rev_cyno.get(u, []):
            if dest_is_risky and not allow_risky_cyno:
                continue

            fuel, cyno_j, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u
            bm_p = base_min_id.get(p, 2**31 - 1)

            cand: Cost = (
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

            cand_reordered: Cost = (cand[1], cand[0], cand[2], cand[3], cand[4], cand[5], cand[6], cand[7], cand[8])
            old = best.get(p, inf_cost())
            old_reordered: Cost = (old[1], old[0], old[2], old[3], old[4], old[5], old[6], old[7], old[8])

            better = cand_reordered < old_reordered
            equal = cand_reordered == old_reordered
            if better or (equal and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_CYNO, fuel_edge)
                heapq.heappush(heap, (cand, p))

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


def write_jsonl_gz_atomic(path: str, rows: Iterable[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


# -----------------------------
# Crossroads (routeSDEsafe100-only)
# -----------------------------

def compute_crossroads_counts_intermediate_only(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
    origin_ids: List[int],
) -> Dict[int, int]:
    """
    Count, for each system u, how many routeSDEsafe100 paths (origin->...->dest)
    pass through u as an INTERMEDIATE node (Option B):
      - excludes origin and excludes dest
    Only counts u if systems[u].sec >= 0.45 (enforced at the end).
    """

    origins: Set[int] = set(origin_ids)

    # Build children map from next_hop:
    children: Dict[int, List[int]] = {}
    nodes_in_tree: Set[int] = {dest_id}

    for o in origin_ids:
        nxt = base_next.get(o)
        if nxt is None:
            continue  # unreachable in base stargate-only
        children.setdefault(nxt, []).append(o)
        nodes_in_tree.add(o)
        nodes_in_tree.add(nxt)

    # But we only added first edge per origin. We need all edges:
    # for every node p with next_hop[p]=u, add p to children[u]
    children = {}
    for p, u in base_next.items():
        children.setdefault(u, []).append(p)
        nodes_in_tree.add(p)
        nodes_in_tree.add(u)

    # Compute depth (distance to dest following next pointers) for ordering
    sys.setrecursionlimit(20000)
    depth_memo: Dict[int, int] = {}

    def depth(n: int) -> int:
        if n == dest_id:
            return 0
        if n in depth_memo:
            return depth_memo[n]
        nxt = base_next.get(n)
        if nxt is None:
            depth_memo[n] = -1  # unreachable
            return -1
        d = depth(nxt)
        if d < 0:
            depth_memo[n] = -1
            return -1
        depth_memo[n] = d + 1
        return depth_memo[n]

    depths = []
    for n in nodes_in_tree:
        d = depth(n)
        if d >= 0:
            depths.append((d, n))

    # process from farthest to nearest
    depths.sort(reverse=True)

    subtree: Dict[int, int] = {dest_id: 0}

    for _d, n in depths:
        if n == dest_id:
            continue
        # base subtree count: 1 if n is an origin with stargates
        c = 1 if n in origins else 0
        # plus contributions from children that point to n (i.e., have next_hop[child]=n)
        for ch in children.get(n, []):
            c += subtree.get(ch, 0)
        subtree[n] = c

    # Convert to "intermediate-only pass count"
    pass_count: Dict[int, int] = {}
    for n, c in subtree.items():
        if n == dest_id:
            continue
        # as intermediate: routes through n excluding when origin == n
        inter = c - (1 if n in origins else 0)
        if inter > 0 and systems[n].sec >= LOWSEC_THRESHOLD:
            pass_count[n] = inter

    return pass_count


# -----------------------------
# routeType (simple, Jita-specific naming)
# -----------------------------

_ROMANS = {2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X"}


def _roman_if_gt_1(n: int) -> str:
    if n <= 1:
        return ""
    return _ROMANS.get(n, str(n))


def build_route_type(
    base_next: Dict[int, int],
    origin_id: int,
    dest_id: int,
    has_route: bool,
    cyno_count: int,
    risky_cyno_count: int,
    stargates_count: int,
    st_lowsec: int,
    st_ganksec: int,
) -> str:
    if not has_route:
        return "no route"

    # prefix priority for security exposure on stargate part
    color = ""
    if st_lowsec > 0:
        color = "red "
    elif st_ganksec > 0:
        color = "yellow "

    risky = "risky " if risky_cyno_count > 0 else ""

    if cyno_count == 0:
        base = "highway"
        roman = ""
    else:
        # island if origin has no base stargate-only route to Jita (i.e., no next hop in base tree)
        is_island_start = (origin_id != dest_id) and (base_next.get(origin_id) is None)
        base = "island" if is_island_start else "spaceport"
        roman = _roman_if_gt_1(cyno_count)
        if roman:
            base = f"{base} {roman}"

    # Append total stargates at end
    return f"{risky}{color}{base} {stargates_count}".strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solarsystems", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--ganksystems", required=True)  # .txt
    ap.add_argument("--out", required=True)          # routes.jsonl.gz
    ap.add_argument("--out-crossroads", default=None)
    ap.add_argument("--jita-name", default="Jita")
    args = ap.parse_args()

    systems, name_to_id = load_systems(args.solarsystems)
    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: Destination system '{args.jita_name}' not found.", file=sys.stderr)
        return 2

    # Origins: all systems that contain stargates
    origin_ids = sorted(gate_adj.keys())

    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)

    # routeSDEsafe100 precompute (Safer+100)
    base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, jita_id)

    # Types under your rules
    type_sets = build_type_sets(systems, gate_adj, gank_ids, base_has_lowsec)

    # Cyno adjacency (only LD destinations)
    rev_cyno = build_reverse_cyno_edges_bruteforce_LD_only(systems, type_sets["LD"])

    # Two-pass: risky cyno only if needed
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

    def compute_counts_from_steps(oid: int, steps: List[List[Any]]) -> Tuple[int, int, int, int, int, int, int]:
        """
        Returns:
          cyno_count, cyno_safe, cyno_risky,
          st_count, st_hisec, st_midsec, st_ganksec, st_lowsec
        """
        cyno_count = 0
        cyno_safe = 0
        cyno_risky = 0

        st_count = 0
        st_hisec = 0
        st_midsec = 0
        st_ganksec = 0
        st_lowsec = 0

        cur = oid
        for step in steps:
            etype = step[0]
            dest_name = step[1]
            meta = step[2]
            did = name_to_id.get(dest_name)

            if did is None:
                # should not happen; ignore defensively
                cur = cur
                continue

            if etype == EDGE_CYNO:
                cyno_count += 1
                if systems[did].cyno_jump_security == "risky":
                    cyno_risky += 1
                elif systems[did].cyno_jump_security == "safe":
                    cyno_safe += 1
                cur = did
            else:
                # aggregated gate step: meta = number of gates, but destination is the last system
                # we need to walk meta times to count intermediate gate nodes; however we only store the final node name.
                # For counting stargate categories we count ONLY the visited-by-gate nodes along the actual path.
                # We reconstruct by simulating next hops from cur using nxt_step pointers (already encoded in steps).
                # Given that steps are aggregated, we can't know intermediates without walking. We'll walk using next pointers by re-running reconstruction raw.
                # To keep determinism, we recompute counts by replaying the original raw path using next_safe/next_risky later (below).
                st_count += int(meta)
                cur = did

        return cyno_count, cyno_safe, cyno_risky, st_count, st_hisec, st_midsec, st_ganksec, st_lowsec

    def replay_full_path_nodes(origin: int, nxt_map: Dict[int, Tuple[int, str, int]]) -> Tuple[List[Tuple[str, int, int]], List[int]]:
        """
        Returns:
          raw edges: (etype, next_id, meta)
          visited nodes sequence (excluding origin): each hop destination system id in order
        """
        raw: List[Tuple[str, int, int]] = []
        nodes: List[int] = []
        cur = origin
        seen = set()
        while cur != jita_id:
            if cur in seen:
                return [], []
            seen.add(cur)
            ns = nxt_map.get(cur)
            if ns is None:
                return [], []
            nxt, etype, meta = ns
            raw.append((etype, nxt, meta))
            nodes.append(nxt)
            cur = nxt
        return raw, nodes

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]

        if oid == jita_id:
            route_type = build_route_type(base_next, oid, jita_id, True, 0, 0, 0, 0, 0)
            return {
                "solarSystem": o.name,
                "routeType": route_type,     # replaces hasRoute
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
            }

        chosen_cost: Optional[Cost] = None
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

        # replay the full node sequence to count per-category precisely
        _raw, path_nodes = replay_full_path_nodes(oid, chosen_next)

        cyno_count = 0
        cyno_safe = 0
        cyno_risky = 0
        st_count = 0
        st_hisec = 0
        st_midsec = 0
        st_ganksec = 0
        st_lowsec = 0

        # Walk path nodes; for each node, determine if it was entered by gate or cyno by looking at next-edge type
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

        # jumpFuel: store DOUBLE of total cyno fuel used (but keep per-cyno meta in route unchanged)
        fuel_real, _cyno_j, _risky_j, _low2high, _gank_hi, _neg_min, _gates, _inter, _bm = chosen_cost
        fuel_out = int(fuel_real) * 2

        route_type = build_route_type(
            base_next=base_next,
            origin_id=oid,
            dest_id=jita_id,
            has_route=True,
            cyno_count=cyno_count,
            risky_cyno_count=cyno_risky,
            stargates_count=st_count,
            st_lowsec=st_lowsec,
            st_ganksec=st_ganksec,
        )

        return {
            "solarSystem": o.name,
            "routeType": route_type,
            "jumpFuel": fuel_out,
            "cynoJumps": {"count": int(cyno_count), "safe": int(cyno_safe), "risky": int(cyno_risky)},
            "stargates": {"count": int(st_count), "hisec": int(st_hisec), "midsec": int(st_midsec), "ganksec": int(st_ganksec), "lowsec": int(st_lowsec)},
            "route": steps,
        }

    # Write routes
    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)
    write_jsonl_gz_atomic(out_path, (row_for_origin(oid) for oid in origin_ids))
    print(f"OK: wrote {out_path} for {len(origin_ids)} origin systems (systems with stargates).")

    # Write crossroads (routeSDEsafe100-only)
    out_cross = args.out_crossroads
    if not out_cross:
        # default sibling file next to routes.jsonl.gz
        out_cross = os.path.join(os.path.dirname(out_path), "crossroads.jsonl.gz")

    cross_counts = compute_crossroads_counts_intermediate_only(
        systems=systems,
        base_next=base_next,
        dest_id=jita_id,
        origin_ids=origin_ids,
    )

    # Sort by routes desc, then name asc for determinism
    sorted_cross = sorted(
        cross_counts.items(),
        key=lambda kv: (-kv[1], systems[kv[0]].name),
    )

    def cross_rows() -> Iterable[dict]:
        for sid, cnt in sorted_cross:
            yield {"crossroad": systems[sid].name, "routes": int(cnt)}

    if os.path.exists(out_cross):
        os.remove(out_cross)
    write_jsonl_gz_atomic(out_cross, cross_rows())
    print(f"OK: wrote {out_cross} with {len(sorted_cross)} crossroads (sec>=0.45, intermediate-only).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
