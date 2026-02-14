#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build data/sde/routes.jsonl.gz from scratch.

Implements:
1) routeSDEsafe100 (stargate-only) imitating CCP "Safer" cost function with security_penalty=100.
   Source reference: CCP Route Calculation guide (Safer) describes:
     penalty_cost = exp(0.15 * security_penalty), with thresholds 0.45 and <=0 handling.

2) Final routing: stargate + cynojump with the user's constraints and lexicographic "exclusion descending" criteria.
   Risky cyno jumps are only allowed if there is no route possible without risky cyno jumps.
"""

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
ISO_NUM = 16565
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000 isotopes/m denominator


@dataclass(frozen=True)
class System:
    system_id: int
    name: str
    sec: float
    x: float
    y: float
    z: float
    faction: Optional[str]
    cyno_jump_security: str  # "safe" | "risky" | "unsafe" | "no jump" | ...


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
    # Expected: "Heydieles ↔ Actee"
    if not group:
        return None
    if "↔" in group:
        parts = [p.strip() for p in group.split("↔")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    # Fallback: sometimes could be "<->"
    if "<->" in group:
        parts = [p.strip() for p in group.split("<->")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    return None


def _split_stargate_arrow(name: str) -> Optional[Tuple[str, str]]:
    # Expected: "Heydieles → Actee"
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
            # Fallback to the stargate name "A → B"
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

    # Convert to sorted lists for determinism
    out: Dict[int, List[int]] = {}
    for k, vs in adj.items():
        out[k] = sorted(vs)
    return out


def load_ganksystems_ids(ganksystems_json: str, name_to_id: Dict[str, int]) -> Set[int]:
    with open(ganksystems_json, "rt", encoding="utf-8") as f:
        data = json.load(f)

    found: Set[int] = set()

    def visit(x: Any) -> None:
        if x is None:
            return
        if isinstance(x, int):
            found.add(x)
            return
        if isinstance(x, str):
            sid = name_to_id.get(x.strip())
            if sid is not None:
                found.add(sid)
            return
        if isinstance(x, list):
            for it in x:
                visit(it)
            return
        if isinstance(x, dict):
            # Common keys
            if "solarSystemID" in x and isinstance(x["solarSystemID"], int):
                found.add(int(x["solarSystemID"]))
            if "solarSystem" in x and isinstance(x["solarSystem"], str):
                sid = name_to_id.get(x["solarSystem"].strip())
                if sid is not None:
                    found.add(sid)
            for v in x.values():
                visit(v)
            return

    visit(data)
    return found


# -----------------------------
# routeSDEsafe100 (stargate-only)
# -----------------------------

def safer_cost(sec_to: float, penalty_cost: float) -> float:
    # CCP Safer:
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
) -> Tuple[Dict[int, Tuple[float, int]], Dict[int, int]]:
    """
    Reverse Dijkstra from dest_id across stargate graph.

    Distance is a tuple: (total_cost, gate_count) for deterministic tie-breaking.
    next_hop[node] gives the next system towards dest_id in the chosen best path.
    """
    penalty_cost = math.exp(0.15 * 100.0)  # security_penalty=100

    INF = float("inf")
    dist: Dict[int, Tuple[float, int]] = {dest_id: (0.0, 0)}
    next_hop: Dict[int, int] = {}

    heap: List[Tuple[float, int, int]] = []
    heapq.heappush(heap, (0.0, 0, dest_id))

    while heap:
        cost_u, gates_u, u = heapq.heappop(heap)
        cur = dist.get(u)
        if cur is None or cur[0] != cost_u or cur[1] != gates_u:
            continue

        for p in gate_adj.get(u, []):
            sec_u = systems[u].sec
            inc = safer_cost(sec_u, penalty_cost)
            new_cost = cost_u + inc
            new_gates = gates_u + 1
            cand = (new_cost, new_gates)

            old = dist.get(p, (INF, 10**18))
            # Deterministic tie-break: if same (cost,gates), choose smaller next hop id.
            better = cand < old
            equal = cand == old
            if better or (equal and u < next_hop.get(p, 2**31 - 1)):
                dist[p] = cand
                next_hop[p] = u
                heapq.heappush(heap, (new_cost, new_gates, p))

    return dist, next_hop


def compute_base_flags(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
) -> Tuple[Dict[int, bool], Dict[int, int]]:
    """
    For each node, computes:
      - has_lowsec_in_base[node]: whether the base route (excluding origin and dest) contains any system with sec < 0.45
      - min_id_in_base[node]: min solarSystemID among systems in base route excluding origin and dest, or INF if none
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
# Final routing (stargate + cyno)
# -----------------------------

Cost = Tuple[int, int, int, int, int, float, int, int]
# (risky_cyno, isotopes, cyno_jumps, low_to_high_gate, gank_highsec_entries, neg_min_gate_sec, stargates, intermediate_systems)

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cyno"


def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def is_null_or_worse(sec: float) -> bool:
    return sec <= 0.0


def ceil_div_float(numer: float, denom: float) -> int:
    return int(math.ceil(numer / denom))


def build_reverse_cyno_edges_bruteforce(
    systems: Dict[int, System],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    Build reverse cyno adjacency: dest -> list of (origin, isotopes, dest_is_risky).

    Brute force over all origins against eligible destinations, with a squared-distance filter.
    """
    eligible_dests: List[Tuple[int, float, float, float, bool]] = []
    for s in systems.values():
        # Only can cynojump TO:
        # - sec < 0.45
        # - cynoJumpSecurity in {"safe","risky"}
        # - NOT (sec <= 0 AND faction == null)
        if s.sec >= LOWSEC_THRESHOLD:
            continue
        if s.cyno_jump_security not in ("safe", "risky"):
            continue
        if s.sec <= 0.0 and s.faction is None:
            continue
        eligible_dests.append((s.system_id, s.x, s.y, s.z, s.cyno_jump_security == "risky"))

    r2 = float(MAX_CYNO_DIST_M) * float(MAX_CYNO_DIST_M)
    rev: Dict[int, List[Tuple[int, int, bool]]] = {d[0]: [] for d in eligible_dests}

    # Pre-materialize systems list for speed
    all_systems = list(systems.values())

    for origin in all_systems:
        ox, oy, oz = origin.x, origin.y, origin.z
        oid = origin.system_id

        # Cyno to itself is nonsensical for routing; skip
        for did, dx, dy, dz, dest_is_risky in eligible_dests:
            if did == oid:
                continue
            ddx = ox - dx
            ddy = oy - dy
            ddz = oz - dz
            d2 = ddx * ddx + ddy * ddy + ddz * ddz
            if d2 > r2:
                continue

            dist_m = math.sqrt(d2)
            iso = int(math.ceil(dist_m * ISO_NUM / ISO_DEN))
            if iso < 1:
                iso = 1

            rev[did].append((oid, iso, dest_is_risky))

    # Deterministic ordering
    for did, items in rev.items():
        items.sort(key=lambda t: (t[0], t[1], t[2]))
    return rev


def compute_lowsec_has_highsec_neighbor(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
) -> Dict[int, bool]:
    out: Dict[int, bool] = {}
    for sid, neighs in gate_adj.items():
        s = systems[sid]
        if not is_lowsec(s.sec):
            out[sid] = False
            continue
        ok = False
        for nb in neighs:
            if systems[nb].sec >= LOWSEC_THRESHOLD:
                ok = True
                break
        out[sid] = ok
    return out


def dijkstra_final_routes(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    rev_cyno: Dict[int, List[Tuple[int, int, bool]]],
    dest_id: int,
    base_has_lowsec: Dict[int, bool],
    gank_lowsec: Set[int],
    gank_highsec: Set[int],
    forced_cyno_origins: Set[int],
    lowsec_has_highsec_neighbor: Dict[int, bool],
    allow_risky_cyno: bool,
) -> Tuple[Dict[int, Cost], Dict[int, Tuple[int, str, int]]]:
    """
    Reverse Dijkstra from dest_id using lexicographic vector cost.

    Returns:
      - best_cost[node] -> Cost tuple
      - next_step[node] -> (next_id, edge_type, edge_meta)
           edge_meta: 1 for stargate, isotopes for cyno
    """
    INF_INT = 10**18
    INF_FLOAT = float("inf")

    def inf_cost() -> Cost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT)

    def gate_allowed(p: int, u: int) -> bool:
        # This function evaluates forward edge p -> u (stargate)
        if p in forced_cyno_origins:
            return False

        su = systems[u].sec

        # Hard bans (destination-based)
        if is_null_or_worse(su):
            return False
        if u in gank_lowsec and su < LOWSEC_THRESHOLD:
            return False

        # Lowsec destination rules
        if is_lowsec(su):
            allow_case1 = base_has_lowsec.get(p, False) and (systems[u].cyno_jump_security in ("safe", "risky"))
            allow_case2 = (systems[p].sec >= LOWSEC_THRESHOLD) and lowsec_has_highsec_neighbor.get(u, False)
            return allow_case1 or allow_case2

        # Highsec destination always allowed (no extra restrictions)
        return True

    best_cost: Dict[int, Cost] = {}
    next_step: Dict[int, Tuple[int, str, int]] = {}

    # Start at destination
    start: Cost = (0, 0, 0, 0, 0, -1.0, 0, 0)  # neg_min_gate_sec starts at -1.0 (min sec starts at 1.0)
    best_cost[dest_id] = start

    heap: List[Tuple[Cost, int]] = []
    heapq.heappush(heap, (start, dest_id))

    while heap:
        cost_u, u = heapq.heappop(heap)
        if best_cost.get(u) != cost_u:
            continue

        # --- Stargate predecessors (p -> u) ---
        for p in gate_adj.get(u, []):
            if not gate_allowed(p, u):
                continue

            sp = systems[p].sec
            su = systems[u].sec

            risky, iso, cyno_j, low2high, gank_hi, neg_min, gates, inter = cost_u
            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            low2high2 = low2high + (1 if (sp < LOWSEC_THRESHOLD and su >= LOWSEC_THRESHOLD) else 0)
            gank_hi2 = gank_hi + (1 if (u != dest_id and u in gank_highsec and systems[u].sec >= LOWSEC_THRESHOLD) else 0)

            # minStargateSecurityStatus: minimum of all systems entered via stargate
            neg_min2 = max(neg_min, -round(su, 6))

            cand: Cost = (risky, iso, cyno_j, low2high2, gank_hi2, neg_min2, gates2, inter2)
            old = best_cost.get(p, inf_cost())

            better = cand < old
            equal = cand == old
            if better or (equal and u < next_step.get(p, (2**31 - 1, "", 0))[0]):
                best_cost[p] = cand
                next_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # --- Cyno predecessors (p -> u) ---
        for (p, iso_edge, dest_is_risky) in rev_cyno.get(u, []):
            if dest_is_risky and not allow_risky_cyno:
                continue

            risky, iso, cyno_j, low2high, gank_hi, neg_min, gates, inter = cost_u
            cand: Cost = (
                risky + (1 if dest_is_risky else 0),
                iso + iso_edge,
                cyno_j + 1,
                low2high,
                gank_hi,
                neg_min,
                gates,
                inter + 1,  # u can't be dest_id here (sec<0.45), but keep rule simple/deterministic
            )
            old = best_cost.get(p, inf_cost())

            better = cand < old
            equal = cand == old
            if better or (equal and u < next_step.get(p, (2**31 - 1, "", 0))[0]):
                best_cost[p] = cand
                next_step[p] = (u, EDGE_CYNO, iso_edge)
                heapq.heappush(heap, (cand, p))

    return best_cost, next_step


def reconstruct_steps(
    systems: Dict[int, System],
    dest_id: int,
    origin_id: int,
    next_step: Dict[int, Tuple[int, str, int]],
) -> List[List[Any]]:
    """
    Builds the compressed route steps array from origin to destination.
    """
    if origin_id == dest_id:
        return []

    steps_raw: List[Tuple[str, int, int]] = []
    cur = origin_id
    seen = set()

    while cur != dest_id:
        if cur in seen:
            # Safety against unexpected cycles (shouldn't happen in Dijkstra)
            return []
        seen.add(cur)

        ns = next_step.get(cur)
        if ns is None:
            return []

        nxt, etype, meta = ns
        steps_raw.append((etype, nxt, meta))
        cur = nxt

    # Compress stargate runs
    out: List[List[Any]] = []
    gate_count = 0
    gate_last_system: Optional[int] = None

    def flush_gates() -> None:
        nonlocal gate_count, gate_last_system
        if gate_count > 0 and gate_last_system is not None:
            out.append([EDGE_STARGATE, systems[gate_last_system].name, gate_count])
        gate_count = 0
        gate_last_system = None

    for etype, nxt, meta in steps_raw:
        if etype == EDGE_STARGATE:
            gate_count += meta  # meta == 1
            gate_last_system = nxt
        else:
            flush_gates()
            out.append([EDGE_CYNO, systems[nxt].name, meta])  # meta = isotopes for that jump

    flush_gates()
    return out


def write_jsonl_gz_atomic(path: str, rows: Iterable[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"

    # Ensure we leave no residues beyond tmp (which we replace atomically)
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solarsystems", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--ganksystems", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jita-name", default="Jita")
    args = ap.parse_args()

    systems, name_to_id = load_systems(args.solarsystems)
    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: Destination system '{args.jita_name}' not found in solarsystems.", file=sys.stderr)
        return 2

    # Origins to output: all systems that contain stargates (appear in the stargate graph)
    origin_ids = sorted(gate_adj.keys())

    gank_ids = load_ganksystems_ids(args.ganksystems, name_to_id)
    gank_lowsec = {sid for sid in gank_ids if systems.get(sid) and systems[sid].sec < LOWSEC_THRESHOLD}
    gank_highsec = {sid for sid in gank_ids if systems.get(sid) and systems[sid].sec >= LOWSEC_THRESHOLD}

    # User decision: gank systems with sec<0.45 CAN be origin, but must exit via cyno; if not possible => no route.
    forced_cyno_origins = set(gank_lowsec)

    # Base routeSDEsafe100 (stargate-only, imitating CCP safer cost fn with penalty=100)
    _, base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, _base_min_id = compute_base_flags(systems, base_next, jita_id)

    lowsec_has_highsec_neighbor = compute_lowsec_has_highsec_neighbor(systems, gate_adj)

    # Build reverse cyno edges by brute force (as requested)
    rev_cyno = build_reverse_cyno_edges_bruteforce(systems)

    # Run final routing twice:
    # 1) safe-only cyno
    # 2) safe + risky cyno
    best1, next1 = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        base_has_lowsec=base_has_lowsec,
        gank_lowsec=gank_lowsec,
        gank_highsec=gank_highsec,
        forced_cyno_origins=forced_cyno_origins,
        lowsec_has_highsec_neighbor=lowsec_has_highsec_neighbor,
        allow_risky_cyno=False,
    )

    best2, next2 = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        base_has_lowsec=base_has_lowsec,
        gank_lowsec=gank_lowsec,
        gank_highsec=gank_highsec,
        forced_cyno_origins=forced_cyno_origins,
        lowsec_has_highsec_neighbor=lowsec_has_highsec_neighbor,
        allow_risky_cyno=True,
    )

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]
        if oid == jita_id:
            return {
                "solarSystem": o.name,
                "hasRoute": True,
                "stargates": 0,
                "jumps": 0,
                "isotopes": 0,
                "route": [],
            }

        if oid in best1:
            cost = best1[oid]
            steps = reconstruct_steps(systems, jita_id, oid, next1)
        elif oid in best2:
            cost = best2[oid]
            steps = reconstruct_steps(systems, jita_id, oid, next2)
        else:
            return {
                "solarSystem": o.name,
                "hasRoute": False,
                "stargates": 0,
                "jumps": 0,
                "isotopes": 0,
                "route": [],
            }

        risky, isotopes, cyno_jumps, _low2high, _gank_hi, _neg_min, stargates, _inter = cost
        # risky is not a field in output; it's enforced by the 2-pass rule.
        return {
            "solarSystem": o.name,
            "hasRoute": True,
            "stargates": int(stargates),
            "jumps": int(cyno_jumps),
            "isotopes": int(isotopes),
            "route": steps,
        }

    # Full rebuild: write deterministically and atomically
    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)

    rows = (row_for_origin(oid) for oid in origin_ids)
    write_jsonl_gz_atomic(out_path, rows)

    print(f"OK: wrote {out_path} for {len(origin_ids)} origin systems (systems with stargates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
