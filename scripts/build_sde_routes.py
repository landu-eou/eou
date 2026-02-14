#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import os
import re
import sys
import heapq
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable, Any, Set

LOWSEC_THRESHOLD = 0.45

# Cyno constraints (user-provided)
MAX_CYNO_DIST_M = 94_600_000_000_000_000  # 94600000000000000 m
ISO_NUM = 2350
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


def load_ganksystems_ids(ganksystems_path: str, name_to_id: Dict[str, int]) -> Set[int]:
    """
    Accepts:
      - Valid JSON: array/object/etc.
      - Non-standard set-like format: { "Uedama", "Sivala", ... }
      - Plain text: one system name per line (optionally comma-separated)
    Returns a set of solarSystemID.
    """
    text = ""
    with open(ganksystems_path, "rt", encoding="utf-8") as f:
        text = f.read()

    # 1) Try strict JSON
    data = None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None

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
            if "solarSystemID" in x and isinstance(x["solarSystemID"], int):
                found.add(int(x["solarSystemID"]))
            if "solarSystem" in x and isinstance(x["solarSystem"], str):
                sid = name_to_id.get(x["solarSystem"].strip())
                if sid is not None:
                    found.add(sid)
            for v in x.values():
                visit(v)
            return

    if data is not None:
        visit(data)
        return found

    # 2) Fallback: extract quoted strings
    quoted = re.findall(r'"([^"]+)"', text)
    if quoted:
        for name in quoted:
            sid = name_to_id.get(name.strip())
            if sid is not None:
                found.add(sid)
        return found

    # 3) Fallback: plain text lines
    cleaned = text.replace("{", "\n").replace("}", "\n")
    for line in cleaned.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.endswith(","):
            line = line[:-1].strip()
        parts = [p.strip() for p in line.split(",") if p.strip()]
        for p in parts:
            if len(p) >= 2 and p[0] == p[-1] == '"':
                p = p[1:-1].strip()
            sid = name_to_id.get(p)
            if sid is not None:
                found.add(sid)

    return found


# -----------------------------
# routeSDEsafe100 (stargate-only) - CCP Safer imitation
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

    Distance is (total_cost, gate_count) for deterministic tie-breaking.
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
            better = cand < old
            equal = cand == old
            if better or (equal and u < next_hop.get(p, 2**31 - 1)):
                dist[p] = cand
                next_hop[p] = u
                heapq.heappush(heap, (new_cost, new_gates, p))

    return dist, next_hop


def compute_base_has_lowsec(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
) -> Dict[int, bool]:
    """
    For each node, true if base route (excluding origin and dest) includes any system with sec < 0.45.
    This defines your [Tipo I] when combined with sec>=0.45.
    """
    memo: Dict[int, bool] = {}
    sys.setrecursionlimit(20000)

    def dfs(n: int) -> bool:
        if n == dest_id:
            return False
        if n in memo:
            return memo[n]
        nxt = base_next.get(n)
        if nxt is None:
            memo[n] = False
            return False
        child = dfs(nxt)
        here = child
        if nxt != dest_id and systems[nxt].sec < LOWSEC_THRESHOLD:
            here = True
        memo[n] = here
        return here

    for sid in systems.keys():
        dfs(sid)

    return memo


# -----------------------------
# Types according to your new norms
# -----------------------------

def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def is_nl(sec: float) -> bool:
    # [Tipo NL]: securityStatus < 0.45 (includes lowsec + sec<=0)
    return sec < LOWSEC_THRESHOLD


def build_type_flags(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    gank_ids: Set[int],
    base_has_lowsec: Dict[int, bool],
) -> Dict[str, Set[int]]:
    """
    Build sets for:
      S, Hg, HG, Lg, LD, LDg, I, NL
    """
    has_gate: Set[int] = set(gate_adj.keys())

    S: Set[int] = set()
    Hg: Set[int] = set()
    HG: Set[int] = set()
    Lg: Set[int] = set()
    LD: Set[int] = set()
    LDg: Set[int] = set()
    I: Set[int] = set()
    NL: Set[int] = set()

    for sid, s in systems.items():
        if sid in has_gate and s.sec <= 1.0:
            S.add(sid)

        if s.sec >= LOWSEC_THRESHOLD and sid in has_gate:
            if sid in gank_ids:
                HG.add(sid)
            else:
                Hg.add(sid)

        if is_lowsec(s.sec):
            if sid not in gank_ids:
                Lg.add(sid)

        if is_lowsec(s.sec) and s.cyno_jump_security in ("safe", "risky"):
            LD.add(sid)
            if sid not in gank_ids:
                LDg.add(sid)

        if is_nl(s.sec):
            NL.add(sid)

        # [Tipo I]: sec>=0.45 AND base route includes lowsec
        if s.sec >= LOWSEC_THRESHOLD and base_has_lowsec.get(sid, False):
            I.add(sid)

    return {
        "has_gate": has_gate,
        "S": S,
        "Hg": Hg,
        "HG": HG,
        "Lg": Lg,
        "LD": LD,
        "LDg": LDg,
        "I": I,
        "NL": NL,
    }


# -----------------------------
# Final routing (stargate + cyno) with your lexicographic rules
# -----------------------------

Cost = Tuple[int, int, int, int, int, float, int, int]
# (risky_cyno, isotopes, cyno_jumps, low_to_high_gate, gank_highsec_entries, neg_min_gate_sec, stargates, intermediate_systems)

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cyno"


def build_reverse_cyno_edges_bruteforce_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    Reverse cyno adjacency for *destinations only in [Tipo LD]*, brute force.
    dest -> list of (origin, isotopes, dest_is_risky)
    """
    eligible_dests: List[Tuple[int, float, float, float, bool]] = []
    for did in LD:
        d = systems[did]
        eligible_dests.append((did, d.x, d.y, d.z, d.cyno_jump_security == "risky"))

    r2 = float(MAX_CYNO_DIST_M) * float(MAX_CYNO_DIST_M)
    rev: Dict[int, List[Tuple[int, int, bool]]] = {d[0]: [] for d in eligible_dests}

    all_systems = list(systems.values())
    for origin in all_systems:
        ox, oy, oz = origin.x, origin.y, origin.z
        oid = origin.system_id

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
    allow_risky_cyno: bool,
) -> Tuple[Dict[int, Cost], Dict[int, Tuple[int, str, int]]]:
    """
    Reverse Dijkstra from dest_id using lexicographic vector cost.

    Uses your new stargate rules:
      - General: [S] Ø [NL] and [NL] Ø [S]
      - Exception: [I] -> [LDg]
      - Exception: [Lg] -> [Hg]
      - Highsec movement allowed: [Hg] -> [HG] -> [Hg]
      - Specific restriction: [Lg] Ø [HG] (do not enter highsec gank from lowsec non-gank)

    Cyno:
      - Only to [LD], no exceptions
      - Risky cyno only if allow_risky_cyno=True (2-pass selection implemented in main)
    """
    has_gate = type_sets["has_gate"]
    S = type_sets["S"]
    NL = type_sets["NL"]
    I = type_sets["I"]
    LDg = type_sets["LDg"]
    Lg = type_sets["Lg"]
    Hg = type_sets["Hg"]
    HG = type_sets["HG"]

    INF_INT = 10**18
    INF_FLOAT = float("inf")

    def inf_cost() -> Cost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT)

    def gate_allowed(p: int, u: int) -> bool:
        # Evaluate forward stargate edge p -> u under your rules.
        p_in_S = p in S
        u_in_S = u in S
        p_in_NL = p in NL
        u_in_NL = u in NL

        # If either endpoint has no stargate in SDE graph, no gate traversal
        if p not in has_gate or u not in has_gate:
            return False

        # General bans:
        # [S] Ø [NL]
        if p_in_S and u_in_NL:
            # Exception: [I] -> [LDg]
            if (p in I) and (u in LDg):
                return True
            return False

        # [NL] Ø [S]
        if p_in_NL and u_in_S:
            # Exception: [Lg] -> [Hg], but also enforce [Lg] Ø [HG]
            if (p in Lg) and (u in Hg):
                return True
            return False

        # If both highsec (>=0.45) or both not in NL/S conflict, allow.
        # Highsec gank/no-gank movement is allowed (Hg <-> HG).
        # (This also allows lowsec<->lowsec traversal in principle, but lowsec is NL and S is true,
        #  so lowsec->lowsec is covered by [S] Ø [NL] as p_in_S and u_in_NL => forbidden unless I->LDg.
        #  That's intended per your rule.)
        return True

    best_cost: Dict[int, Cost] = {}
    next_step: Dict[int, Tuple[int, str, int]] = {}

    # Start at destination
    start: Cost = (0, 0, 0, 0, 0, -1.0, 0, 0)
    best_cost[dest_id] = start

    heap: List[Tuple[Cost, int]] = []
    heapq.heappush(heap, (start, dest_id))

    while heap:
        cost_u, u = heapq.heappop(heap)
        if best_cost.get(u) != cost_u:
            continue

        # Stargate predecessors (p -> u)
        for p in gate_adj.get(u, []):
            if not gate_allowed(p, u):
                continue

            sp = systems[p].sec
            su = systems[u].sec

            risky, iso, cyno_j, low2high, gank_hi, neg_min, gates, inter = cost_u

            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            # Metric #4:
            # "stargate lleve desde sistema con securityStatus < 0.45 hacia sistema con 0.45 <= securityStatus"
            low2high2 = low2high + (1 if (sp < LOWSEC_THRESHOLD and su >= LOWSEC_THRESHOLD) else 0)

            # Metric #5: count entries/nodes intermediate that are ganksystems with sec>=0.45
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (systems[u].sec >= LOWSEC_THRESHOLD)) else 0)

            # Metric #6: max(minStargateSecurityStatus) -> store negative of min entered-by-gate sec
            # We update with the security of the system we ENTER via gate (u).
            neg_min2 = max(neg_min, -round(su, 6))

            cand: Cost = (risky, iso, cyno_j, low2high2, gank_hi2, neg_min2, gates2, inter2)
            old = best_cost.get(p, inf_cost())

            better = cand < old
            equal = cand == old
            if better or (equal and u < next_step.get(p, (2**31 - 1, "", 0))[0]):
                best_cost[p] = cand
                next_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # Cyno predecessors (p -> u)
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
                inter + 1,
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
    if origin_id == dest_id:
        return []

    steps_raw: List[Tuple[str, int, int]] = []
    cur = origin_id
    seen = set()

    while cur != dest_id:
        if cur in seen:
            return []
        seen.add(cur)

        ns = next_step.get(cur)
        if ns is None:
            return []

        nxt, etype, meta = ns
        steps_raw.append((etype, nxt, meta))
        cur = nxt

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
            gate_count += meta
            gate_last_system = nxt
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

    # Origins: all systems that contain stargates (as requested)
    origin_ids = sorted(gate_adj.keys())

    gank_ids = load_ganksystems_ids(args.ganksystems, name_to_id)

    # Base routeSDEsafe100 (stargate-only, Safer+100)
    _, base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec = compute_base_has_lowsec(systems, base_next, jita_id)

    # Build type sets under your new norms
    type_sets = build_type_flags(systems, gate_adj, gank_ids, base_has_lowsec)

    # Cyno: ONLY to [Tipo LD]
    rev_cyno = build_reverse_cyno_edges_bruteforce_LD_only(systems, type_sets["LD"])

    # Final routing twice (risky only if needed)
    best_safe, next_safe = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        allow_risky_cyno=False,
    )

    best_risky, next_risky = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
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

        # Prefer safe-only solution; if none, allow risky.
        if oid in best_safe:
            cost = best_safe[oid]
            steps = reconstruct_steps(systems, jita_id, oid, next_safe)
        elif oid in best_risky:
            cost = best_risky[oid]
            steps = reconstruct_steps(systems, jita_id, oid, next_risky)
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
        return {
            "solarSystem": o.name,
            "hasRoute": True,
            "stargates": int(stargates),
            "jumps": int(cyno_jumps),
            "isotopes": int(isotopes),
            "route": steps,
        }

    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)

    rows = (row_for_origin(oid) for oid in origin_ids)
    write_jsonl_gz_atomic(out_path, rows)

    print(f"OK: wrote {out_path} for {len(origin_ids)} origin systems (systems with stargates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
