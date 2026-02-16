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

# Cyno constraints (user)
MAX_CYNO_DIST_M = 94_600_000_000_000_000  # 94600000000000000 m
ISO_NUM = 16565
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000


EDGE_STARGATE = "stargate"
EDGE_CYNO = "cyno"


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
        cyno = str(row.get("cynoJumpSecurity", "no jump")).strip().lower()

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
    # Expected: "A ↔ B" (or "<->")
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
    # Fallback: "A → B" (or "->")
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
    Undirected adjacency graph from stargates.jsonl.gz using stargateGroup pairs.
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

    # Deterministic ordering
    return {k: sorted(vs) for k, vs in adj.items()}


def load_ganksystems_ids_txt(ganksystems_txt: str, name_to_id: Dict[str, int]) -> Set[int]:
    out: Set[int] = set()
    with open(ganksystems_txt, "rt", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            sid = name_to_id.get(s)
            if sid is not None:
                out.add(sid)
    return out


# -----------------------------
# Base routeSDEsafe100 (stargate-only, ESI-like Safer + penalty=100)
# -----------------------------

def safer_cost(sec_to: float, penalty_cost: float) -> float:
    # CCP "Safer": (documented in their route calculation guide)
    # if sec <= 0.0 -> 2*penalty_cost
    # elif sec < 0.45 -> penalty_cost
    # else -> 0.90
    if sec_to <= 0.0:
        return 2.0 * penalty_cost
    if sec_to < LOWSEC_THRESHOLD:
        return penalty_cost
    return 0.90


def dijkstra_base_safer100(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    dest_id: int,
) -> Dict[int, int]:
    """
    Reverse Dijkstra from dest_id (only stargates) with Safer(100) costs.
    Returns next_hop[node] = next system towards dest in best base route.
    Deterministic tie-break: if equal dist, pick smaller next hop system_id.
    """
    penalty_cost = math.exp(0.15 * 100.0)

    INF = float("inf")
    dist: Dict[int, Tuple[float, int]] = {dest_id: (0.0, 0)}  # (cost, hops)
    next_hop: Dict[int, int] = {}
    heap: List[Tuple[float, int, int]] = [(0.0, 0, dest_id)]
    heapq.heapify(heap)

    while heap:
        cost_u, hops_u, u = heapq.heappop(heap)
        cur = dist.get(u)
        if cur is None or cur[0] != cost_u or cur[1] != hops_u:
            continue

        # predecessors p -> u (undirected graph)
        for p in gate_adj.get(u, []):
            inc = safer_cost(systems[u].sec, penalty_cost)
            cand = (cost_u + inc, hops_u + 1)
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
    For each system:
      - has_lowsec: base route contains any system with sec<0.45 excluding origin/dest
      - min_id: min solarSystemID on base route excluding origin/dest (INF if none)
    Iterative (no recursion).
    """
    INF_ID = 2**31 - 1
    has_low: Dict[int, bool] = {}
    min_id: Dict[int, int] = {}

    for start in systems.keys():
        if start in has_low:
            continue

        path: List[int] = []
        cur = start
        seen: Set[int] = set()

        while True:
            if cur == dest_id:
                has_low[cur] = False
                min_id[cur] = INF_ID
                break
            if cur in has_low:
                break
            if cur in seen:
                # cycle (should not happen), mark as no flags
                has_low[cur] = False
                min_id[cur] = INF_ID
                break

            seen.add(cur)
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


# -----------------------------
# Final routing (stargates + cyno) with your constraints
# -----------------------------

def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def lowsec_has_highsec_neighbor(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
) -> Dict[int, bool]:
    """
    For each lowsec system, whether it has at least one stargate neighbor with sec>=0.45.
    """
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


def cyno_dest_eligible(s: System) -> bool:
    # Must be sec < 0.45 and grade safe/risky
    if s.sec >= LOWSEC_THRESHOLD:
        return False
    if s.cyno_jump_security not in ("safe", "risky"):
        return False
    # Forbidden: sec<=0 AND faction==null
    if s.sec <= 0.0 and s.faction is None:
        return False
    return True


def isotopes_for_distance(dist_m: float) -> int:
    # ceil(dist * ISO_NUM / ISO_DEN)
    return int(math.ceil(dist_m * ISO_NUM / ISO_DEN))


def build_reverse_cyno_edges_bruteforce(
    systems: Dict[int, System],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    Reverse cyno adjacency:
      rev[dest] = [(origin, isotopes, dest_is_risky), ...]

    Brute force as requested, with squared-distance prefilter.
    """
    eligible_dests: List[Tuple[int, float, float, float, bool]] = []
    for s in systems.values():
        if cyno_dest_eligible(s):
            eligible_dests.append((s.system_id, s.x, s.y, s.z, s.cyno_jump_security == "risky"))

    r2 = float(MAX_CYNO_DIST_M) * float(MAX_CYNO_DIST_M)
    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in eligible_dests}

    all_systems = list(systems.values())

    for o in all_systems:
        ox, oy, oz = o.x, o.y, o.z
        oid = o.system_id
        for did, dx, dy, dz, dest_is_risky in eligible_dests:
            if did == oid:
                continue
            ddx = ox - dx
            ddy = oy - dy
            ddz = oz - dz
            d2 = ddx * ddx + ddy * ddy + ddz * ddz
            if d2 > r2:
                continue
            dist = math.sqrt(d2)
            iso = isotopes_for_distance(dist)
            if iso < 1:
                iso = 1
            rev[did].append((oid, iso, dest_is_risky))

    # deterministic order
    for did in rev:
        rev[did].sort(key=lambda t: (t[0], t[1], t[2]))
    return rev


# Cost vector for lexicographic ordering (your exclusion rule)
# 1) risky_cyno_jumps (min)
# 2) isotopes (min)
# 3) cyno_jumps (min)
# 4) lowsec->highsec stargates (min)
# 5) gank-highsec entries (min)
# 6) max minStargateSecurityStatus (we store as negative-min to maximize; min in tuple)
# 7) stargates (min)
# 8) intermediate systems excluding origin/dest (min)
# 9) base_min_id (min) -- constant per origin; kept as last tie-break for determinism
Cost = Tuple[int, int, int, int, int, float, int, int, int]


def dijkstra_final(
    systems: Dict[int, System],
    gate_adj: Dict[int, List[int]],
    rev_cyno: Dict[int, List[Tuple[int, int, bool]]],
    dest_id: int,
    gank_ids: Set[int],
    base_has_lowsec: Dict[int, bool],
    base_min_id: Dict[int, int],
    lowsec_has_hisec_nb: Dict[int, bool],
    forced_cyno_origins: Set[int],
    allow_risky_cyno: bool,
) -> Tuple[Dict[int, Cost], Dict[int, Tuple[int, str, int]]]:
    """
    Reverse Dijkstra from destination with mixed edges.

    next_step[p] = (next_node, edge_type, edge_meta)
      - stargate: meta = 1
      - cyno: meta = isotopes
    """
    INF_INT = 10**18
    INF_FLOAT = float("inf")
    INF_ID = 2**31 - 1

    def inf_cost() -> Cost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT, INF_ID)

    def gate_allowed(p: int, u: int) -> bool:
        # forward edge p -> u (stargate), must obey your stargate rules
        if p in forced_cyno_origins:
            return False

        su = systems[u].sec

        # Hard bans: cannot enter via stargate
        if su <= 0.0:
            return False
        if u in gank_ids and su < LOWSEC_THRESHOLD:
            return False

        # lowsec destination rules
        if is_lowsec(su):
            case1 = base_has_lowsec.get(p, False) and (systems[u].cyno_jump_security in ("safe", "risky"))
            case2 = (systems[p].sec >= LOWSEC_THRESHOLD) and lowsec_has_hisec_nb.get(u, False)
            return case1 or case2

        # highsec destination ok
        return True

    best: Dict[int, Cost] = {}
    next_step: Dict[int, Tuple[int, str, int]] = {}

    # Start at destination
    # neg_minGateSec: start at -1.0 meaning minGateSec starts at 1.0 (max possible)
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

            (risky_c, iso_c, cyno_c, low2hi_c, gank_hi_c, neg_min_c, gates_c, inter_c, _bm) = cost_u

            sec_p = systems[p].sec
            sec_u = systems[u].sec

            gates2 = gates_c + 1
            inter2 = inter_c + (0 if u == dest_id else 1)

            low2hi2 = low2hi_c + (1 if (sec_p < LOWSEC_THRESHOLD and sec_u >= LOWSEC_THRESHOLD) else 0)
            gank_hi2 = gank_hi_c + (1 if (u != dest_id and u in gank_ids and sec_u >= LOWSEC_THRESHOLD) else 0)

            # minStargateSecurityStatus is min over systems entered via stargate.
            # We want MAX of that minimum => store negated and minimize.
            neg_min2 = min(neg_min_c, -round(sec_u, 6))

            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (risky_c, iso_c, cyno_c, low2hi2, gank_hi2, neg_min2, gates2, inter2, bm_p)
            old = best.get(p, inf_cost())

            if cand < old or (cand == old and u < next_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                next_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # Cyno predecessors (p -> u)
        for (p, iso_edge, dest_is_risky) in rev_cyno.get(u, []):
            if dest_is_risky and not allow_risky_cyno:
                continue

            (risky_c, iso_c, cyno_c, low2hi_c, gank_hi_c, neg_min_c, gates_c, inter_c, _bm) = cost_u
            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (
                risky_c + (1 if dest_is_risky else 0),
                iso_c + iso_edge,
                cyno_c + 1,
                low2hi_c,
                gank_hi_c,
                neg_min_c,     # cyno doesn't affect minGateSec
                gates_c,       # cyno doesn't add stargates
                inter_c + 1,   # destination system becomes intermediate (except dest is Jita which can't be cyno target)
                bm_p,
            )
            old = best.get(p, inf_cost())

            if cand < old or (cand == old and u < next_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                next_step[p] = (u, EDGE_CYNO, iso_edge)
                heapq.heappush(heap, (cand, p))

    return best, next_step


def reconstruct_raw_steps(
    origin_id: int,
    dest_id: int,
    next_step: Dict[int, Tuple[int, str, int]],
) -> List[Tuple[str, int, int]]:
    """
    Raw steps: list of (edge_type, next_system_id, meta)
      - stargate meta=1
      - cyno meta=isotopes
    """
    if origin_id == dest_id:
        return []

    raw: List[Tuple[str, int, int]] = []
    cur = origin_id
    seen: Set[int] = set()

    while cur != dest_id:
        if cur in seen:
            return []
        seen.add(cur)

        ns = next_step.get(cur)
        if ns is None:
            return []
        nxt, etype, meta = ns
        raw.append((etype, nxt, meta))
        cur = nxt

    return raw


def compress_route(
    systems: Dict[int, System],
    raw: List[Tuple[str, int, int]],
) -> List[List[Any]]:
    """
    Output 'route' compression:
      - consecutive stargates are grouped: ["stargate", systemNameAfterRun, stargatesCount]
      - each cyno is a step: ["cyno", systemName, isotopes]
    """
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
            gate_count += meta  # meta == 1
            gate_last = nxt
        else:
            flush_gates()
            out.append([EDGE_CYNO, systems[nxt].name, meta])

    flush_gates()
    return out


def totals_from_raw(raw: List[Tuple[str, int, int]]) -> Tuple[int, int, int]:
    stargates = sum(1 for etype, _, __ in raw if etype == EDGE_STARGATE)
    jumps = sum(1 for etype, _, __ in raw if etype == EDGE_CYNO)
    isotopes = sum(meta for etype, _, meta in raw if etype == EDGE_CYNO)
    return stargates, jumps, isotopes


def write_jsonl_gz_atomic(path: str, rows: Iterable[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        # mtime=0 => deterministic gzip header time
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
    # Compatibility: workflows that still pass --stations won't break.
    ap.add_argument("--stations", default=None)
    args = ap.parse_args()

    systems, name_to_id = load_systems(args.solarsystems)
    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: destination system '{args.jita_name}' not found.", file=sys.stderr)
        return 2

    # Origins: all systems that contain stargates
    origin_ids = sorted(gate_adj.keys())

    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)
    # Origen gank lowsec: allowed as origin, but must exit via cyno only
    forced_cyno_origins = {sid for sid in gank_ids if systems[sid].sec < LOWSEC_THRESHOLD}

    # Base precompute (Safer+100, stargates only)
    base_next = dijkstra_base_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, jita_id)

    # Helpers for lowsec gate entry rule
    lowsec_has_hisec_nb = lowsec_has_highsec_neighbor(systems, gate_adj)

    # Cyno reverse edges (brute force)
    rev_cyno = build_reverse_cyno_edges_bruteforce(systems)

    # Two-pass final routing:
    # pass1: safe-only cyno
    # pass2: safe+risky
    best1, next1 = dijkstra_final(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        gank_ids=gank_ids,
        base_has_lowsec=base_has_lowsec,
        base_min_id=base_min_id,
        lowsec_has_hisec_nb=lowsec_has_hisec_nb,
        forced_cyno_origins=
