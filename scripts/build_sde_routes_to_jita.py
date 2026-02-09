#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import io
import json
import math
import os
import heapq
from typing import Dict, List, Optional, Tuple, Set


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

SOLARSYSTEMS_GZ = os.path.join(ROOT, "data", "sde", "solarsystems.jsonl.gz")
STARGATES_GZ = os.path.join(ROOT, "data", "sde", "stargates.jsonl.gz")
MAP_SYSTEMS = os.path.join(ROOT, "data", "test", "mapSolarSystems.jsonl")
OUT_GZ = os.path.join(ROOT, "data", "test", "SDEroutes.jsonl.gz")

DEST_NAME = "Jita"

# ESI route "Safer"
SECURITY_PENALTY = 100
PENALTY_COST = math.exp(0.15 * SECURITY_PENALTY)


def read_jsonl_gz(path: str):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_jsonl(path: str):
    with open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def safer_cost_to_enter(security_status: float) -> float:
    """
    Cost function per CCP Route Calculation guide for 'Safer':
      if sec <= 0.0: 2 * exp(0.15*penalty)
      elif sec < 0.45: exp(0.15*penalty)
      else: 0.90
    """
    if security_status <= 0.0:
        return 2.0 * PENALTY_COST
    if security_status < 0.45:
        return 1.0 * PENALTY_COST
    return 0.90


def parse_stargate_group(group: str) -> Optional[Tuple[str, str]]:
    """
    Expected: 'A ↔ B'
    Returns (A, B) stripped, or None if not parseable.
    """
    if not group:
        return None
    # Some files may use different spacing; normalize around the symbol.
    if "↔" in group:
        parts = group.split("↔")
        if len(parts) == 2:
            a = parts[0].strip()
            b = parts[1].strip()
            if a and b:
                return a, b
    return None


def parse_stargate_fallback(stargate: str) -> Optional[Tuple[str, str]]:
    """
    Fallback: 'A → B' (one direction line). We'll treat as an undirected connection
    at graph level (since group usually exists).
    """
    if not stargate:
        return None
    if "→" in stargate:
        parts = stargate.split("→")
        if len(parts) == 2:
            a = parts[0].strip()
            b = parts[1].strip()
            if a and b:
                return a, b
    return None


def load_systems() -> Tuple[Dict[int, str], Dict[str, int]]:
    id_to_name: Dict[int, str] = {}
    name_to_id: Dict[str, int] = {}

    for row in read_jsonl_gz(SOLARSYSTEMS_GZ):
        sid = int(row["solarSystemID"])
        name = str(row["solarSystem"])
        id_to_name[sid] = name
        # If duplicates ever existed, keep first; but SDE should be unique.
        name_to_id.setdefault(name, sid)

    return id_to_name, name_to_id


def load_security() -> Dict[int, float]:
    sec: Dict[int, float] = {}
    for row in read_jsonl(MAP_SYSTEMS):
        sid = int(row["_key"])
        # If missing, treat as <=0.0 later.
        sec[sid] = float(row.get("securityStatus", -1.0))
    return sec


def load_edges(name_to_id: Dict[str, int]) -> List[List[int]]:
    # Build undirected edges by parsing stargateGroup (preferred)
    # and mapping system names to IDs.
    edge_set: Set[Tuple[int, int]] = set()

    for row in read_jsonl_gz(STARGATES_GZ):
        a_b = parse_stargate_group(str(row.get("stargateGroup", "")))
        if a_b is None:
            a_b = parse_stargate_fallback(str(row.get("stargate", "")))
        if a_b is None:
            continue

        a_name, b_name = a_b
        a_id = name_to_id.get(a_name)
        b_id = name_to_id.get(b_name)
        if a_id is None or b_id is None or a_id == b_id:
            continue

        u, v = (a_id, b_id) if a_id < b_id else (b_id, a_id)
        edge_set.add((u, v))

    # adjacency list
    # We must size adjacency by max system id range? No, use dict then compact to list.
    # But we want O(1) neighbor iteration; simplest: dict[int, list[int]] then later map.
    adj: Dict[int, List[int]] = {}
    for u, v in edge_set:
        adj.setdefault(u, []).append(v)
        adj.setdefault(v, []).append(u)

    # Convert to dense list indexed by system id max for speed (ids are ~30M; too sparse).
    # So keep as dict-based adjacency. Return dict? We'll keep dict to avoid huge memory.
    # We'll store as list-of-lists via mapping from ids? Too heavy; keep dict in outer scope.
    # Here we return dict-like adjacency using a "list of lists" signature by wrapping later.
    # We'll actually return the dict as adjacency by type ignore at call site.
    return adj  # type: ignore[return-value]


def dijkstra_reverse_from_dest(
    dest_id: int,
    adjacency: Dict[int, List[int]],
    security: Dict[int, float],
) -> Tuple[Dict[int, float], Dict[int, Optional[int]]]:
    """
    Compute best cost-to-destination for all nodes, using reverse relaxation:
      dist[u] = dist[v] + cost_to_enter(v)
    where edge exists u<->v.

    Returns:
      dist: node->mincost to reach dest
      next_hop: node->the neighbor to go next toward dest, or None (dest itself)
    """
    dist: Dict[int, float] = {dest_id: 0.0}
    next_hop: Dict[int, Optional[int]] = {dest_id: None}

    heap: List[Tuple[float, int]] = [(0.0, dest_id)]

    while heap:
        d_v, v = heapq.heappop(heap)
        if d_v != dist.get(v, float("inf")):
            continue

        # cost to enter v (when coming from any neighbor u -> v)
        sec_v = security.get(v, -1.0)
        w = safer_cost_to_enter(sec_v)

        for u in adjacency.get(v, []):
            cand = d_v + w
            d_u = dist.get(u, float("inf"))

            # Deterministic tie-break: if cost equal, pick smaller next-hop id
            if cand < d_u - 0.0:
                dist[u] = cand
                next_hop[u] = v
                heapq.heappush(heap, (cand, u))
            elif cand == d_u:
                # tie-break
                cur = next_hop.get(u)
                if cur is None or v < cur:
                    next_hop[u] = v

    return dist, next_hop


def count_stargates(origin_id: int, dest_id: int, next_hop: Dict[int, Optional[int]]) -> Optional[int]:
    if origin_id == dest_id:
        return 0

    # If origin not reachable, or chain breaks, return None
    if origin_id not in next_hop:
        return None

    hops = 0
    seen = set()
    cur = origin_id
    while cur != dest_id:
        if cur in seen:
            # safety against cycles (should not happen if next_hop from Dijkstra)
            return None
        seen.add(cur)

        nxt = next_hop.get(cur)
        if nxt is None:
            return None
        hops += 1
        cur = nxt

        # Just in case
        if hops > 100000:
            return None

    return hops


def main() -> None:
    # Load systems
    id_to_name, name_to_id = load_systems()

    dest_id = name_to_id.get(DEST_NAME)
    if dest_id is None:
        raise RuntimeError(f'Destination system "{DEST_NAME}" not found in {SOLARSYSTEMS_GZ}')

    # Load security status
    security = load_security()

    # Load edges / adjacency
    adjacency: Dict[int, List[int]] = load_edges(name_to_id)  # type: ignore[assignment]

    # Dijkstra reverse from Jita
    _, next_hop = dijkstra_reverse_from_dest(dest_id, adjacency, security)

    # Write output gz (overwrite)
    os.makedirs(os.path.dirname(OUT_GZ), exist_ok=True)
    with gzip.open(OUT_GZ, "wt", encoding="utf-8", newline="\n") as out:
        # Iterate all systems from solarsystems (authoritative list)
        # Sort by name for stable output (optional but helps diffs)
        for sid, sname in sorted(id_to_name.items(), key=lambda kv: kv[1]):
            hops = count_stargates(sid, dest_id, next_hop)
            record = {
                "solarSystem": sname,
                "route": f"{sname} → {DEST_NAME}",
                "stargates": hops,  # int or None
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
