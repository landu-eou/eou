#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
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


def classify_security(security_status: float) -> Tuple[bool, bool]:
    """
    Returns (is_low, is_null)
      low: 0 < sec < 0.45
      null: sec <= 0.0
    """
    is_null = security_status <= 0.0
    is_low = (security_status > 0.0) and (security_status < 0.45)
    return is_low, is_null


def parse_stargate_group(group: str) -> Optional[Tuple[str, str]]:
    """
    Expected: 'A ↔ B'
    Returns (A, B) stripped, or None if not parseable.
    """
    if not group:
        return None
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
    Fallback: 'A → B' (one direction line).
    We'll treat it as an undirected connection at graph level.
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
        name_to_id.setdefault(name, sid)

    return id_to_name, name_to_id


def load_security() -> Dict[int, float]:
    sec: Dict[int, float] = {}
    for row in read_jsonl(MAP_SYSTEMS):
        sid = int(row["_key"])
        sec[sid] = float(row.get("securityStatus", -1.0))
    return sec


def load_adjacency(name_to_id: Dict[str, int]) -> Dict[int, List[int]]:
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

    adj: Dict[int, List[int]] = {}
    for u, v in edge_set:
        adj.setdefault(u, []).append(v)
        adj.setdefault(v, []).append(u)

    # Determinismo: ordenar vecinos
    for k in adj:
        adj[k].sort()

    return adj


def dijkstra_reverse_from_dest(
    dest_id: int,
    adjacency: Dict[int, List[int]],
    security: Dict[int, float],
) -> Tuple[Dict[int, float], Dict[int, Optional[int]]]:
    """
    Reverse Dijkstra:
      dist[u] = dist[v] + cost_to_enter(v)
    where edge exists u<->v.

    next_hop[u] = v means from u go to v as the next system toward dest.
    """
    dist: Dict[int, float] = {dest_id: 0.0}
    next_hop: Dict[int, Optional[int]] = {dest_id: None}
    heap: List[Tuple[float, int]] = [(0.0, dest_id)]

    while heap:
        d_v, v = heapq.heappop(heap)
        if d_v != dist.get(v, float("inf")):
            continue

        sec_v = security.get(v, -1.0)
        w = safer_cost_to_enter(sec_v)

        for u in adjacency.get(v, []):
            cand = d_v + w
            d_u = dist.get(u, float("inf"))

            if cand < d_u:
                dist[u] = cand
                next_hop[u] = v
                heapq.heappush(heap, (cand, u))
            elif cand == d_u:
                # Tie-break determinista: next hop con menor ID
                cur = next_hop.get(u)
                if cur is None or v < cur:
                    next_hop[u] = v

    return dist, next_hop


def build_path(origin_id: int, dest_id: int, next_hop: Dict[int, Optional[int]]) -> Optional[List[int]]:
    """
    Returns list of system IDs including origin and dest, or None if unreachable.
    """
    if origin_id == dest_id:
        return [origin_id]

    if origin_id not in next_hop:
        return None

    path: List[int] = []
    seen = set()
    cur = origin_id

    while True:
        if cur in seen:
            return None
        seen.add(cur)

        path.append(cur)
        if cur == dest_id:
            return path

        nxt = next_hop.get(cur)
        if nxt is None:
            return None
        cur = nxt

        if len(path) > 200000:
            return None


def summarize_route_security(path_ids: List[int], security: Dict[int, float]) -> Tuple[int, int]:
    """
    Counts:
      sslow: 0 < sec < 0.45
      ssnull: sec <= 0.0
    includes origin and destination
    """
    sslow = 0
    ssnull = 0
    for sid in path_ids:
        sec = security.get(sid, -1.0)
        is_low, is_null = classify_security(sec)
        if is_low:
            sslow += 1
        if is_null:
            ssnull += 1
    return sslow, ssnull


def main() -> None:
    id_to_name, name_to_id = load_systems()

    dest_id = name_to_id.get(DEST_NAME)
    if dest_id is None:
        raise RuntimeError(f'Destination system "{DEST_NAME}" not found in {SOLARSYSTEMS_GZ}')

    security = load_security()
    adjacency = load_adjacency(name_to_id)

    _, next_hop = dijkstra_reverse_from_dest(dest_id, adjacency, security)

    os.makedirs(os.path.dirname(OUT_GZ), exist_ok=True)

    with gzip.open(OUT_GZ, "wt", encoding="utf-8", newline="\n") as out:
        # salida estable: por nombre
        for sid, sname in sorted(id_to_name.items(), key=lambda kv: kv[1]):
            path = build_path(sid, dest_id, next_hop)

            if path is None:
                record = {
                    "solarSystem": sname,
                    "route": f"{sname} → {DEST_NAME}",
                    "stargates": None,
                    "sslow": None,
                    "ssnull": None,
                }
            else:
                stargates = len(path) - 1
                sslow, ssnull = summarize_route_security(path, security)

                record = {
                    "solarSystem": sname,
                    "route": f"{sname} → {DEST_NAME}",
                    "stargates": stargates,
                    "sslow": sslow,
                    "ssnull": ssnull,
                }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
