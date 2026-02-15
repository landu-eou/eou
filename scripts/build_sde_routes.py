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
    One system name per line.
    Allows blank lines and comments with # (also inline: "Uedama # comment").
    """
    out: Set[int] = set()
    with open(ganksystems_txt, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "#" in line:
                line = line.split("#", 1)[0].strip()
            if not line:
                continue
            sid = name_to_id.get(line)
            if sid is not None:
                out.add(sid)
    return out


# -----------------------------
# routeSDEsafe100 (stargate-only)
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
# Types
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
        if sid in has_gate and s.sec <= 1.0:
            S.add(sid)

        if is_nl(s.sec):
            NL.add(sid)

        if sid in has_gate and s.sec >= LOWSEC_THRESHOLD:
            if sid in gank_ids:
                HG.add(sid)
            else:
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
        "HG": HG,
        "Lg": Lg,
        "LD": LD,
        "LDg": LDg,
        "I": I,
    }


# -----------------------------
# Final routing + counters
# -----------------------------

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"

# Cost order:
# 1 cynoJumps, 2 jumpFuel, 3 riskyCynoJumps, 4 low->high gate count,
# 5 gank highsec entries, 6 maximize minStargateSecurityStatus,
# 7 stargates, 8 intermediate systems, 9 base_min_id
Cost = Tuple[int, int, int, int, int, float, int, int, int]


def build_reverse_cyno_edges_bruteforce_LD_only(
    systems: Dict[int, System],
    LD: Set[int],
) -> Dict[int, List[Tuple[int, int, bool]]]:
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
    HG = type_sets["HG"]

    INF_INT = 10**18
    INF_FLOAT = float("inf")
    INF_ID = 2**31 - 1

    def inf_cost() -> Cost:
        return (INF_INT, INF_INT, INF_INT, INF_INT, INF_INT, INF_FLOAT, INF_INT, INF_INT, INF_ID)

    def gate_allowed(p: int, u: int) -> bool:
        if p not in has_gate or u not in has_gate:
            return False

        # explicit [Lg] Ø [HG]
        if (p in Lg) and (u in HG):
            return False

        # [S] Ø [NL] unless [I] -> [LDg]
        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        # [NL] Ø [S] unless [Lg] -> [Hg]
        if (p in NL) and (u in S):
            return (p in Lg) and (u in Hg)

        return True

    best: Dict[int, Cost] = {}
    nxt_step: Dict[int, Tuple[int, str, int]] = {}

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

            cyno_j, fuel, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u

            gates2 = gates + 1
            inter2 = inter + (0 if u == dest_id else 1)

            low2high2 = low2high + (1 if (sec_p < LOWSEC_THRESHOLD and sec_u >= LOWSEC_THRESHOLD) else 0)
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (sec_u >= LOWSEC_THRESHOLD)) else 0)
            neg_min2 = max(neg_min, -round(sec_u, 6))

            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (cyno_j, fuel, risky_j, low2high2, gank_hi2, neg_min2, gates2, inter2, bm_p)
            old = best.get(p, inf_cost())

            better = cand < old
            equal = cand == old
            if better or (equal and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_STARGATE, 1)
                heapq.heappush(heap, (cand, p))

        # Cyno predecessors (p -> u)
        for (p, fuel_edge, dest_is_risky) in rev_cyno.get(u, []):
            cyno_j, fuel, risky_j, low2high, gank_hi, neg_min, gates, inter, _bm = cost_u
            bm_p = base_min_id.get(p, INF_ID)

            cand: Cost = (
                cyno_j + 1,
                fuel + fuel_edge,
                risky_j + (1 if dest_is_risky else 0),
                low2high,
                gank_hi,
                neg_min,
                gates,
                inter + 1,
                bm_p,
            )
            old = best.get(p, inf_cost())

            better = cand < old
            equal = cand == old
            if better or (equal and u < nxt_step.get(p, (2**31 - 1, "", 0))[0]):
                best[p] = cand
                nxt_step[p] = (u, EDGE_CYNO, fuel_edge)
                heapq.heappush(heap, (cand, p))

    return best, nxt_step


def reconstruct_raw_hops(
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


def compress_steps(
    systems: Dict[int, System],
    raw: List[Tuple[str, int, int]],
) -> List[List[Any]]:
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


def compute_route_counters(
    systems: Dict[int, System],
    gank_ids: Set[int],
    raw: List[Tuple[str, int, int]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    safe = 0
    risky = 0

    hisec = 0
    midsec = 0
    ganksec = 0
    lowsec = 0

    for etype, nxt, _meta in raw:
        s = systems[nxt]

        if etype == EDGE_CYNO:
            if s.cyno_jump_security == "safe":
                safe += 1
            elif s.cyno_jump_security == "risky":
                risky += 1
        else:
            if s.system_id in gank_ids and s.sec >= LOWSEC_THRESHOLD:
                ganksec += 1
            elif s.sec >= 0.65 and s.system_id not in gank_ids:
                hisec += 1
            elif (LOWSEC_THRESHOLD <= s.sec < 0.65) and s.system_id not in gank_ids:
                midsec += 1
            elif (0.0 < s.sec < LOWSEC_THRESHOLD) and s.system_id not in gank_ids:
                lowsec += 1

    cyno_counts = {"safe": safe, "risky": risky}
    gate_counts = {"hisec": hisec, "midsec": midsec, "ganksec": ganksec, "lowsec": lowsec}
    return cyno_counts, gate_counts


def write_jsonl_gz_atomic(path: str, rows: Iterable[dict], inner_filename: str) -> None:
    """
    Atomic write + set gzip header filename to inner_filename.
    (El nombre "interno" se controla con el parámetro filename del constructor.)
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename=inner_filename) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
                    f.write("\n")

    os.replace(tmp, path)


def make_routescat_record(route_row: dict) -> dict:
    """
    Convert a routes.jsonl row into a category signature without solarSystem,
    with booleans meaning (>0).
    """
    has_route = bool(route_row.get("hasRoute", False))
    jump_fuel = int(route_row.get("jumpFuel", 0))

    cj = route_row.get("cynoJumps", {}) or {}
    sg = route_row.get("stargates", {}) or {}

    cj_count = int(cj.get("count", 0))
    cj_safe = int(cj.get("safe", 0))
    cj_risky = int(cj.get("risky", 0))

    sg_count = int(sg.get("count", 0))
    sg_hisec = int(sg.get("hisec", 0))
    sg_midsec = int(sg.get("midsec", 0))
    sg_ganksec = int(sg.get("ganksec", 0))
    sg_lowsec = int(sg.get("lowsec", 0))

    return {
        "hasRoute": has_route,
        "jumpFuel": (jump_fuel > 0),
        "cynoJumps": {
            "count": (cj_count > 0),
            "safe": (cj_safe > 0),
            "risky": (cj_risky > 0),
        },
        "stargates": {
            "count": (sg_count > 0),
            "hisec": (sg_hisec > 0),
            "midsec": (sg_midsec > 0),
            "ganksec": (sg_ganksec > 0),
            "lowsec": (sg_lowsec > 0),
        },
    }


def stable_key_cat(cat: dict) -> Tuple:
    """
    Stable sorting for deterministic output.
    """
    cj = cat["cynoJumps"]
    sg = cat["stargates"]
    return (
        cat["hasRoute"],
        cat["jumpFuel"],
        cj["count"], cj["safe"], cj["risky"],
        sg["count"], sg["hisec"], sg["midsec"], sg["ganksec"], sg["lowsec"],
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solarsystems", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--ganksystems", required=True)  # .txt
    ap.add_argument("--out", required=True)          # routes.jsonl.gz
    ap.add_argument("--out-cat", default=None)       # routesCat.jsonl.gz
    ap.add_argument("--jita-name", default="Jita")
    args = ap.parse_args()

    systems, name_to_id = load_systems(args.solarsystems)
    gate_adj = load_stargates_graph(args.stargates, name_to_id)

    jita_id = name_to_id.get(args.jita_name)
    if jita_id is None:
        print(f"ERROR: Destination system '{args.jita_name}' not found.", file=sys.stderr)
        return 2

    origin_ids = sorted(gate_adj.keys())
    gank_ids = load_ganksystems_ids_txt(args.ganksystems, name_to_id)

    base_next = dijkstra_route_sde_safer100(systems, gate_adj, jita_id)
    base_has_lowsec, base_min_id = compute_base_flags(systems, base_next, jita_id)

    type_sets = build_type_sets(systems, gate_adj, gank_ids, base_has_lowsec)
    rev_cyno = build_reverse_cyno_edges_bruteforce_LD_only(systems, type_sets["LD"])

    best, nxt = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=jita_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
    )

    # --- We'll collect unique category signatures while emitting routes rows
    cat_set: Set[str] = set()

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]

        if oid == jita_id:
            row = {
                "solarSystem": o.name,
                "hasRoute": True,
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
            }
        elif oid not in best:
            row = {
                "solarSystem": o.name,
                "hasRoute": False,
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
            }
        else:
            cyno_j, fuel_total, _risky_j, _low2high, _gank_hi, _neg_min, gates, _inter, _bm = best[oid]
            raw = reconstruct_raw_hops(jita_id, oid, nxt)
            steps = compress_steps(systems, raw)
            cyno_counts, gate_counts = compute_route_counters(systems, gank_ids, raw)

            # jumpFuel is doubled (global total), but per-cynoJump fuel stays in steps
            jump_fuel_out = int(fuel_total) * 2

            row = {
                "solarSystem": o.name,
                "hasRoute": True,
                "jumpFuel": int(jump_fuel_out),
                "cynoJumps": {
                    "count": int(cyno_j),
                    "safe": int(cyno_counts["safe"]),
                    "risky": int(cyno_counts["risky"]),
                },
                "stargates": {
                    "count": int(gates),
                    "hisec": int(gate_counts["hisec"]),
                    "midsec": int(gate_counts["midsec"]),
                    "ganksec": int(gate_counts["ganksec"]),
                    "lowsec": int(gate_counts["lowsec"]),
                },
                "route": steps,
            }

        # add category signature (no duplicates)
        cat = make_routescat_record(row)
        cat_set.add(json.dumps(cat, separators=(",", ":"), sort_keys=True))
        return row

    out_path = args.out
    if os.path.exists(out_path):
        os.remove(out_path)

    write_jsonl_gz_atomic(
        out_path,
        (row_for_origin(oid) for oid in origin_ids),
        inner_filename="routes.jsonl",
    )

    # Determine routesCat output path
    out_cat = args.out_cat
    if out_cat is None:
        # If out is ".../routes.jsonl.gz" -> ".../routesCat.jsonl.gz"
        base = out_path
        if base.endswith("routes.jsonl.gz"):
            out_cat = base[:-len("routes.jsonl.gz")] + "routesCat.jsonl.gz"
        else:
            out_cat = base + ".cat.jsonl.gz"

    if os.path.exists(out_cat):
        os.remove(out_cat)

    # Decode, sort deterministically, and write unique categories
    cats = [json.loads(s) for s in cat_set]
    cats.sort(key=stable_key_cat)

    write_jsonl_gz_atomic(
        out_cat,
        cats,
        inner_filename="routesCat.jsonl",
    )

    print(f"OK: wrote {out_path} (routes) and {out_cat} (unique categories={len(cats)}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
