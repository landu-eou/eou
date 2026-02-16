#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import gzip
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import heapq
import hashlib
import urllib.request
import urllib.error
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable, Any, Set

# -----------------------------
# ENV / Constants
# -----------------------------

WORKFLOW_FILE = os.environ.get("WORKFLOW_FILE", "eou_sde_routes_sde-gh.yml")
SHEETS_ID = os.environ["SHEETS_ID"]
SHEET_TAB = os.environ.get("SHEET_TAB", "workflows")
SHEETS_WORKFLOW_ROW = int(os.environ.get("SHEETS_WORKFLOW_ROW", "10"))
LOCK_TIME = int(os.environ.get("LOCK_TIME", "60"))

GOOGLE_OAUTH_ACCESS_TOKEN = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "").strip()

# Paths
PATH_SOLARSYSTEMS = "data/sde/solarsystems.jsonl.gz"
PATH_STARGATES = "data/sde/stargates.jsonl.gz"
PATH_STATIONS = "data/sde/stations.jsonl.gz"
PATH_GANKS = "data/sde/ganksystems.txt"  # tu repo lo tiene en .txt

OUT_ROUTES = "data/sde/routes.jsonl.gz"
STATE_ROUTES = "states/routes.json"

# Destination fixed station
DEST_STATION_ID = 60003760
DEST_STATION_NAME = "Jita IV - Moon 4 - Caldari Navy"

LOWSEC_THRESHOLD = 0.45
HISEC_THRESHOLD = 0.65

# Cyno parameters (tus valores)
MAX_CYNO_DIST_M = 94_600_000_000_000_000  # 94600000000000000 m
ISO_NUM = 16565
ISO_DEN = 9_460_000_000_000_000  # 9460000000000000

EDGE_STARGATE = "stargate"
EDGE_CYNO = "cynoJump"

CYNO_GRADE_RANK: Dict[str, int] = {
    "no jump": 0,
    "unsafe": 1,
    "risky": 2,
    "safe": 3,
}
CYNO_RANK_GRADE = {v: k for k, v in CYNO_GRADE_RANK.items()}

ETAG_FILES = [PATH_SOLARSYSTEMS, PATH_STARGATES, PATH_STATIONS, PATH_GANKS]

WORMHOLE_REGION_RE = re.compile(r"^[A-Z]-R\d{5}$")


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


# -----------------------------
# Sheets helpers (REST)
# -----------------------------

def sheets_a1(col_letter: str, row: int) -> str:
    return f"{SHEET_TAB}!{col_letter}{row}"


def sheets_serial_utc(epoch_seconds: float) -> float:
    # serial days since 1899-12-30
    return epoch_seconds / 86400.0 + 25569.0


def sheets_get_value(a1_range: str) -> Optional[str]:
    if not GOOGLE_OAUTH_ACCESS_TOKEN:
        die("GOOGLE_OAUTH_ACCESS_TOKEN missing.")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{urllib.parse.quote(a1_range)}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {GOOGLE_OAUTH_ACCESS_TOKEN}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            values = data.get("values", [])
            if not values or not values[0]:
                return None
            return str(values[0][0])
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        die(f"Sheets GET failed: {e.code} {body}")
    except Exception as e:
        die(f"Sheets GET failed: {e}")


def sheets_update_values(updates: Dict[str, Any]) -> None:
    if not GOOGLE_OAUTH_ACCESS_TOKEN:
        die("GOOGLE_OAUTH_ACCESS_TOKEN missing.")

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values:batchUpdate"
    data = [{"range": rng, "values": [[v]]} for rng, v in updates.items()]

    payload = {"valueInputOption": "RAW", "data": data}
    raw = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=raw,
        headers={
            "Authorization": f"Bearer {GOOGLE_OAUTH_ACCESS_TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _ = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        die(f"Sheets UPDATE failed: {e.code} {body}")
    except Exception as e:
        die(f"Sheets UPDATE failed: {e}")


def set_status(status: str) -> None:
    sheets_update_values({sheets_a1("B", SHEETS_WORKFLOW_ROW): status})


def set_status_next(status: str, next_serial: float) -> None:
    sheets_update_values({
        sheets_a1("B", SHEETS_WORKFLOW_ROW): status,
        sheets_a1("D", SHEETS_WORKFLOW_ROW): next_serial,
    })


def set_status_next_last(status: str, next_serial: float, last_serial: float) -> None:
    sheets_update_values({
        sheets_a1("B", SHEETS_WORKFLOW_ROW): status,
        sheets_a1("D", SHEETS_WORKFLOW_ROW): next_serial,
        sheets_a1("I", SHEETS_WORKFLOW_ROW): last_serial,
    })


# -----------------------------
# Git helpers
# -----------------------------

def run(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def git_config_bot() -> None:
    run(["git", "config", "user.name", "github-actions[bot]"])
    run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])


def git_pull_rebase() -> None:
    run(["git", "pull", "--rebase", "--autostash", "origin", "main"])


def git_commit_push(paths: List[str], message: str) -> None:
    run(["git", "add", "--"] + paths)
    st = run(["git", "status", "--porcelain"], check=True).stdout
    # si nada staged (o no aparece en status), no commit
    if not any(p in st for p in paths):
        return
    run(["git", "commit", "-m", message])
    run(["git", "push", "origin", "HEAD:main"])


# -----------------------------
# ETag state (pretty JSON)
# -----------------------------

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_state_etags(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    with open(path, "rt", encoding="utf-8") as f:
        obj = json.load(f)
    et = obj.get("etag", {})
    if isinstance(et, dict):
        return {str(k): str(v) for k, v in et.items()}
    return {}


def write_state_etags_atomic(path: str, etags: Dict[str, str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"

    ordered = dict(sorted(etags.items(), key=lambda x: x[0]))

    if os.path.exists(tmp):
        os.remove(tmp)

    with open(tmp, "wt", encoding="utf-8", newline="\n") as f:
        json.dump({"etag": ordered}, f, ensure_ascii=False, indent=2)
        f.write("\n")

    os.replace(tmp, path)


def compute_current_etags() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in ETAG_FILES:
        if not os.path.exists(p):
            die(f"Required input missing: {p}")
        out[p] = sha256_file(p)
    return out


# -----------------------------
# SDE parsing
# -----------------------------

def read_jsonl_gz(path: str) -> Iterable[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


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
    region: str
    constellation: str
    solar_system_type: str  # opcional; si no existe queda ""


@dataclass(frozen=True)
class Station:
    station_id: int
    name: str
    system_name: str
    cyno_dock_security: str


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
        region = str(row.get("region", "")).strip()
        constel = str(row.get("constellation", "")).strip()
        sstype = str(row.get("solarSystemType", row.get("type", ""))).strip().lower()

        s = System(
            system_id=sid,
            name=name,
            sec=sec,
            x=x, y=y, z=z,
            faction=faction if faction is not None else None,
            cyno_jump_security=cyno,
            region=region,
            constellation=constel,
            solar_system_type=sstype,
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
    out: Dict[int, str] = {}
    for sid, s in systems.items():
        if sid == dest_system_id:
            out[sid] = dest_station_name
        else:
            grade = system_cyno_grade.get(sid, s.cyno_jump_security)
            out[sid] = choose_station_for_system(s.name, grade, stations_by_sysname)
    return out


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
            s = line.strip()
            if s and not s.startswith("#"):
                sid = name_to_id.get(s)
                if sid is not None:
                    out.add(sid)
    return out


# -----------------------------
# routeSDEsafe100 (base)
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


def compute_base_has_lowsec_and_min_id(
    systems: Dict[int, System],
    base_next: Dict[int, int],
    dest_id: int,
) -> Tuple[Dict[int, bool], Dict[int, int]]:
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


# -----------------------------
# Types + Cyno edges
# -----------------------------

def is_lowsec(sec: float) -> bool:
    return 0.0 < sec < LOWSEC_THRESHOLD


def fuel_for_distance_m(dist_m: float) -> int:
    f = int(math.ceil(dist_m * ISO_NUM / ISO_DEN))
    return 1 if f < 1 else f


def build_reverse_cyno_edges_grid(
    systems: Dict[int, System],
    system_cyno_grade: Dict[int, str],
) -> Dict[int, List[Tuple[int, int, bool]]]:
    """
    rev[dest] = [(origin, fuel, dest_is_risky), ...]

    Destino cyno permitido:
      - sec < 0.45
      - grade in {safe,risky}
      - NO (sec <= 0 AND faction is null)

    ORIGEN cyno permitido (nuevo requisito):
      - SOLO desde sistemas con sec < 0.45
    """
    eligible_dests: List[Tuple[int, float, float, float, bool]] = []
    for s in systems.values():
        if s.sec >= LOWSEC_THRESHOLD:
            continue
        grade = norm_cyno_grade(system_cyno_grade.get(s.system_id, s.cyno_jump_security))
        if grade not in ("safe", "risky"):
            continue
        if s.sec <= 0.0 and s.faction is None:
            continue
        eligible_dests.append((s.system_id, s.x, s.y, s.z, grade == "risky"))

    cell = float(MAX_CYNO_DIST_M)
    r2 = cell * cell

    def cell_key(x: float, y: float, z: float) -> Tuple[int, int, int]:
        return (int(math.floor(x / cell)), int(math.floor(y / cell)), int(math.floor(z / cell)))

    grid: Dict[Tuple[int, int, int], List[Tuple[int, float, float, float, bool]]] = {}
    for did, x, y, z, is_risky in eligible_dests:
        grid.setdefault(cell_key(x, y, z), []).append((did, x, y, z, is_risky))

    rev: Dict[int, List[Tuple[int, int, bool]]] = {did: [] for (did, *_rest) in eligible_dests}

    # ORIGENES: solo sec < 0.45 (nuevo requisito)
    for o in systems.values():
        if o.sec >= LOWSEC_THRESHOLD:
            continue

        oid = o.system_id
        ok = cell_key(o.x, o.y, o.z)
        ox, oy, oz = o.x, o.y, o.z
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

        if s.sec < LOWSEC_THRESHOLD:
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
# FINAL selection criteria (new order)
# -----------------------------
# 1) cynoJumps
# 2) fuel
# 3) risky_present (0 => only-safe/none; 1 => some risky)  (rule #3)
# 4) low2high
# 5) gank_hi_entries
# 6) neg_minGateSec  (maximize min gate security)
# 7) stargates
# 8) intermediates
# 9) base_min_id
Cost = Tuple[int, int, int, int, int, float, int, int, int]


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

    # gank lowsec origins: permitidos como origen, pero no pueden salir por gate (solo cyno)
    forced_cyno_origins = {sid for sid in gank_ids if systems[sid].sec < LOWSEC_THRESHOLD}

    def gate_allowed(p: int, u: int) -> bool:
        if p in forced_cyno_origins:
            return False

        if p not in has_gate or u not in has_gate:
            return False

        # Reglas especiales lowsec<->hisec basadas en base_has_lowsec y LDg/Lg/Hg (idénticas a tu lógica previa)
        if (p in S) and (u in NL):
            return (p in I) and (u in LDg)

        if (p in NL) and (u in S):
            return (p in Lg) and (u in Hg)

        # BANS duros:
        su = systems[u]
        if su.sec <= 0.0:
            return False
        if su.sec < LOWSEC_THRESHOLD and u in gank_ids:
            return False
        return True

    best: Dict[int, Cost] = {}
    nxt_step: Dict[int, Tuple[int, str, int]] = {}

    # neg_minGateSec arranca en -1.0 (seguridad min inicial)
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

            # criterio #5: contar nodos intermedios ganksec (sec>=0.45 y en lista gank)
            gank_hi2 = gank_hi + (1 if (u != dest_id and (u in gank_ids) and (sec_u >= LOWSEC_THRESHOLD)) else 0)

            # criterio #6: max min gate security (usamos -minSec para min lexicográfico)
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
# Route reconstruction / formatting
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
    raw_edges: List[Tuple[str, int, int]],
) -> List[str]:
    # Requisito: NO incluir el sistema origen.
    expanded: List[str] = []

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

    return expanded


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
    risky_present: int,
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
    if has_cyno and risky_present > 0:
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


def solar_system_class(
    s: System,
    *,
    in_gank: bool,
    has_gate: bool,
    base_has_lowsec: bool,
) -> str:
    if s.region == "Pochven":
        return "pochven"
    if s.region == "Yasna Zakh":
        return "zarzakh"
    if s.region in ("A821-A", "J7HZ-F", "UUA-F4"):
        return "jove"

    if s.solar_system_type == "wormhole":
        return "wormhole"
    if WORMHOLE_REGION_RE.match(s.region or ""):
        return "wormhole"

    if in_gank and s.sec < LOWSEC_THRESHOLD:
        return "campsec"
    if in_gank and s.sec >= LOWSEC_THRESHOLD and has_gate:
        return "ganksec"

    if s.sec <= 0.0 and not in_gank:
        if s.faction is not None:
            return "npcnull"
        return "sovnull"

    if 0.0 < s.sec < LOWSEC_THRESHOLD and not in_gank:
        return "lowsec"

    if not in_gank and base_has_lowsec:
        if s.sec >= HISEC_THRESHOLD:
            return "hisland"
        if LOWSEC_THRESHOLD <= s.sec < HISEC_THRESHOLD:
            return "midsland"

    if not in_gank:
        if s.sec >= HISEC_THRESHOLD:
            return "hisec"
        if LOWSEC_THRESHOLD <= s.sec < HISEC_THRESHOLD:
            return "midsec"

    return "unknown"


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


# -----------------------------
# Build routes
# -----------------------------

def build_routes() -> None:
    systems, name_to_id = load_systems(PATH_SOLARSYSTEMS)
    stations_by_sysname, stations_by_id = load_stations(PATH_STATIONS)
    system_cyno_grade = compute_system_cyno_from_stations(systems, stations_by_sysname)

    dest_station = stations_by_id.get(DEST_STATION_ID)
    if dest_station is None:
        die(f"Destination station {DEST_STATION_ID} not found in {PATH_STATIONS}.")

    dest_system_id = name_to_id.get(dest_station.system_name)
    if dest_system_id is None:
        die(f"Destination system '{dest_station.system_name}' not found in solarsystems.")

    station_name_by_system_id = precompute_station_name_by_system_id(
        systems=systems,
        system_cyno_grade=system_cyno_grade,
        stations_by_sysname=stations_by_sysname,
        dest_system_id=dest_system_id,
        dest_station_name=DEST_STATION_NAME,
    )

    gate_adj = load_stargates_graph(PATH_STARGATES, name_to_id)
    origin_ids = sorted(gate_adj.keys())

    gank_ids = load_ganksystems_ids_txt(PATH_GANKS, name_to_id)

    base_next = dijkstra_route_sde_safer100(systems, gate_adj, dest_system_id)
    base_has_lowsec, base_min_id = compute_base_has_lowsec_and_min_id(systems, base_next, dest_system_id)

    type_sets = build_type_sets(
        systems=systems,
        system_cyno_grade=system_cyno_grade,
        gate_adj=gate_adj,
        gank_ids=gank_ids,
        base_has_lowsec=base_has_lowsec,
    )

    rev_cyno = build_reverse_cyno_edges_grid(systems, system_cyno_grade)

    best, nxt = dijkstra_final_routes(
        systems=systems,
        gate_adj=gate_adj,
        rev_cyno=rev_cyno,
        dest_id=dest_system_id,
        type_sets=type_sets,
        gank_ids=gank_ids,
        base_min_id=base_min_id,
    )

    has_gate = type_sets["has_gate"]

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
                    if s2.sec >= HISEC_THRESHOLD:
                        hisec += 1
                    else:
                        midsec += 1

        return safe_c, risky_c, hisec, midsec, ganksec, lowsec

    def row_for_origin(oid: int) -> dict:
        o = systems[oid]
        in_gank = oid in gank_ids

        cls = solar_system_class(
            o,
            in_gank=in_gank,
            has_gate=(oid in has_gate),
            base_has_lowsec=base_has_lowsec.get(oid, False),
        )

        if oid == dest_system_id:
            return {
                "solarSystem": o.name,
                "solarSystemClass": cls,
                "routeType": "highway 0",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [DEST_STATION_NAME],
            }

        if oid not in best:
            return {
                "solarSystem": o.name,
                "solarSystemClass": cls,
                "routeType": "no route",
                "jumpFuel": 0,
                "cynoJumps": {"count": 0, "safe": 0, "risky": 0},
                "stargates": {"count": 0, "hisec": 0, "midsec": 0, "ganksec": 0, "lowsec": 0},
                "route": [],
                "routExpanded": [],
            }

        raw_edges = reconstruct_raw_edges(dest_id=dest_system_id, origin_id=oid, nxt_step=nxt)
        compact_route = make_compact_route_with_stations(station_name_by_system_id, raw_edges)
        expanded = make_route_expanded(systems, station_name_by_system_id, raw_edges)

        cyno_j, fuel, risky_present, _low2high, _gank_hi, _neg_min, st_total, _inter, _bm = best[oid]

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
            "count": int(cyno_j),
            "safe": int(safe_c),
            "risky": int(risky_c),
        }

        rt = build_route_type(
            has_route=True,
            origin_is_jita=False,
            compact_route=compact_route,
            risky_present=int(risky_present),
            stargates_lowsec_count=int(lowsec),
            stargates_ganksec_count=int(ganksec),
            stargates_total=int(st_total),
        )

        return {
            "solarSystem": o.name,
            "solarSystemClass": cls,
            "routeType": rt,
            "jumpFuel": int(jump_fuel),
            "cynoJumps": cj_obj,
            "stargates": st_obj,
            "route": compact_route,
            "routExpanded": expanded,
        }

    if os.path.exists(OUT_ROUTES):
        try:
            os.remove(OUT_ROUTES)
        except OSError:
            pass

    write_jsonl_gz_atomic(OUT_ROUTES, (row_for_origin(oid) for oid in origin_ids))

    # quick gzip validate
    with gzip.open(OUT_ROUTES, "rb") as f:
        _ = f.read(1)


# -----------------------------
# Orchestration (lock + etag + build + commit + next_run)
# status(B10): in_progress / completed / failed
# -----------------------------

def main() -> int:
    now = time.time()
    now_serial = sheets_serial_utc(now)

    # Anti-colas: si next_run ya está en el futuro, salir sin tocar nada
    existing_next_raw = sheets_get_value(sheets_a1("D", SHEETS_WORKFLOW_ROW))
    try:
        existing_next = float(existing_next_raw) if existing_next_raw else 0.0
    except ValueError:
        existing_next = 0.0

    if existing_next > now_serial:
        set_status("completed")
        return 0

    # Lock inicial + status in_progress
    lock_serial = sheets_serial_utc(now + float(LOCK_TIME))
    set_status_next("in_progress", lock_serial)

    prev_etags = load_state_etags(STATE_ROUTES)
    cur_etags = compute_current_etags()

    changed = any(prev_etags.get(p) != cur_etags.get(p) for p in ETAG_FILES)

    if not changed:
        next6h = sheets_serial_utc(now + 6 * 3600.0)
        set_status_next("completed", next6h)
        return 0

    try:
        build_routes()
    except Exception as e:
        next10m = sheets_serial_utc(time.time() + 10 * 60.0)
        set_status_next("failed", next10m)
        print(f"Build failed: {e}", file=sys.stderr)
        return 1

    # Build OK -> write pretty state
    write_state_etags_atomic(STATE_ROUTES, cur_etags)

    # One commit (routes + state)
    git_config_bot()
    git_pull_rebase()
    git_commit_push(
        paths=[OUT_ROUTES, STATE_ROUTES],
        message="sde: rebuild routes.jsonl.gz (criteria update, no-cyno-from-hisec)",
    )

    now2 = time.time()
    next6h = sheets_serial_utc(now2 + 6 * 3600.0)
    last_mod = sheets_serial_utc(now2)
    set_status_next_last("completed", next6h, last_mod)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
