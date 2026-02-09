#!/usr/bin/env python3
from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple


# Inputs (en el repo)
SOLARSYSTEMS_GZ = "data/sde/solarsystems.jsonl.gz"
STARGATES_GZ = "data/sde/stargates.jsonl.gz"

# Outputs (TODO dentro de route_test/)
OUT_ROUTE_JSONL = "route_test/route.jsonl"
OUT_ROUTE_NAMES_JSON = "route_test/route_names.json"
OUT_META_JSON = "route_test/route.meta.json"


# ----------------------------
# Presupuesto / Heurística
# ----------------------------
# Workflow tiene timeout 20 min. Reservamos margen para parseo + escritura + commit.
SEARCH_BUDGET_SECONDS = 18 * 60  # 18 min búsqueda

K_CANDIDATES = 20          # K objetivos "cercanos" a evaluar desde current
LOOKAHEAD_TOP_L = 6        # calcular lookahead solo para los L mejores por distancia
LOOKAHEAD_LAMBDA = 1.0     # peso del lookahead
EPSILON = 0.12             # epsilon-greedy para diversificar
TOP_M_FOR_EPSILON = 3      # elige aleatorio entre top M con prob EPSILON
MAX_RESTARTS = 100000      # límite alto; manda el tiempo


# ----------------------------
# Utilidades
# ----------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"JSON inválido en {path}:{lineno}: {e}") from e
            if isinstance(obj, dict):
                yield obj


def normalize_int(x: Any) -> Optional[int]:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.isdigit():
            return int(s)
    return None


# ----------------------------
# Parseo de tus formatos
# ----------------------------
def build_system_maps() -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    solarsystems.jsonl.gz:
      {"solarSystemID":30000001,"solarSystem":"Tanoo",...}
    """
    id_to_name: Dict[int, str] = {}
    name_to_id: Dict[str, int] = {}

    for obj in iter_jsonl_gz(SOLARSYSTEMS_GZ):
        sid = normalize_int(obj.get("solarSystemID"))
        nm = obj.get("solarSystem")
        if sid is None:
            continue
        if not isinstance(nm, str) or not nm.strip():
            continue
        nm = nm.strip()
        id_to_name[sid] = nm
        name_to_id.setdefault(nm, sid)

    if not id_to_name:
        raise RuntimeError("No se pudo construir el mapa de sistemas (id_to_name vacío).")

    return id_to_name, name_to_id


def parse_gate_endpoints(obj: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    stargates.jsonl.gz:
      {"stargateGroup":"Arnatele ↔ Bereye","solarSystem":"Arnatele",...}
      {"stargate":"Arnatele → Junsoraert",...}
    """
    g = obj.get("stargateGroup")
    if isinstance(g, str) and "↔" in g:
        parts = [p.strip() for p in g.split("↔")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]

    s = obj.get("stargate")
    if isinstance(s, str) and "→" in s:
        parts = [p.strip() for p in s.split("→")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]

    return None


def build_adjacency(name_to_id: Dict[str, int]) -> Dict[int, List[int]]:
    """
    Grafo no-dirigido (↔). Devuelve listas ordenadas (determinismo).
    """
    adj_set: Dict[int, Set[int]] = {}

    for obj in iter_jsonl_gz(STARGATES_GZ):
        endpoints = parse_gate_endpoints(obj)
        if endpoints is None:
            continue
        a_name, b_name = endpoints
        a = name_to_id.get(a_name)
        b = name_to_id.get(b_name)
        if a is None or b is None or a == b:
            continue

        adj_set.setdefault(a, set()).add(b)
        adj_set.setdefault(b, set()).add(a)

    if not adj_set:
        raise RuntimeError("No se construyeron aristas (adj vacío). Revisa stargates.jsonl.gz.")

    return {u: sorted(vs) for u, vs in adj_set.items()}


def find_jita_id(name_to_id: Dict[str, int]) -> int:
    jita = name_to_id.get("Jita")
    if jita is not None:
        return jita
    for nm, sid in name_to_id.items():
        if nm.lower() == "jita":
            return sid
    raise RuntimeError("No se encontró el sistema 'Jita' en solarsystems.jsonl.gz")


# ----------------------------
# BFS helpers
# ----------------------------
def reachable_from(root: int, adj: Dict[int, List[int]]) -> Set[int]:
    seen: Set[int] = set()
    q: Deque[int] = deque([root])
    while q:
        u = q.popleft()
        if u in seen:
            continue
        seen.add(u)
        for v in adj.get(u, ()):
            if v not in seen:
                q.append(v)
    return seen


def bfs_collect_k_unvisited(
    start: int,
    adj: Dict[int, List[int]],
    unvisited: Set[int],
    k: int,
    deadline: float,
) -> Tuple[List[Tuple[int, int]], Dict[int, int], Dict[int, int]]:
    """
    BFS desde start hasta encontrar k nodos en unvisited.
    Devuelve:
      - candidates: [(node, dist), ...]
      - parent: para reconstruir path desde start
      - dist: distancias desde start (solo para nodos descubiertos)
    """
    parent: Dict[int, int] = {start: start}
    dist: Dict[int, int] = {start: 0}
    q: Deque[int] = deque([start])

    candidates: List[Tuple[int, int]] = []
    while q and len(candidates) < k:
        if time.monotonic() > deadline:
            break
        u = q.popleft()
        du = dist[u]
        for v in adj.get(u, ()):
            if v in parent:
                continue
            parent[v] = u
            dist[v] = du + 1
            if v in unvisited:
                candidates.append((v, dist[v]))
                if len(candidates) >= k:
                    break
            q.append(v)

    return candidates, parent, dist


def reconstruct_path(start: int, target: int, parent: Dict[int, int]) -> List[int]:
    path_rev: List[int] = [target]
    cur = target
    while cur != start:
        cur = parent[cur]
        path_rev.append(cur)
    path_rev.reverse()
    return path_rev


def bfs_distance_to_nearest_unvisited(
    start: int,
    adj: Dict[int, List[int]],
    unvisited: Set[int],
    deadline: float,
) -> Optional[int]:
    """
    Devuelve distancia mínima en saltos desde start hasta cualquier nodo en unvisited.
    """
    if not unvisited:
        return 0

    seen: Set[int] = {start}
    q: Deque[Tuple[int, int]] = deque([(start, 0)])

    while q:
        if time.monotonic() > deadline:
            return None
        u, d = q.popleft()
        for v in adj.get(u, ()):
            if v in seen:
                continue
            if v in unvisited:
                return d + 1
            seen.add(v)
            q.append((v, d + 1))
    return None


# ----------------------------
# Heurística: greedy + lookahead + epsilon + restarts
# ----------------------------
def build_route_one_run(
    start: int,
    adj: Dict[int, List[int]],
    reachable: Set[int],
    rng: random.Random,
    deadline: float,
) -> Optional[List[int]]:
    route: List[int] = [start]
    unvisited: Set[int] = set(reachable)
    unvisited.discard(start)
    current = start

    while unvisited:
        if time.monotonic() > deadline:
            return None

        candidates, parent, _dist = bfs_collect_k_unvisited(
            current, adj, unvisited, K_CANDIDATES, deadline
        )
        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1])

        # score = d(current,t) + lambda * d(t, nearest_unvisited_from_t)
        scored: List[Tuple[float, int, int]] = []  # (score, t, dct)
        L = min(LOOKAHEAD_TOP_L, len(candidates))
        for i, (t, dct) in enumerate(candidates):
            score = float(dct)
            if i < L:
                unv2 = unvisited.copy()
                unv2.discard(t)
                d2 = bfs_distance_to_nearest_unvisited(t, adj, unv2, deadline)
                if d2 is None:
                    # si no da tiempo, penaliza un poco
                    score += LOOKAHEAD_LAMBDA * 9999.0
                else:
                    score += LOOKAHEAD_LAMBDA * float(d2)
            scored.append((score, t, dct))

        scored.sort(key=lambda x: x[0])

        # epsilon-greedy: a veces elige aleatoriamente entre top M
        if rng.random() < EPSILON:
            m = min(TOP_M_FOR_EPSILON, len(scored))
            _, target, _ = rng.choice(scored[:m])
        else:
            best_score = scored[0][0]
            ties = [tpl for tpl in scored if tpl[0] == best_score]
            _, target, _ = rng.choice(ties)

        if target not in parent:
            return None

        path = reconstruct_path(current, target, parent)
        for sid in path[1:]:
            route.append(sid)
            unvisited.discard(sid)
        current = route[-1]

    return route


def validate_route(route: List[int], reachable: Set[int], adj: Dict[int, List[int]], id_to_name: Dict[int, str]) -> None:
    if not route:
        raise RuntimeError("Ruta vacía.")

    # Conectividad salto-a-salto
    for i in range(len(route) - 1):
        a, b = route[i], route[i + 1]
        if b not in set(adj.get(a, ())):
            raise RuntimeError(f"Salto inválido: {a} -> {b} no existe como stargate en el grafo.")

    # Cobertura
    visited = set(route)
    missing = reachable - visited
    if missing:
        raise RuntimeError(f"La ruta NO cubre todos los sistemas alcanzables: faltan {len(missing)}.")

    # Nombres
    unnamed = [sid for sid in route if sid not in id_to_name]
    if unnamed:
        raise RuntimeError(f"Faltan nombres para {len(unnamed)} systems (ej. {unnamed[:10]}).")


def main() -> None:
    for p in (SOLARSYSTEMS_GZ, STARGATES_GZ):
        if not os.path.exists(p):
            raise RuntimeError(f"Falta input en repo: {p}")

    ensure_dir("route_test")

    id_to_name, name_to_id = build_system_maps()
    jita_id = find_jita_id(name_to_id)
    adj = build_adjacency(name_to_id)

    reachable = reachable_from(jita_id, adj)
    if len(reachable) <= 1:
        raise RuntimeError(f"Alcanzables desde Jita = {len(reachable)} (esperado >> 1). Revisa stargates.jsonl.gz.")

    t0 = time.monotonic()
    deadline = t0 + SEARCH_BUDGET_SECONDS

    # Semilla base reproducible por inputs
    base_seed_material = (sha256_file(SOLARSYSTEMS_GZ) + sha256_file(STARGATES_GZ))[:16]
    base_seed = int(base_seed_material, 16)

    best_route: Optional[List[int]] = None
    best_len = 10**18
    best_seed: Optional[int] = None
    attempted = 0
    completed = 0

    while attempted < MAX_RESTARTS and time.monotonic() < deadline:
        attempted += 1
        seed = base_seed ^ attempted
        rng = random.Random(seed)

        route = build_route_one_run(jita_id, adj, reachable, rng, deadline)
        if route is None:
            break  # sin tiempo para completar otro run

        completed += 1
        rlen = len(route)
        if rlen < best_len:
            best_len = rlen
            best_route = route
            best_seed = seed

    if best_route is None:
        raise RuntimeError("No se pudo completar ni un run dentro del presupuesto de tiempo.")

    validate_route(best_route, reachable, adj, id_to_name)

    # route.jsonl (1 línea)
    with open(OUT_ROUTE_JSONL, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"route": best_route}, separators=(",", ":")) + "\n")

    # route_names.json (pretty)
    names = [id_to_name[sid] for sid in best_route]
    with open(OUT_ROUTE_NAMES_JSON, "w", encoding="utf-8", newline="\n") as f:
        json.dump(names, f, ensure_ascii=False, indent=2)
        f.write("\n")

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "jita_id": jita_id,
        "reachable_systems_count": len(reachable),
        "unique_systems_in_route": len(set(best_route)),
        "route_length": len(best_route),
        "heuristic": "greedy_k_candidates_lookahead + random_restarts_until_deadline",
        "params": {
            "search_budget_seconds": SEARCH_BUDGET_SECONDS,
            "k_candidates": K_CANDIDATES,
            "lookahead_top_l": LOOKAHEAD_TOP_L,
            "lookahead_lambda": LOOKAHEAD_LAMBDA,
            "epsilon": EPSILON,
            "top_m_for_epsilon": TOP_M_FOR_EPSILON,
            "max_restarts": MAX_RESTARTS,
        },
        "runs": {
            "attempted": attempted,
            "completed": completed,
            "best_seed": best_seed,
        },
        "inputs": {
            "solarsystems": {"path": SOLARSYSTEMS_GZ, "sha256": sha256_file(SOLARSYSTEMS_GZ)},
            "stargates": {"path": STARGATES_GZ, "sha256": sha256_file(STARGATES_GZ)},
        },
        "timing": {
            "search_elapsed_seconds": round(time.monotonic() - t0, 3),
        },
    }
    with open(OUT_META_JSON, "w", encoding="utf-8", newline="\n") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"OK: wrote {OUT_ROUTE_JSONL}, {OUT_ROUTE_NAMES_JSON}, {OUT_META_JSON}")
    print(f"Reachable systems from Jita: {len(reachable)} | Best route length: {len(best_route)}")
    print(f"Runs attempted: {attempted} | completed: {completed} | best_seed: {best_seed}")


if __name__ == "__main__":
    main()
