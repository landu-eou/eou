#!/usr/bin/env python3
from __future__ import annotations

import gzip
import hashlib
import json
import os
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


def build_system_maps() -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    solarsystems.jsonl.gz (tu formato):
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
    stargates.jsonl.gz (tu formato):
    {"stargateGroup":"Arnatele ↔ Bereye", "solarSystem":"Arnatele", ...}
    y/o
    {"stargate":"Arnatele → Junsoraert", ...}

    Preferimos stargateGroup (↔) por ser bidireccional y más estable.
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


def build_adjacency(name_to_id: Dict[str, int]) -> Dict[int, Set[int]]:
    """
    Grafo no-dirigido (permite backtracking y representa stargates ↔).
    """
    adj: Dict[int, Set[int]] = {}
    skipped_unknown = 0

    for obj in iter_jsonl_gz(STARGATES_GZ):
        endpoints = parse_gate_endpoints(obj)
        if endpoints is None:
            continue

        a_name, b_name = endpoints
        a = name_to_id.get(a_name)
        b = name_to_id.get(b_name)
        if a is None or b is None or a == b:
            skipped_unknown += 1
            continue

        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    if not adj:
        raise RuntimeError(
            "No se construyeron aristas de stargates (adj vacío). Revisa parseo de stargates.jsonl.gz."
        )

    return adj


def find_jita_id(name_to_id: Dict[str, int]) -> int:
    jita = name_to_id.get("Jita")
    if jita is not None:
        return jita
    for nm, sid in name_to_id.items():
        if nm.lower() == "jita":
            return sid
    raise RuntimeError("No se encontró el sistema 'Jita' en solarsystems.jsonl.gz")


def reachable_from(root: int, adj: Dict[int, Set[int]]) -> Set[int]:
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


def bfs_to_nearest_unvisited(
    start: int,
    adj: Dict[int, Set[int]],
    unvisited: Set[int],
) -> Optional[List[int]]:
    """
    BFS desde 'start' hasta encontrar el primer nodo en 'unvisited'.
    Devuelve el camino más corto (lista de IDs) desde start a ese objetivo, incluyendo start y target.
    Si start ya es unvisited, devuelve [start].
    """
    if start in unvisited:
        return [start]

    parent: Dict[int, int] = {start: start}
    q: Deque[int] = deque([start])

    target: Optional[int] = None

    while q:
        u = q.popleft()
        for v in adj.get(u, ()):
            if v in parent:
                continue
            parent[v] = u
            if v in unvisited:
                target = v
                q.clear()
                break
            q.append(v)

    if target is None:
        return None

    # reconstruir camino start -> target
    path_rev: List[int] = [target]
    cur = target
    while cur != start:
        cur = parent[cur]
        path_rev.append(cur)
    path_rev.reverse()
    return path_rev


def build_route_greedy_nearest_unvisited(
    start: int,
    adj: Dict[int, Set[int]],
    reachable: Set[int],
) -> List[int]:
    """
    Heurística greedy tipo nearest-neighbor:
    - Mantiene conjunto unvisited = reachable \ {start}
    - Repite: BFS desde current hasta el unvisited más cercano; añade el path completo (incluye intermedios)
      y marca como visitados todos los nodos del path.
    """
    route: List[int] = [start]
    visited: Set[int] = {start}
    unvisited: Set[int] = set(reachable)
    unvisited.discard(start)

    current = start
    steps = 0

    while unvisited:
        steps += 1
        path = bfs_to_nearest_unvisited(current, adj, unvisited)
        if path is None:
            # No debería ocurrir si reachable está bien calculado
            raise RuntimeError(
                f"No se pudo alcanzar ningún unvisited desde {current}. "
                "¿Grafo inconsistente o reachable incorrecto?"
            )

        # path incluye current al inicio. Añadimos desde el siguiente para no duplicar.
        for sid in path[1:]:
            route.append(sid)
            visited.add(sid)
            unvisited.discard(sid)

        current = route[-1]

        # Freno defensivo ante bucles improbables
        if steps > len(reachable) * 10:
            raise RuntimeError("Demasiadas iteraciones buscando unvisited. Posible inconsistencia en datos.")

    return route


def validate_route(route: List[int], reachable: Set[int], adj: Dict[int, Set[int]], id_to_name: Dict[int, str]) -> None:
    if not route:
        raise RuntimeError("Ruta vacía.")

    # Conectividad salto-a-salto
    for i in range(len(route) - 1):
        a, b = route[i], route[i + 1]
        if b not in adj.get(a, set()):
            raise RuntimeError(f"Salto inválido: {a} -> {b} no existe como stargate en el grafo.")

    # Cobertura total de alcanzables
    visited = set(route)
    missing = reachable - visited
    if missing:
        raise RuntimeError(f"La ruta NO cubre todos los sistemas alcanzables: faltan {len(missing)}.")

    # Nombres presentes para route_names.json
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
        raise RuntimeError(
            f"Alcanzables desde Jita = {len(reachable)} (esperado >> 1). Revisa stargates.jsonl.gz."
        )

    # Ruta aproximada “más corta”: greedy nearest-unvisited usando BFS (shortest paths en saltos)
    route = build_route_greedy_nearest_unvisited(jita_id, adj, reachable)

    validate_route(route, reachable, adj, id_to_name)

    # route.jsonl (1 línea)
    with open(OUT_ROUTE_JSONL, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"route": route}, separators=(",", ":")) + "\n")

    # route_names.json (pretty)
    names = [id_to_name[sid] for sid in route]
    with open(OUT_ROUTE_NAMES_JSON, "w", encoding="utf-8", newline="\n") as f:
        json.dump(names, f, ensure_ascii=False, indent=2)
        f.write("\n")

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "jita_id": jita_id,
        "reachable_systems_count": len(reachable),
        "unique_systems_in_route": len(set(route)),
        "route_length": len(route),
        "heuristic": "greedy_nearest_unvisited_bfs",
        "inputs": {
            "solarsystems": {"path": SOLARSYSTEMS_GZ, "sha256": sha256_file(SOLARSYSTEMS_GZ)},
            "stargates": {"path": STARGATES_GZ, "sha256": sha256_file(STARGATES_GZ)},
        },
    }
    with open(OUT_META_JSON, "w", encoding="utf-8", newline="\n") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"OK: wrote {OUT_ROUTE_JSONL}, {OUT_ROUTE_NAMES_JSON}, {OUT_META_JSON}")
    print(f"Reachable systems from Jita: {len(reachable)} | Route length: {len(route)}")


if __name__ == "__main__":
    main()
