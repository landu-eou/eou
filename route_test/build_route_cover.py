#!/usr/bin/env python3
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# Entradas (en el repo)
SOLARSYSTEMS_GZ = "data/sde/solarsystems.jsonl.gz"
STARGATES_GZ = "data/sde/stargates.jsonl.gz"

# Salidas (TODO dentro de route_test/)
OUT_ROUTE_JSONL = "route_test/route.jsonl"
OUT_ROUTE_NAMES_JSON = "route_test/route_names.json"
OUT_META_JSON = "route_test/route.meta.json"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    """
    Lee JSONL.gz en streaming. gzip.open soporta modo texto ('rt') con encoding.
    """
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"JSON inválido en {path}:{lineno}: {e}") from e
            if not isinstance(obj, dict):
                continue
            yield obj


def pick_first(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def normalize_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        x = x.strip()
        if x.isdigit():
            return int(x)
    return None


def extract_system_id(obj: Dict[str, Any]) -> Optional[int]:
    # Tu conversión puede usar "_key" o "solarSystemID" u otros.
    cand = pick_first(obj, ["solarSystemID", "_key", "systemID", "system_id", "id"])
    return normalize_int(cand)


def extract_system_name(obj: Dict[str, Any]) -> Optional[str]:
    # Tu conversión dice campo "solarSystem". Si no, intentamos name/en, name.
    v = pick_first(obj, ["solarSystem", "solarSystemName", "name"])
    if isinstance(v, str) and v.strip():
        return v.strip()

    # Si "name" es dict (p.ej. {"en":"Jita"}):
    if isinstance(v, dict):
        for kk in ("en", "en-us", "en_us"):
            vv = v.get(kk)
            if isinstance(vv, str) and vv.strip():
                return vv.strip()

    # Otros posibles campos
    for k in ("name_en", "en", "systemName"):
        vv = obj.get(k)
        if isinstance(vv, str) and vv.strip():
            return vv.strip()

    return None


def extract_gate_edge(obj: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    """
    mapStargates en SDE: solarSystemID + destination.solarSystemID
    En tu conversión, los nombres pueden variar un poco.
    """
    a = normalize_int(pick_first(obj, ["solarSystemID", "originSolarSystemID", "fromSolarSystemID"]))
    dest = obj.get("destination")
    b = None
    if isinstance(dest, dict):
        b = normalize_int(pick_first(dest, ["solarSystemID", "toSolarSystemID", "destinationSolarSystemID"]))

    if b is None:
        b = normalize_int(pick_first(obj, ["destinationSolarSystemID", "toSolarSystemID"]))

    if a is None or b is None:
        return None
    if a == b:
        return None
    return (a, b)


def build_graph() -> Tuple[Dict[int, str], Dict[int, Set[int]]]:
    # 1) sistemas
    id_to_name: Dict[int, str] = {}
    for obj in iter_jsonl_gz(SOLARSYSTEMS_GZ):
        sid = extract_system_id(obj)
        if sid is None:
            continue
        nm = extract_system_name(obj)
        if nm:
            id_to_name[sid] = nm

    # 2) stargates -> adyacencias (usamos conexión como no-dirigida para permitir backtracking)
    adj: Dict[int, Set[int]] = {}
    for obj in iter_jsonl_gz(STARGATES_GZ):
        edge = extract_gate_edge(obj)
        if edge is None:
            continue
        a, b = edge
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    return id_to_name, adj


def find_jita_id(id_to_name: Dict[int, str]) -> int:
    # Busca nombre exacto "Jita"
    for sid, nm in id_to_name.items():
        if nm == "Jita":
            return sid
    # fallback: case-insensitive
    for sid, nm in id_to_name.items():
        if nm.lower() == "jita":
            return sid
    raise RuntimeError("No se encontró el sistema 'Jita' en solarsystems.jsonl.gz")


def reachable_from(root: int, adj: Dict[int, Set[int]]) -> Set[int]:
    seen: Set[int] = set()
    stack = [root]
    while stack:
        u = stack.pop()
        if u in seen:
            continue
        seen.add(u)
        for v in adj.get(u, ()):
            if v not in seen:
                stack.append(v)
    return seen


def build_spanning_tree(root: int, adj: Dict[int, Set[int]], reachable: Set[int]) -> Dict[int, List[int]]:
    """
    Árbol de expansión determinista (ordenamos vecinos).
    children[u] = [hijos...]
    """
    children: Dict[int, List[int]] = {root: []}
    parent: Dict[int, int] = {root: root}

    stack = [root]
    while stack:
        u = stack.pop()
        neigh = sorted(v for v in adj.get(u, set()) if v in reachable)
        for v in neigh:
            if v in parent:
                continue
            parent[v] = u
            children.setdefault(u, []).append(v)
            children.setdefault(v, [])
            stack.append(v)

    # Seguridad: el árbol debe cubrir reachable
    missing = reachable - set(parent.keys())
    if missing:
        raise RuntimeError(f"Árbol incompleto: faltan {len(missing)} nodos alcanzables.")
    return children


def dfs_walk_from_tree(root: int, children: Dict[int, List[int]]) -> List[int]:
    """
    Genera un walk DFS con backtracking (Euler tour del árbol).
    Cada transición es entre vecinos (arista del árbol => arista del grafo).
    """
    route: List[int] = [root]
    stack: List[Tuple[int, int]] = [(root, 0)]  # (node, next_child_idx)

    while stack:
        u, idx = stack[-1]
        ch = children.get(u, [])
        if idx < len(ch):
            v = ch[idx]
            stack[-1] = (u, idx + 1)
            route.append(v)
            stack.append((v, 0))
        else:
            stack.pop()
            if stack:
                # backtrack al padre (top de stack)
                parent = stack[-1][0]
                route.append(parent)

    return route


def validate_route(route: List[int], reachable: Set[int], adj: Dict[int, Set[int]], id_to_name: Dict[int, str]) -> None:
    if not route:
        raise RuntimeError("Ruta vacía.")
    if route[0] not in reachable:
        raise RuntimeError("El origen no está en el conjunto alcanzable.")

    # 1) Conectividad salto-a-salto
    for i in range(len(route) - 1):
        a, b = route[i], route[i + 1]
        if b not in adj.get(a, set()):
            raise RuntimeError(f"Salto inválido en ruta: {a} -> {b} no existe como conexión stargate.")

    # 2) Cobertura
    visited = set(route)
    missing = reachable - visited
    if missing:
        raise RuntimeError(f"La ruta NO cubre todos los sistemas alcanzables: faltan {len(missing)}.")

    # 3) Todos tienen nombre (para route_names.json)
    unnamed = [sid for sid in route if sid not in id_to_name]
    if unnamed:
        # No fallamos por completo si falta un nombre (depende de tu conversión),
        # pero es muy útil detectarlo: lo hacemos error para que el pipeline sea consistente.
        raise RuntimeError(f"Faltan nombres para {len(unnamed)} systems en solarsystems: ej. {unnamed[:10]}")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    # Preconditions
    for p in (SOLARSYSTEMS_GZ, STARGATES_GZ):
        if not os.path.exists(p):
            raise RuntimeError(f"Falta input en repo: {p}")

    ensure_dir("route_test")

    id_to_name, adj = build_graph()
    jita_id = find_jita_id(id_to_name)

    reachable = reachable_from(jita_id, adj)
    children = build_spanning_tree(jita_id, adj, reachable)
    route = dfs_walk_from_tree(jita_id, children)

    validate_route(route, reachable, adj, id_to_name)

    # Outputs
    with open(OUT_ROUTE_JSONL, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"route": route}, separators=(",", ":")) + "\n")

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
