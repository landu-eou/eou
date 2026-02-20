#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EOU Autopilot Route Generator (SDE-only)

Genera eou_ap/eou_ap_list.txt con una secuencia de waypoints (solarSystemID y stationID)
siguiendo estas reglas:

- "Visitar" = tener waypoint.
- Ruta interna por constelación: camino que visita todos sus sistemas con mínimo nº total de stargates
  (usando DP exacta estilo Held–Karp sobre distancias BFS).
- Encadenado en región: desde el punto final de constelación, ir al sistema más cercano (en saltos) de una constelación
  no visitada dentro de la región; repetir hasta completar región.
- Al acabar región (waypoint fin de región): waypoint a la estación NPC más cercana (BFS).
  *IMPORTANTE*: NO se vuelve físicamente al fin de región. El fin de región se usa como ANCLA LÓGICA
  para calcular la siguiente región (dentro del bloque), pero no se imprime un "retorno" obligatorio.
- Regiones se visitan por bloques: no se pasa al siguiente bloque hasta completar el actual.
  Dentro de un bloque, la siguiente región es la más cercana (en saltos) al waypoint fin de región (ANCLA),
  no a la estación.

Datos: usa SOLO estos ficheros del repo:
- data/sde/regions.jsonl.gz
- data/sde/constellations.jsonl.gz
- data/sde/solarsystems.jsonl.gz
- data/sde/stations.jsonl.gz
- data/sde/stargates.jsonl.gz
"""

from __future__ import annotations

import gzip
import json
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple


# ---------------------------
# Config (bloques / regiones)
# ---------------------------

BLOCKS: List[List[str]] = [
    # 1º Bloque Imperio
    [
        "The Forge",
        "The Citadel",
        "Domain",
        "Kador",
        "Tash-Murkon",
        "Genesis",
        "Kor-Azor",
        "The Bleak Lands",
        "Devoid",
        "Lonetrek",
        "Citadel",
        "Black Rise",
        "Essence",
        "Verge Vendor",
        "Placid",
        "Sinq Laison",
        "Everyshore",
        "Solitude",
        "Heimatar",
        "Metropolis",
        "Molden Heath",
        "Derelik",
        "Aridia",
        "Khanid",
    ],
    # 2º Bloque Soberanía NPC
    [
        "Stain",
        "Venal",
        "Syndicate",
        "Curse",
        "Great Wildlands",
        "Outer Ring",
    ],
    # 3º Bloque Soberanía Compartida (NPC y Jugador)
    [
        "Delve",
        "Fountain",
        "Pure Blind",
        "Geminate",
    ],
    # 4º Bloque Soberanía Completa de Jugador
    [
        "Deklein",
        "Fade",
        "Branch",
        "Tenal",
        "Tribute",
        "Vale of the Silent",
        "Etherium Reach",
        "The Kalevala Expanse",
        "Malpais",
        "Perrigen Falls",
        "Oasa",
        "Outer Passage",
        "Esoteria",
        "Paragon Soul",
        "Period Basis",
        "Querious",
        "Catch",
        "Immensea",
        "Cache",
        "The Spire",
        "Impass",
        "Providence",
        "Cobalt Edge",
        "Omist",
        "Tenerifis",
        "Feythabolis",
        "Cloud Ring",
        "Insmother",
        "Detorid",
        "Scalding Pass",
        "Wicked Creek",
    ],
    # 5º Bloque The Deathless
    [
        "Yasna Zakh",
    ],
]

START_SYSTEM_NAME = "Jita"  # origen


# ---------------------------
# Data structures
# ---------------------------

@dataclass(frozen=True)
class SystemMeta:
    system_id: int
    name: str
    constellation: str
    region: str


# ---------------------------
# Helpers: IO
# ---------------------------

def load_jsonl_gz(path: str) -> Iterable[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def require_file(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing required file: {path}")


# ---------------------------
# Graph utilities (BFS)
# ---------------------------

def bfs_nearest_by_level(
    start: int,
    adj: Dict[int, List[int]],
    is_target,
) -> Optional[Tuple[int, int]]:
    """
    BFS por niveles para obtener el target más cercano en saltos.
    Si hay varios a la misma distancia, desempata por menor ID.
    Devuelve (node_id, distance) o None si no hay.
    """
    if is_target(start):
        return (start, 0)

    seen: Set[int] = {start}
    frontier: List[int] = [start]
    dist = 0

    while frontier:
        dist += 1
        next_frontier: List[int] = []
        hits: List[int] = []

        for node in frontier:
            for nb in adj.get(node, []):
                if nb in seen:
                    continue
                seen.add(nb)
                if is_target(nb):
                    hits.append(nb)
                else:
                    next_frontier.append(nb)

        if hits:
            return (min(hits), dist)

        frontier = next_frontier

    return None


def bfs_distances_to_targets(
    start: int,
    adj: Dict[int, List[int]],
    targets: Set[int],
) -> Dict[int, int]:
    """
    BFS desde start pero parando cuando se han encontrado distancias a todos los targets.
    Devuelve dict {target: dist}.
    """
    if not targets:
        return {}

    remaining = set(targets)
    remaining.discard(start)

    found: Dict[int, int] = {}
    q = deque([start])
    dist: Dict[int, int] = {start: 0}

    while q and remaining:
        node = q.popleft()
        nd = dist[node] + 1
        for nb in adj.get(node, []):
            if nb in dist:
                continue
            dist[nb] = nd
            if nb in remaining:
                found[nb] = nd
                remaining.remove(nb)
                if not remaining:
                    break
            q.append(nb)

    return found


# ---------------------------
# Core planner
# ---------------------------

class Planner:
    def __init__(
        self,
        systems: Dict[int, SystemMeta],
        system_id_by_name: Dict[str, int],
        stations_in_system: Dict[int, List[int]],
        adj: Dict[int, List[int]],
        reachable: Set[int],
        regions_in_blocks: List[List[str]],
    ):
        self.systems = systems
        self.system_id_by_name = system_id_by_name
        self.stations_in_system = stations_in_system
        self.adj = adj
        self.reachable = reachable
        self.regions_in_blocks = regions_in_blocks

        # region -> constellation -> systems
        self.systems_by_region_const: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
        for sid, meta in systems.items():
            if sid not in reachable:
                continue
            self.systems_by_region_const[meta.region][meta.constellation].append(sid)

        # sort lists deterministically
        for region, cd in self.systems_by_region_const.items():
            for const, lst in cd.items():
                lst.sort()

    def _append_waypoint(self, out: List[int], wp: int) -> None:
        # Evita duplicados consecutivos
        if not out or out[-1] != wp:
            out.append(wp)

    def _nearest_system_in_unvisited_constellations(
        self,
        start: int,
        region: str,
        visited_constellations: Set[str],
    ) -> Optional[int]:
        def is_target(sid: int) -> bool:
            meta = self.systems.get(sid)
            return (
                meta is not None
                and meta.region == region
                and meta.constellation not in visited_constellations
                and sid in self.reachable
            )

        res = bfs_nearest_by_level(start, self.adj, is_target)
        return res[0] if res else None

    def _nearest_system_in_regions(
        self,
        start: int,
        region_names: Set[str],
    ) -> Optional[int]:
        def is_target(sid: int) -> bool:
            meta = self.systems.get(sid)
            return (
                meta is not None
                and meta.region in region_names
                and sid in self.reachable
            )

        res = bfs_nearest_by_level(start, self.adj, is_target)
        return res[0] if res else None

    def _nearest_station_id(self, start: int) -> Optional[Tuple[int, int, int]]:
        """
        Devuelve (stationID, stationSystemID, dist) de la estación más cercana por BFS.
        Si hay varias a misma distancia, elige menor stationID (vía ordenación por sistema+min station).
        """
        def is_target_system(sid: int) -> bool:
            return sid in self.stations_in_system and len(self.stations_in_system[sid]) > 0

        res = bfs_nearest_by_level(start, self.adj, is_target_system)
        if not res:
            return None
        sys_id, dist = res
        station_id = min(self.stations_in_system[sys_id])
        return (station_id, sys_id, dist)

    @staticmethod
    def _held_karp_path_nodes_ordered_start_first(
        nodes: List[int],
        dist_m: List[List[int]],
        next_target_distance_fn,
    ) -> List[int]:
        """
        Held–Karp para camino Hamiltoniano mínimo:
        - nodes[0] es el inicio fijo
        - final libre
        - desempate: menor distancia al "siguiente objetivo" y luego menor end_id
        """
        n = len(nodes)
        if n == 1:
            return [nodes[0]]

        size = 1 << (n - 1)
        INF = 10**18
        dp = [[INF] * n for _ in range(size)]
        parent: List[List[Optional[int]]] = [[None] * n for _ in range(size)]

        # init: start(0) -> j
        for j in range(1, n):
            m = 1 << (j - 1)
            dp[m][j] = dist_m[0][j]
            parent[m][j] = 0

        # transitions
        for mask in range(size):
            for j in range(1, n):
                bitj = 1 << (j - 1)
                if not (mask & bitj):
                    continue
                prev_mask = mask ^ bitj
                if prev_mask == 0:
                    continue
                best = dp[mask][j]
                best_k = parent[mask][j]
                for k in range(1, n):
                    bitk = 1 << (k - 1)
                    if not (prev_mask & bitk):
                        continue
                    cand = dp[prev_mask][k] + dist_m[k][j]
                    if cand < best:
                        best = cand
                        best_k = k
                dp[mask][j] = best
                parent[mask][j] = best_k

        full = size - 1

        # choose best end
        best_end_idx = None
        best_cost = INF
        best_next = INF
        best_end_id = INF

        for j in range(1, n):
            cost = dp[full][j]
            if cost >= INF:
                continue
            end_id = nodes[j]
            next_d = next_target_distance_fn(end_id)
            if (
                cost < best_cost
                or (cost == best_cost and next_d < best_next)
                or (cost == best_cost and next_d == best_next and end_id < best_end_id)
            ):
                best_cost = cost
                best_next = next_d
                best_end_id = end_id
                best_end_idx = j

        if best_end_idx is None:
            raise RuntimeError("No path found in constellation DP")

        # reconstruct
        order_idx: List[int] = [best_end_idx]
        mask = full
        j = best_end_idx
        while True:
            pj = parent[mask][j]
            if pj is None:
                raise RuntimeError("DP reconstruct failed")
            if pj == 0:
                order_idx.append(0)
                break
            mask = mask ^ (1 << (j - 1))
            j = pj
            order_idx.append(j)

        order_idx.reverse()
        return [nodes[i] for i in order_idx]

    def _constellation_path(
        self,
        const_systems: List[int],
        start_system: int,
        next_target_distance_fn,
    ) -> List[int]:
        """
        Camino mínimo dentro de una constelación empezando en start_system.
        """
        nodes_all = list(const_systems)
        if start_system not in nodes_all:
            # entrada "rara": usa menor ID por determinismo
            start_system = min(nodes_all)

        # start primero, resto ascendente
        rest = [x for x in nodes_all if x != start_system]
        rest.sort()
        nodes = [start_system] + rest
        n = len(nodes)

        idx = {sid: i for i, sid in enumerate(nodes)}
        dist_m = [[0] * n for _ in range(n)]
        nodes_set = set(nodes)

        for sid in nodes:
            targets = nodes_set - {sid}
            dmap = bfs_distances_to_targets(sid, self.adj, targets)
            for t, d in dmap.items():
                dist_m[idx[sid]][idx[t]] = d

        # conectividad mínima (si 0 fuera de diagonal, no hay conexión)
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if dist_m[i][j] == 0:
                    raise RuntimeError(
                        f"Constellation disconnected in stargate graph: "
                        f"{self.systems[nodes[i]].name} -> {self.systems[nodes[j]].name}"
                    )

        return self._held_karp_path_nodes_ordered_start_first(nodes, dist_m, next_target_distance_fn)

    def plan_region(
        self,
        region: str,
        entry_system: int,
        pending_regions_in_block: Set[str],
    ) -> Tuple[List[int], int]:
        """
        Planifica una región completa:
        - Constelación a constelación con ruta interna mínima por constelación.
        Devuelve:
          (waypoints, region_end_system) donde region_end_system es el ANCLA lógica de la región.
        """
        if entry_system not in self.reachable:
            raise RuntimeError(f"Entry system {entry_system} not reachable")

        waypoints: List[int] = []

        const_map = self.systems_by_region_const.get(region, {})
        if not const_map:
            raise RuntimeError(f"No reachable systems found for region: {region}")

        visited_constellations: Set[str] = set()
        current_system = entry_system

        while True:
            meta = self.systems[current_system]

            # si estamos fuera de región por entrada borde, ajusta a algún sistema de la región
            if meta.region != region:
                nearest_in_region = self._nearest_system_in_regions(current_system, {region})
                if nearest_in_region is None:
                    raise RuntimeError(f"Cannot find any system in region {region} from {current_system}")
                current_system = nearest_in_region
                meta = self.systems[current_system]

            current_const = meta.constellation

            # Si la const ya está visitada, salta a la siguiente no visitada
            if current_const in visited_constellations:
                nxt = self._nearest_system_in_unvisited_constellations(current_system, region, visited_constellations)
                if nxt is None:
                    break
                current_system = nxt
                continue

            # Marca constelación visitada
            visited_constellations.add(current_const)

            systems_in_const = const_map.get(current_const, [])
            if not systems_in_const:
                # constelación sin sistemas reachable (extraño), sigue
                nxt = self._nearest_system_in_unvisited_constellations(current_system, region, visited_constellations)
                if nxt is None:
                    break
                current_system = nxt
                continue

            remaining_consts_exist = any(c not in visited_constellations for c in const_map.keys())

            def next_target_distance_fn(end_sid: int) -> int:
                # 1) si quedan constelaciones en la región: distancia al sistema más cercano de una const no visitada
                if remaining_consts_exist:
                    def is_target(sid: int) -> bool:
                        m = self.systems.get(sid)
                        return (
                            m is not None
                            and m.region == region
                            and m.constellation not in visited_constellations
                            and sid in self.reachable
                        )
                    res = bfs_nearest_by_level(end_sid, self.adj, is_target)
                    return res[1] if res else 10**9

                # 2) si región termina: distancia a alguna región pendiente del bloque (para escoger buen endpoint)
                if pending_regions_in_block:
                    def is_target2(sid: int) -> bool:
                        m = self.systems.get(sid)
                        return (
                            m is not None
                            and m.region in pending_regions_in_block
                            and sid in self.reachable
                        )
                    res2 = bfs_nearest_by_level(end_sid, self.adj, is_target2)
                    return res2[1] if res2 else 10**9

                return 10**9

            # Ruta mínima dentro de constelación
            path = self._constellation_path(systems_in_const, current_system, next_target_distance_fn)
            for sid in path:
                self._append_waypoint(waypoints, sid)

            current_system = path[-1]

            # Siguiente constelación no visitada dentro de la región
            nxt = self._nearest_system_in_unvisited_constellations(current_system, region, visited_constellations)
            if nxt is None:
                break
            current_system = nxt

        region_end_system = current_system
        return waypoints, region_end_system

    def plan_all(self) -> List[int]:
        """
        Plan global por bloques.
        Importante:
          - current_anchor es el ANCLA lógica (fin de región) para elegir la siguiente región.
          - out contiene waypoints "físicos" (incluye stationID), pero NO afecta a la elección de la siguiente región.
        """
        if START_SYSTEM_NAME not in self.system_id_by_name:
            raise RuntimeError(f"Start system not found: {START_SYSTEM_NAME}")

        start_system = self.system_id_by_name[START_SYSTEM_NAME]
        if start_system not in self.reachable:
            raise RuntimeError("Start system is not reachable in stargate graph")

        out: List[int] = []
        current_anchor = start_system  # ancla lógica (al inicio, Jita)

        # Filtra regiones existentes en el SDE (por nombre)
        all_region_names_in_sde = {m.region for m in self.systems.values()}
        blocks: List[List[str]] = []
        for block in self.regions_in_blocks:
            blocks.append([r for r in block if r in all_region_names_in_sde])

        # Asegura que la región de Jita esté en el primer bloque
        jita_region = self.systems[start_system].region
        if blocks and jita_region not in blocks[0]:
            blocks[0] = [jita_region] + blocks[0]

        for bi, block_regions in enumerate(blocks, start=1):
            pending: Set[str] = set(block_regions)

            while pending:
                # Región “candidata” si el ancla ya está en una región pendiente
                anchor_region = self.systems[current_anchor].region

                if anchor_region in pending:
                    entry_system = current_anchor
                    region_to_visit = anchor_region
                else:
                    # Elige región pendiente más cercana EN SALTOS desde el ANCLA
                    entry_system = self._nearest_system_in_regions(current_anchor, pending)
                    if entry_system is None:
                        raise RuntimeError(
                            f"Cannot reach any pending region in block {bi} from anchor {current_anchor}"
                        )
                    region_to_visit = self.systems[entry_system].region

                pending.remove(region_to_visit)

                # Planifica región completa
                region_waypoints, region_end = self.plan_region(
                    region=region_to_visit,
                    entry_system=entry_system,
                    pending_regions_in_block=set(pending),
                )

                # Emite waypoints de región (sistemas)
                for wp in region_waypoints:
                    self._append_waypoint(out, wp)

                # Fijamos ANCLA lógica al fin de región
                current_anchor = region_end

                # Añade estación NPC más cercana (waypoint físico),
                # PERO NO volvemos al ancla: el ancla solo se usa para calcular la siguiente región.
                station_info = self._nearest_station_id(current_anchor)
                if station_info is not None:
                    station_id, _station_sys, _dist = station_info
                    self._append_waypoint(out, station_id)

        return out


# ---------------------------
# Load SDE + build planner
# ---------------------------

def build_everything() -> Planner:
    # Required files
    require_file("data/sde/regions.jsonl.gz")
    require_file("data/sde/constellations.jsonl.gz")
    require_file("data/sde/solarsystems.jsonl.gz")
    require_file("data/sde/stations.jsonl.gz")
    require_file("data/sde/stargates.jsonl.gz")

    # Load solar systems
    systems: Dict[int, SystemMeta] = {}
    system_id_by_name: Dict[str, int] = {}

    for rec in load_jsonl_gz("data/sde/solarsystems.jsonl.gz"):
        sid = int(rec["solarSystemID"])
        name = rec["solarSystem"]
        const = rec["constellation"]
        region = rec["region"]
        systems[sid] = SystemMeta(system_id=sid, name=name, constellation=const, region=region)
        system_id_by_name[name] = sid

    # Load stations (NPC stations) -> stationID per systemID
    stations_in_system: Dict[int, List[int]] = defaultdict(list)
    for rec in load_jsonl_gz("data/sde/stations.jsonl.gz"):
        stid = int(rec["stationID"])
        sys_name = rec["solarSystem"]
        sid = system_id_by_name.get(sys_name)
        if sid is None:
            continue
        stations_in_system[sid].append(stid)

    for sid in list(stations_in_system.keys()):
        stations_in_system[sid].sort()

    # Build graph from stargates
    edges: Dict[int, Set[int]] = defaultdict(set)

    def parse_group(group: str) -> Optional[Tuple[str, str]]:
        # Esperamos "A ↔ B"
        if "↔" in group:
            a, b = group.split("↔", 1)
            return a.strip(), b.strip()
        # fallback si cambiara el separador
        for sep in ["<->", "↔︎", " - "]:
            if sep in group:
                a, b = group.split(sep, 1)
                return a.strip(), b.strip()
        return None

    for rec in load_jsonl_gz("data/sde/stargates.jsonl.gz"):
        grp = rec.get("stargateGroup")
        if not grp:
            continue
        parsed = parse_group(str(grp))
        if not parsed:
            continue
        a_name, b_name = parsed
        a_id = system_id_by_name.get(a_name)
        b_id = system_id_by_name.get(b_name)
        if a_id is None or b_id is None:
            continue
        edges[a_id].add(b_id)
        edges[b_id].add(a_id)

    # adjacency as sorted lists (deterministic)
    adj: Dict[int, List[int]] = {}
    for sid, nbs in edges.items():
        adj[sid] = sorted(nbs)

    # Reachable component from start
    if START_SYSTEM_NAME not in system_id_by_name:
        raise RuntimeError(f"Start system {START_SYSTEM_NAME} not present in solarsystems dataset")
    start_id = system_id_by_name[START_SYSTEM_NAME]
    if start_id not in adj:
        raise RuntimeError("Start system has no stargates in stargates dataset")

    reachable: Set[int] = set()
    q = deque([start_id])
    reachable.add(start_id)
    while q:
        node = q.popleft()
        for nb in adj.get(node, []):
            if nb not in reachable:
                reachable.add(nb)
                q.append(nb)

    return Planner(
        systems=systems,
        system_id_by_name=system_id_by_name,
        stations_in_system=stations_in_system,
        adj=adj,
        reachable=reachable,
        regions_in_blocks=BLOCKS,
    )


def main() -> None:
    planner = build_everything()
    waypoints = planner.plan_all()

    os.makedirs("eou_ap", exist_ok=True)
    out_path = "eou_ap/eou_ap_list.txt"

    with open(out_path, "w", encoding="utf-8") as f:
        for wp in waypoints:
            f.write(f"{wp}\n")

    print(f"OK: wrote {len(waypoints)} waypoints to {out_path}")


if __name__ == "__main__":
    main()
