#!/usr/bin/env python3
"""
Pipeline principal de ingesta de market orders.

Hace exactamente la fase de ingesta y persistencia de estado:
- lee regiones + estructuras del repositorio
- construye planner a partir de pages cache histórico
- selecciona tuning real del run
- descarga páginas de ESI con retries compartidos
- consolida y deduplica en SQLite temporal con single writer
- reconstruye pages cache
- escribe run_metrics y finalize_summary
- escribe un run log de diagnóstico en .tmp

Y se detiene aquí.
No calcula hubs, no calcula precios, no genera salidas analíticas finales.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def load_sibling(filename: str, module_name: str):
    """
    Carga dinámica de módulos vecinos por ruta.

    Esto evita el problema de importar nombres de archivo con guiones.
    """
    here = Path(__file__).resolve().parent
    path = here / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


types_mod = load_sibling("eou_marketOrders_esi-gh-bq_types.py", "mo_types")
sde_mod = load_sibling("eou_marketOrders_esi-gh-bq_sde.py", "mo_sde")
sqlite_mod = load_sibling("eou_marketOrders_esi-gh-bq_sqlite.py", "mo_sqlite")
fetch_mod = load_sibling("eou_marketOrders_esi-gh-bq_fetch.py", "mo_fetch")
tuning_mod = load_sibling("eou_marketOrders_esi-gh-bq_tuning.py", "mo_tuning")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_pages_cache(path: str) -> Dict[str, Dict[int, int]]:
    """
    Convierte el pages cache persistente a índices rápidos:
      {
        "region": {region_id: pages, ...},
        "structure": {structure_id: pages, ...},
      }
    """
    p = Path(path)
    if not p.exists():
        return {"region": {}, "structure": {}}

    with p.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    regions = {}
    for row in raw.get("stations", []):
        try:
            regions[int(row["regionID"])] = max(0, int(row["pages"]))
        except Exception:
            continue

    structures = {}
    for row in raw.get("structures", []):
        try:
            structures[int(row["stationID"])] = max(0, int(row["pages"]))
        except Exception:
            continue

    return {"region": regions, "structure": structures}


def plan_entities(entities: List[object], workers: int, historical_pages: Dict[int, int], force_refresh: bool) -> List[List[object]]:
    """
    Planner greedy balanceado por pages_est.

    - si hay pages cache histórico y no force_refresh: usamos ese peso
    - si no: pages_est = 1
    """
    planned = []
    for entity in entities:
        if force_refresh:
            pages_est = 1
        else:
            pages_est = max(1, int(historical_pages.get(entity.entity_id, 1)))
        planned.append((pages_est, entity))

    planned.sort(key=lambda item: item[0], reverse=True)

    buckets: List[List[object]] = [[] for _ in range(workers)]
    loads = [0] * workers

    for pages_est, entity in planned:
        idx = min(range(workers), key=lambda i: loads[i])
        buckets[idx].append(entity)
        loads[idx] += pages_est

    return buckets


def rebuild_pages_cache(path: str, results: List[dict]) -> None:
    """
    Reconstruye completamente el pages cache persistente.
    """
    stations = []
    structures = []

    for row in results:
        if row["kind"] == "region":
            stations.append({
                "regionID": int(row["entity_id"]),
                "region": row["name"],
                "pages": int(row["pages"]),
            })
        elif row["kind"] == "structure" and not bool(row["ignored"]):
            structures.append({
                "stationID": int(row["entity_id"]),
                "station": row["name"],
                "pages": int(row["pages"]),
            })

    stations.sort(key=lambda x: x["regionID"])
    structures.sort(key=lambda x: x["stationID"])

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump({"stations": stations, "structures": structures}, f, indent=2, ensure_ascii=False)
        f.write("\n")


def write_json(path: str, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2 if p.name.endswith(".json") else None, ensure_ascii=False)
        f.write("\n")


class RunLogger:
    """
    Logger mínimo persistente del run.

    Vive en .tmp y sirve para depurar el pipeline sin imprimir secretos.
    """

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def log(self, message: str) -> None:
        ts = utc_now_iso()
        with self.path.open("a", encoding="utf-8") as f:
            f.write(f"{ts} {message}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--regions-path", required=True)
    parser.add_argument("--stations-path", required=True)
    parser.add_argument("--structures-path", required=True)
    parser.add_argument("--sde-dir", required=True)
    parser.add_argument("--pages-cache-path", required=True)
    parser.add_argument("--tuning-path", required=True)
    parser.add_argument("--run-metrics-path", required=True)
    parser.add_argument("--finalize-summary-path", required=True)
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--run-log-path", required=True)
    parser.add_argument("--esi-base", required=True)
    parser.add_argument("--esi-datasource", required=True)
    parser.add_argument("--esi-user-agent", required=True)
    parser.add_argument("--force-refresh", required=True)
    args = parser.parse_args()

    logger = RunLogger(args.run_log_path)
    logger.log("PIPELINE START")
    logger.log(f"regions file: {args.regions_path}")
    logger.log(f"stations file: {args.stations_path}")
    logger.log(f"structures file: {args.structures_path}")
    logger.log(f"sde dir: {args.sde_dir}")
    logger.log(f"pages cache path: {args.pages_cache_path}")
    logger.log(f"tuning path: {args.tuning_path}")
    logger.log(f"sqlite path: {args.sqlite_path}")

    started = time.monotonic()
    force_refresh = str(args.force_refresh).strip().lower() == "true"
    logger.log(f"force refresh: {force_refresh}")

    # Carga de inputs persistentes del repo.
    regions_raw = sde_mod.load_regions(args.regions_path)
    stations_raw = sde_mod.load_stations(args.stations_path)
    structures_raw = sde_mod.load_structures(args.structures_path)
    types_file = sde_mod.detect_types_file(args.sde_dir)

    logger.log(f"regions loaded: {len(regions_raw)}")
    logger.log(f"stations loaded: {len(stations_raw)}")
    logger.log(f"structures loaded: {len(structures_raw)}")
    logger.log(f"types file detected: {types_file}")

    regions = [types_mod.Entity(**row) for row in regions_raw]
    structures = [types_mod.Entity(**row) for row in structures_raw]

    # Selección real del run a partir del tuning state persistente.
    tuning_state = tuning_mod.load_state(args.tuning_path)
    if not tuning_state:
        logger.log("ERROR: tuning state not initialized")
        raise RuntimeError("Tuning state not initialized. Run tuning init before pipeline.")

    selected = tuning_mod.select_run_config(tuning_state)
    max_workers = int(selected["max_workers"])
    retry_budget_value = int(selected["retry_budget"])

    logger.log(f"selected max_workers: {max_workers}")
    logger.log(f"selected retry_budget: {retry_budget_value}")

    # Pool de tokens para endpoints autenticados de estructuras.
    ordered_tokens = fetch_mod.build_ordered_token_pairs_from_env()
    token_pool = types_mod.TokenPool(ordered_tokens)
    logger.log(f"structure tokens loaded: {token_pool.size()}")

    if structures and token_pool.size() == 0:
        logger.log("ERROR: structures exist but no structure tokens available")
        raise RuntimeError("No structure market tokens available in GitHub Secrets for structure ingestion")

    # Pages cache histórico para planner.
    old_pages = load_pages_cache(args.pages_cache_path)
    logger.log(f"historical cached regions: {len(old_pages['region'])}")
    logger.log(f"historical cached structures: {len(old_pages['structure'])}")

    region_buckets = plan_entities(regions, max_workers, old_pages["region"], force_refresh)
    structure_buckets = plan_entities(structures, max_workers, old_pages["structure"], force_refresh)

    # Un bucket final por worker que mezcle regiones + estructuras.
    buckets: List[List[object]] = []
    for i in range(max_workers):
        combined = []
        combined.extend(region_buckets[i] if i < len(region_buckets) else [])
        combined.extend(structure_buckets[i] if i < len(structure_buckets) else [])
        buckets.append(combined)

    total_entities = sum(len(b) for b in buckets)
    logger.log(f"entities scheduled: {total_entities}")

    for idx, bucket in enumerate(buckets):
        region_count = sum(1 for e in bucket if e.kind == "region")
        structure_count = sum(1 for e in bucket if e.kind == "structure")
        logger.log(
            f"worker[{idx}] entities={len(bucket)} regions={region_count} structures={structure_count}"
        )

    stats = types_mod.StatsCollector()
    retry_budget = types_mod.RetryBudget(retry_budget_value)
    writer = sqlite_mod.SQLiteWriter(args.sqlite_path)
    writer.start()
    logger.log("sqlite writer started")

    results: List[dict] = []

    def worker_run(worker_idx: int, worker_entities: List[object]) -> List[dict]:
        session = fetch_mod.build_session(args.esi_user_agent, pool_size=16)
        local_results: List[dict] = []
        logger.log(f"worker[{worker_idx}] start entities={len(worker_entities)}")
        try:
            for entity in worker_entities:
                logger.log(f"worker[{worker_idx}] fetching kind={entity.kind} id={entity.entity_id} name={entity.name}")
                result = fetch_mod.fetch_entity(
                    entity=entity,
                    session=session,
                    esi_base=args.esi_base,
                    datasource=args.esi_datasource,
                    polite_delay_s=0.30,
                    stats=stats,
                    retry_budget=retry_budget,
                    token_pool=token_pool if entity.kind == "structure" else None,
                    batch_callback=writer.enqueue,
                )
                local_results.append(result)
                logger.log(
                    f"worker[{worker_idx}] done kind={result['kind']} id={result['entity_id']} pages={result['pages']} ignored={result['ignored']}"
                )
            logger.log(f"worker[{worker_idx}] finished results={len(local_results)}")
            return local_results
        finally:
            session.close()
            logger.log(f"worker[{worker_idx}] session closed")

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(worker_run, idx, bucket)
                for idx, bucket in enumerate(buckets)
            ]
            for fut in as_completed(futures):
                worker_results = fut.result()
                results.extend(worker_results)
                logger.log(f"future completed results={len(worker_results)} accumulated_results={len(results)}")

        writer.close()
        logger.log("sqlite writer closed")

        # Solo si el run ha sido correcto regeneramos el pages cache persistente.
        rebuild_pages_cache(args.pages_cache_path, results)
        logger.log("pages cache rebuilt")

        stats_snapshot = stats.snapshot()
        ingest_seconds = round(time.monotonic() - started, 3)

        logger.log(f"requests sent: {int(stats_snapshot['requests'])}")
        logger.log(f"http200: {int(stats_snapshot['http200'])}")
        logger.log(f"http401: {int(stats_snapshot['http401'])}")
        logger.log(f"http404: {int(stats_snapshot['http404'])}")
        logger.log(f"http420: {int(stats_snapshot['http420'])}")
        logger.log(f"http429: {int(stats_snapshot['http429'])}")
        logger.log(f"http5xx: {int(stats_snapshot['http5xx'])}")
        logger.log(f"ignored_structures: {int(stats_snapshot['ignored_structures'])}")
        logger.log(f"structure404_page1_retry: {int(stats_snapshot['structure404_page1_retry'])}")
        logger.log(f"backoff_seconds: {float(stats_snapshot['backoff_seconds'])}")
        logger.log(f"max_last_modified: {stats_snapshot['max_last_modified']}")
        logger.log(f"retries_used: {retry_budget.used()}")
        logger.log(f"ingest_seconds: {ingest_seconds}")

        result_payload = {
            "ok": True,
            "ingestSeconds": ingest_seconds,
            "retriesUsed": retry_budget.used(),
            "http401": int(stats_snapshot["http401"]),
            "http429": int(stats_snapshot["http429"]),
            "backoffSeconds": float(stats_snapshot["backoff_seconds"]),
            "requests": int(stats_snapshot["requests"]),
        }
        result_payload["score"] = tuning_mod.compute_score(result_payload)

        next_current = tuning_mod.choose_next_current(tuning_state, selected, result_payload)

        run_metrics = {
            "ts": utc_now_iso(),
            "selected": {
                "max_workers": max_workers,
                "retry_budget": retry_budget_value,
            },
            "result": result_payload,
            "next_current": next_current,
            "maxLastModified": stats_snapshot["max_last_modified"],
        }

        write_json(args.run_metrics_path, run_metrics)
        logger.log(f"run metrics written: {args.run_metrics_path}")

        finalize_summary = {
            "ok": True,
            "selected": run_metrics["selected"],
            "requests": result_payload["requests"],
            "retriesUsed": result_payload["retriesUsed"],
            "maxLastModified": run_metrics["maxLastModified"],
            "pagesCachePath": args.pages_cache_path,
            "sqlitePath": args.sqlite_path,
            "runLogPath": args.run_log_path,
        }
        write_json(args.finalize_summary_path, finalize_summary)
        logger.log(f"finalize summary written: {args.finalize_summary_path}")
        logger.log("PIPELINE END OK")
        return 0

    except BaseException as exc:
        logger.log(f"PIPELINE ERROR: {exc}")
        try:
            stats_snapshot = stats.snapshot()
            ingest_seconds = round(time.monotonic() - started, 3)

            logger.log(f"requests sent before failure: {int(stats_snapshot['requests'])}")
            logger.log(f"http401 before failure: {int(stats_snapshot['http401'])}")
            logger.log(f"http429 before failure: {int(stats_snapshot['http429'])}")
            logger.log(f"backoff_seconds before failure: {float(stats_snapshot['backoff_seconds'])}")
            logger.log(f"retries_used before failure: {retry_budget.used()}")
            logger.log(f"ingest_seconds before failure: {ingest_seconds}")

            result_payload = {
                "ok": False,
                "ingestSeconds": ingest_seconds,
                "retriesUsed": retry_budget.used(),
                "http401": int(stats_snapshot["http401"]),
                "http429": int(stats_snapshot["http429"]),
                "backoffSeconds": float(stats_snapshot["backoff_seconds"]),
                "requests": int(stats_snapshot["requests"]),
            }
            result_payload["score"] = tuning_mod.compute_score(result_payload)

            next_current = tuning_mod.choose_next_current(tuning_state, selected, result_payload)

            run_metrics = {
                "ts": utc_now_iso(),
                "selected": {
                    "max_workers": max_workers,
                    "retry_budget": retry_budget_value,
                },
                "result": result_payload,
                "next_current": next_current,
                "maxLastModified": stats_snapshot["max_last_modified"],
                "error": str(exc),
            }
            write_json(args.run_metrics_path, run_metrics)
            logger.log(f"run metrics written on failure: {args.run_metrics_path}")

            finalize_summary = {
                "ok": False,
                "selected": run_metrics["selected"],
                "requests": result_payload["requests"],
                "retriesUsed": result_payload["retriesUsed"],
                "error": str(exc),
                "runLogPath": args.run_log_path,
            }
            write_json(args.finalize_summary_path, finalize_summary)
            logger.log(f"finalize summary written on failure: {args.finalize_summary_path}")
        finally:
            try:
                writer.close()
                logger.log("sqlite writer closed after failure")
            except Exception as close_exc:
                logger.log(f"sqlite writer close after failure raised: {close_exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
