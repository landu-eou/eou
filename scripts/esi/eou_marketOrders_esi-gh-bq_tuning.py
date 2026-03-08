#!/usr/bin/env python3
"""
Estado persistente de autotuning del workflow.

Subcomandos:
- init
- finalize-success
- finalize-failure

Además, este archivo expone funciones reutilizables por pipeline.py para:
- cargar/guardar estado
- seleccionar la combinación real del run
- calcular score
- proponer el siguiente current

El objetivo del autotuning aquí no es maximizar agresividad, sino encontrar una
combinación estable que minimice presión sobre ESI y fallos operativos.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


MIN_WORKERS = 4
MAX_WORKERS = 16
MIN_RETRY_BUDGET = 10
MAX_RETRY_BUDGET = 50
MAX_HISTORY = 5


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def clamp_workers(value: int) -> int:
    return max(MIN_WORKERS, min(MAX_WORKERS, int(value)))


def clamp_retry_budget(value: int) -> int:
    return max(MIN_RETRY_BUDGET, min(MAX_RETRY_BUDGET, int(value)))


def clamp_pair(max_workers: int, retry_budget: int) -> Dict[str, int]:
    return {
        "max_workers": clamp_workers(max_workers),
        "retry_budget": clamp_retry_budget(retry_budget),
    }


def load_state(path: str) -> Dict[str, object]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: str, state: Dict[str, object]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        f.write("\n")


def default_state(requested_max_workers: int, requested_retry_budget: int) -> Dict[str, object]:
    current = clamp_pair(requested_max_workers, requested_retry_budget)
    return {
        "version": 1,
        "status": "idle",
        "next_run": None,
        "failed": 0,
        "current": current,
        "best": {
            "max_workers": current["max_workers"],
            "retry_budget": current["retry_budget"],
            "score": None,
            "ts": None,
        },
        "history": [],
    }


def ensure_state(path: str, requested_max_workers: int, requested_retry_budget: int) -> Dict[str, object]:
    state = load_state(path)
    if not state:
        state = default_state(requested_max_workers, requested_retry_budget)

    # Normalización defensiva.
    current = state.get("current") or {}
    state["current"] = clamp_pair(
        current.get("max_workers", requested_max_workers),
        current.get("retry_budget", requested_retry_budget),
    )

    best = state.get("best") or {}
    best_pair = clamp_pair(
        best.get("max_workers", state["current"]["max_workers"]),
        best.get("retry_budget", state["current"]["retry_budget"]),
    )
    state["best"] = {
        "max_workers": best_pair["max_workers"],
        "retry_budget": best_pair["retry_budget"],
        "score": best.get("score"),
        "ts": best.get("ts"),
    }

    history = state.get("history") or []
    if not isinstance(history, list):
        history = []
    state["history"] = history[-MAX_HISTORY:]
    state["failed"] = int(state.get("failed", 0))
    return state


def compute_score(result: Dict[str, object]) -> float:
    ok = bool(result.get("ok"))
    ingest_seconds = float(result.get("ingestSeconds", 0.0))
    http429 = int(result.get("http429", 0))
    http401 = int(result.get("http401", 0))
    backoff_seconds = float(result.get("backoffSeconds", 0.0))

    score = ingest_seconds
    score += 15.0 * http429
    score += 5.0 * http401
    score += 0.5 * backoff_seconds
    if not ok:
        score += 300.0
    return float(score)


def recent_pairs(history: List[dict]) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    for item in history[-MAX_HISTORY:]:
        selected = item.get("selected") or {}
        try:
            out.append((int(selected["max_workers"]), int(selected["retry_budget"])))
        except Exception:
            continue
    return out


def _candidate_neighbors(base_w: int, base_r: int) -> List[Tuple[int, int]]:
    """
    Vecindario corto alrededor de una combinación base.

    Evitamos cambios bruscos y priorizamos exploración local.
    """
    raw = [
        (base_w, base_r),
        (base_w - 2, base_r),
        (base_w + 2, base_r),
        (base_w, base_r + 5),
        (base_w, base_r - 5),
        (base_w - 2, base_r + 5),
        (base_w + 2, base_r + 5),
        (base_w - 1, base_r),
        (base_w + 1, base_r),
    ]
    out: List[Tuple[int, int]] = []
    seen = set()
    for w, r in raw:
        pair = (clamp_workers(w), clamp_retry_budget(r))
        if pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out


def _pick_not_recent(candidates: List[Tuple[int, int]], recent: List[Tuple[int, int]]) -> Tuple[int, int]:
    for pair in candidates:
        if pair not in recent:
            return pair
    return candidates[0]


def select_run_config(state: Dict[str, object]) -> Dict[str, int]:
    """
    Selecciona la combinación real del run a partir del estado persistente.

    Reglas generales:
    - si el último run falló, volvemos hacia best y subimos retry
    - si hubo presión/rate limit, bajamos workers y subimos retry
    - si el último score empeora y no estábamos en best, volvemos a best
    - si current coincide con best, exploramos vecinos no recientes
    """
    current = state["current"]
    best = state["best"]
    history = state.get("history") or []
    recent = recent_pairs(history)
    last = history[-1] if history else None

    current_pair = (int(current["max_workers"]), int(current["retry_budget"]))
    best_pair = (int(best["max_workers"]), int(best["retry_budget"]))

    # Si el último falló, volvemos hacia best y reforzamos retry budget.
    if last and not bool(last.get("ok", True)):
        base_w, base_r = best_pair
        candidates = _candidate_neighbors(base_w, base_r + 5)
        pair = _pick_not_recent(candidates, recent)
        return {"max_workers": pair[0], "retry_budget": pair[1]}

    # Si hubo presión o limitación, bajar workers y subir retry.
    if last and (
        int(last.get("http429", 0)) > 0
        or float(last.get("backoffSeconds", 0.0)) >= 30.0
        or int(last.get("http401", 0)) > 2
    ):
        base_w = max(MIN_WORKERS, current_pair[0] - 2)
        base_r = min(MAX_RETRY_BUDGET, current_pair[1] + 5)
        candidates = _candidate_neighbors(base_w, base_r)
        pair = _pick_not_recent(candidates, recent)
        return {"max_workers": pair[0], "retry_budget": pair[1]}

    # Si el último score empeora y no estábamos ya en best, volver a best.
    if last and best.get("score") is not None:
        last_score = float(last.get("score", 0.0))
        best_score = float(best["score"])
        if last_score > best_score and current_pair != best_pair:
            return {"max_workers": best_pair[0], "retry_budget": best_pair[1]}

    # Si current == best, explorar cerca evitando las últimas 5 combinaciones.
    if current_pair == best_pair:
        candidates = _candidate_neighbors(current_pair[0], current_pair[1])
        pair = _pick_not_recent(candidates, recent)
        return {"max_workers": pair[0], "retry_budget": pair[1]}

    # Por defecto, mantenemos current.
    return {"max_workers": current_pair[0], "retry_budget": current_pair[1]}


def choose_next_current(state: Dict[str, object], selected: Dict[str, int], result: Dict[str, object]) -> Dict[str, int]:
    """
    Calcula el siguiente current después del run.

    La lógica es coherente con select_run_config, pero usa el resultado actual.
    """
    history = list(state.get("history") or [])
    history.append({
        "selected": selected,
        "ok": bool(result.get("ok")),
        "score": float(result.get("score", 0.0)),
        "http429": int(result.get("http429", 0)),
        "http401": int(result.get("http401", 0)),
        "backoffSeconds": float(result.get("backoffSeconds", 0.0)),
    })
    pseudo_state = deepcopy(state)
    pseudo_state["history"] = history[-MAX_HISTORY:]
    pseudo_state["current"] = selected
    return select_run_config(pseudo_state)


def init_state(path: str, requested_max_workers: int, requested_retry_budget: int, lock_time: int) -> Dict[str, object]:
    state = ensure_state(path, requested_max_workers, requested_retry_budget)
    state["status"] = "in progress"
    state["next_run"] = iso_utc(utc_now() + timedelta(seconds=int(lock_time)))
    save_state(path, state)
    return state


def finalize_success(path: str, run_metrics_path: str, lock_time: int) -> Dict[str, object]:
    state = load_state(path)
    if not state:
        raise RuntimeError("Tuning state not found for finalize-success")

    with open(run_metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    result = metrics["result"]
    selected = metrics["selected"]
    next_current = metrics["next_current"]
    score = float(result["score"])

    history = list(state.get("history") or [])
    history.append({
        "ts": metrics["ts"],
        "ok": bool(result["ok"]),
        "score": score,
        "selected": selected,
        "requests": int(result["requests"]),
        "http401": int(result["http401"]),
        "http429": int(result["http429"]),
        "backoffSeconds": float(result["backoffSeconds"]),
        "ingestSeconds": float(result["ingestSeconds"]),
    })
    history = history[-MAX_HISTORY:]

    state["status"] = "completed"
    state["failed"] = 0
    state["history"] = history
    state["current"] = {
        "max_workers": int(next_current["max_workers"]),
        "retry_budget": int(next_current["retry_budget"]),
    }

    best = state.get("best") or {}
    best_score = best.get("score")
    if result["ok"] and (best_score is None or score < float(best_score)):
        state["best"] = {
            "max_workers": int(selected["max_workers"]),
            "retry_budget": int(selected["retry_budget"]),
            "score": score,
            "ts": metrics["ts"],
        }

    max_last_modified = metrics.get("maxLastModified")
    last_modified_dt = parse_iso_utc(max_last_modified)
    now = utc_now()
    if last_modified_dt is not None:
        next_run_dt = max(last_modified_dt + timedelta(seconds=int(lock_time)), now)
    else:
        next_run_dt = now + timedelta(seconds=int(lock_time))
    state["next_run"] = iso_utc(next_run_dt)

    save_state(path, state)
    return state


def finalize_failure(path: str, run_metrics_path: str, lock_time: int) -> Dict[str, object]:
    state = load_state(path)
    if not state:
        raise RuntimeError("Tuning state not found for finalize-failure")

    metrics = None
    p = Path(run_metrics_path)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            metrics = json.load(f)

    state["failed"] = int(state.get("failed", 0)) + 1
    state["status"] = "failed"

    if metrics:
        result = metrics["result"]
        selected = metrics["selected"]
        next_current = metrics["next_current"]
        score = float(result["score"])

        history = list(state.get("history") or [])
        history.append({
            "ts": metrics["ts"],
            "ok": bool(result["ok"]),
            "score": score,
            "selected": selected,
            "requests": int(result["requests"]),
            "http401": int(result["http401"]),
            "http429": int(result["http429"]),
            "backoffSeconds": float(result["backoffSeconds"]),
            "ingestSeconds": float(result["ingestSeconds"]),
        })
        state["history"] = history[-MAX_HISTORY:]
        state["current"] = {
            "max_workers": int(next_current["max_workers"]),
            "retry_budget": int(next_current["retry_budget"]),
        }

    delay = min(int(lock_time) * (2 ** max(0, state["failed"] - 1)), 3600)
    state["next_run"] = iso_utc(utc_now() + timedelta(seconds=delay))

    save_state(path, state)
    return state


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init")
    p_init.add_argument("--path", required=True)
    p_init.add_argument("--requested-max-workers", required=True, type=int)
    p_init.add_argument("--requested-retry-budget", required=True, type=int)
    p_init.add_argument("--lock-time", required=True, type=int)

    p_ok = sub.add_parser("finalize-success")
    p_ok.add_argument("--path", required=True)
    p_ok.add_argument("--run-metrics-path", required=True)
    p_ok.add_argument("--lock-time", required=True, type=int)

    p_fail = sub.add_parser("finalize-failure")
    p_fail.add_argument("--path", required=True)
    p_fail.add_argument("--run-metrics-path", required=True)
    p_fail.add_argument("--lock-time", required=True, type=int)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.cmd == "init":
        init_state(args.path, args.requested_max_workers, args.requested_retry_budget, args.lock_time)
        return 0

    if args.cmd == "finalize-success":
        finalize_success(args.path, args.run_metrics_path, args.lock_time)
        return 0

    if args.cmd == "finalize-failure":
        finalize_failure(args.path, args.run_metrics_path, args.lock_time)
        return 0

    raise RuntimeError("Unsupported command")


if __name__ == "__main__":
    raise SystemExit(main())
