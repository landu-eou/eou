#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RedisQ poller (durable queue writer).

Objetivo:
- Consultar RedisQ con un queueID estable (derivado del workflow) + suffix opcional.
- Filtrar paquetes (loc gate-range + labels pvp + cat:6).
- Persistir resultados en RAW (NDJSON) con flush cada N segundos, y commit+push por flush.
- Proteger contra abuso:
  - Stop temprano si hay racha de 429 (RedisQ busy por concurrencia de queueID).
  - Backoff con jitter en 5xx/errores de red.
  - Corte por null_streak o timeout.

Contrato:
- Input RAW: fichero NDJSON (se crea si no existe).
- Output: poll_reason via $GITHUB_OUTPUT.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, Optional, Tuple


def _ua(repo: str) -> str:
    return f"EOU-RQ-Gate-Kills/1.1 (+{repo}; GitHub Actions)"


def http_get_json(url: str, timeout: int, user_agent: str) -> Tuple[int, Dict[str, str], Optional[dict]]:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    try:
        with opener.open(req, timeout=timeout) as resp:
            status = resp.getcode()
            headers = {k: v for k, v in resp.headers.items()}
            body = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body) if body else None
            except json.JSONDecodeError:
                data = None
            return status, headers, data
    except urllib.error.HTTPError as e:
        return e.code, {k: v for k, v in e.headers.items()}, None
    except Exception:
        return 0, {}, None


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def _run(cmd: list[str]) -> int:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode


def git_flush(raw_path: str, msg: str) -> None:
    # Si no hay cambios, no hacemos nada.
    if _run(["git", "diff", "--quiet", "--", raw_path]) == 0:
        return

    _run(["git", "add", raw_path])
    _run(["git", "commit", "-m", msg])  # si no hay cambios reales, git falla pero no pasa nada
    if _run(["git", "push"]) == 0:
        return

    # Reintento conservador si hay race con otro push.
    _run(["git", "pull", "--rebase"])
    _run(["git", "push"])


def minimal_raw_line(pkg: dict) -> Optional[dict]:
    # RAW mínimo compatible con enrich:
    # - killID
    # - zkb.hash, zkb.locationID, zkb.labels
    # - rearm
    kill_id = pkg.get("killID")
    zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
    km_hash = zkb.get("hash")
    loc = zkb.get("locationID")
    labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

    if not isinstance(kill_id, int):
        return None
    if not isinstance(km_hash, str) or not km_hash:
        return None
    if not isinstance(loc, int):
        return None

    labels_out: list[str] = [x for x in labels if isinstance(x, str)]

    return {
        "killID": kill_id,
        "zkb": {
            "hash": km_hash,
            "locationID": loc,
            "labels": labels_out,
        },
        "rearm": 0,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--raw", required=True)
    p.add_argument("--workflow-file", required=True)
    p.add_argument("--repo", required=True, help="owner/repo")
    p.add_argument("--queue-suffix", default="")
    p.add_argument("--ttw", type=int, default=1)
    p.add_argument("--poll-max-seconds", type=int, default=1200)
    p.add_argument("--null-max", type=int, default=10)
    p.add_argument("--flush-seconds", type=int, default=60)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--min-interval", type=float, default=1.0)
    p.add_argument("--busy-429-max", type=int, default=8)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    os.makedirs(os.path.dirname(args.raw), exist_ok=True)
    if not os.path.exists(args.raw):
        open(args.raw, "a", encoding="utf-8").close()

    if "/" not in args.repo:
        return 2
    owner, repo = args.repo.split("/", 1)
    wf_base = args.workflow_file[:-4] if args.workflow_file.endswith(".yml") else args.workflow_file
    queue_id = f"{owner}/{repo}/{wf_base}" + (f"/{args.queue_suffix}" if args.queue_suffix else "")

    start = time.monotonic()
    last_req = 0.0
    next_flush = start + args.flush_seconds

    null_streak = 0
    http_other = 0
    http_429_streak = 0

    buffer: list[dict] = []
    poll_reason = "timeout"

    backoff = 0.0
    backoff_cap = 30.0

    def flush() -> None:
        if not buffer:
            return
        with open(args.raw, "a", encoding="utf-8") as f:
            for obj in buffer:
                f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        n = len(buffer)
        buffer.clear()
        git_flush(args.raw, f"rq: poll flush (+{n})")

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= args.poll_max_seconds:
            poll_reason = "timeout"
            break
        if null_streak >= args.null_max:
            poll_reason = "null_streak"
            break
        if http_other >= 40:
            poll_reason = "error"
            break
        if http_429_streak >= args.busy_429_max:
            poll_reason = "redisq_busy"
            break

        now = time.monotonic()
        if now >= next_flush:
            flush()
            next_flush += args.flush_seconds

        since = time.monotonic() - last_req
        if since < args.min_interval:
            time.sleep(args.min_interval - since)

        url = args.base + "?" + urllib.parse.urlencode({"queueID": queue_id, "ttw": str(args.ttw)})
        status, headers, data = http_get_json(url, timeout=args.timeout, user_agent=_ua(args.repo))
        last_req = time.monotonic()

        if status == 200 and isinstance(data, dict) and "package" in data:
            http_429_streak = 0
            pkg = data.get("package")
            if pkg is None:
                null_streak += 1
            elif isinstance(pkg, dict):
                null_streak = 0

                zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
                location_id = zkb.get("locationID")
                labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

                pass_loc = isinstance(location_id, int) and (50000000 <= location_id <= 60000000)
                pass_pvp = any(isinstance(x, str) and x == "pvp" for x in labels)
                pass_cat6 = any(isinstance(x, str) and x == "cat:6" for x in labels)

                if pass_loc and pass_pvp and pass_cat6:
                    line = minimal_raw_line(pkg)
                    if line is not None:
                        buffer.append(line)

                backoff = 0.0
            else:
                http_other += 1

        elif status == 429:
            http_429_streak += 1
            ra = headers.get("Retry-After")
            base_sleep = int(ra) if (ra and ra.isdigit()) else 5
            time.sleep(base_sleep + random.uniform(0.0, 1.5))

        elif status in (500, 502, 503, 504) or status == 0:
            http_429_streak = 0
            backoff = min(backoff_cap, backoff * 2.0 if backoff > 0.0 else 5.0)
            time.sleep(backoff + random.uniform(0.0, 1.0))

        else:
            http_other += 1
            http_429_streak = 0
            backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
            time.sleep(backoff + random.uniform(0.0, 1.0))

    flush()

    write_outputs({"poll_reason": poll_reason})
    return 1 if poll_reason == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
