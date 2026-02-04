#!/usr/bin/env python3
import json
import os
import time
import datetime as dt
import urllib.request
import urllib.parse
from typing import Any, Dict, Optional, Set

# RedisQ endpoint actual
REDISQ_BASE = "https://zkillredisq.stream/listen.php"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def build_url(queue_id: str, ttw: int) -> str:
    params = {"queueID": queue_id, "ttw": str(ttw)}
    return f"{REDISQ_BASE}?{urllib.parse.urlencode(params)}"


def http_get_json(url: str, timeout_s: int) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "gh-actions-redisq-poller/2.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_kill_id(payload: Dict[str, Any]) -> Optional[str]:
    pkg = payload.get("package") or {}
    for key in ("killID", "killmail_id", "killmailID", "kill_id"):
        if key in pkg and pkg[key] is not None:
            return str(pkg[key])
    return None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def get_daily_ndjson_path(base_dir: str) -> str:
    now = dt.datetime.now(dt.timezone.utc)
    out_dir = os.path.join(base_dir, f"{now.year:04d}", f"{now.month:02d}", f"{now.day:02d}")
    ensure_dir(out_dir)
    return os.path.join(out_dir, "redisq.ndjson")


def append_ndjson_line(path: str, record: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")


def main() -> None:
    """
    Requisitos:
    - Poll "cada 10 segundos" (aprox): usamos ttw=10 para long-poll; y luego dormimos el remanente
      para que cada iteración dure ~10s.
    - Termina cuando ocurra una de estas condiciones:
        (1) hayan pasado 20 minutos
        (2) reciba package null 10 veces seguidas
    - Log en vivo (flush=True) y NDJSON diario en redisq_kills/YYYY/MM/DD/redisq.ndjson
    """
    queue_id = os.getenv("QUEUE_ID", "").strip()
    if not queue_id:
        raise SystemExit("QUEUE_ID is required (ej: QUEUE_ID: 'sarandonga').")

    poll_every_s = 10  # pedido
    max_runtime_s = 20 * 60  # 20 minutos
    max_consecutive_nulls = 10  # pedido

    # RedisQ permite ttw 1..10
    ttw = clamp(poll_every_s, 1, 10)

    url = build_url(queue_id, ttw)
    out_path = get_daily_ndjson_path("redisq_kills")

    seen: Set[str] = set()
    appended = 0
    errors = 0
    dups = 0

    consecutive_nulls = 0

    start = time.time()
    deadline = start + max_runtime_s

    print(
        f"[{utc_now_iso()}] START | queueID={queue_id} | poll_every≈{poll_every_s}s | ttw={ttw}s | "
        f"max_runtime={max_runtime_s}s | stop_on_nulls={max_consecutive_nulls}",
        flush=True,
    )
    print(f"[{utc_now_iso()}] URL {url}", flush=True)
    print(f"[{utc_now_iso()}] OUT {out_path}", flush=True)

    # Heartbeat cada 30s
    next_hb = start + 30

    while True:
        now = time.time()

        # Condición (1): runtime >= 20 min
        if now >= deadline:
            print(f"[{utc_now_iso()}] STOP reason=max_runtime_reached", flush=True)
            break

        # Heartbeat
        if now >= next_hb:
            elapsed = int(now - start)
            remaining = max(0, int(deadline - now))
            print(
                f"[{utc_now_iso()}] HB elapsed={elapsed}s remaining={remaining}s kills={appended} "
                f"nulls_streak={consecutive_nulls}/{max_consecutive_nulls} dups={dups} errors={errors}",
                flush=True,
            )
            next_hb = now + 30

        iter_start = time.time()
        timeout_s = min(30, int(deadline - iter_start) + 1)  # no te pases del deadline; margen +1

        try:
            payload = http_get_json(url, timeout_s=timeout_s)
        except Exception as e:
            errors += 1
            # En error, NO incrementamos nulls_streak (porque no es "package null" real)
            print(f"[{utc_now_iso()}] WARN http/json error: {e}", flush=True)
            # backoff corto
            time.sleep(2)
            continue

        kill_id = extract_kill_id(payload)

        if kill_id:
            # Reset streak de nulls si llega un kill
            consecutive_nulls = 0

            if kill_id in seen:
                dups += 1
                print(f"[{utc_now_iso()}] DUP  kill_id={kill_id} (same run)", flush=True)
            else:
                seen.add(kill_id)
                record = {
                    "meta": {
                        "source": "redisq",
                        "ingested_at_utc": utc_now_iso(),
                        "queue_id": queue_id,
                        "kill_id": kill_id,
                    },
                    "raw": payload,
                }
                append_ndjson_line(out_path, record)
                appended += 1
                print(f"[{utc_now_iso()}] KILL kill_id={kill_id} appended -> {out_path}", flush=True)

        else:
            consecutive_nulls += 1
            print(
                f"[{utc_now_iso()}] NULL package=null streak={consecutive_nulls}/{max_consecutive_nulls}",
                flush=True,
            )

            # Condición (2): 10 null seguidos
            if consecutive_nulls >= max_consecutive_nulls:
                print(f"[{utc_now_iso()}] STOP reason=10_consecutive_nulls", flush=True)
                break

        # Intento de "poll cada 10s":
        # - El request ya espera hasta ttw (10) segundos.
        # - Si vuelve antes, dormimos el remanente para que el ciclo sea ~10s.
        elapsed_iter = time.time() - iter_start
        sleep_for = poll_every_s - elapsed_iter
        if sleep_for > 0:
            time.sleep(sleep_for)

    total = int(time.time() - start)
    print(
        f"[{utc_now_iso()}] END total={total}s | appended={appended} | dups={dups} | errors={errors} "
        f"| final_null_streak={consecutive_nulls} | out={out_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
