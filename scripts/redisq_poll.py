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


def http_get_json(url: str, timeout_s: int = 30) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "gh-actions-redisq-poller/1.4",
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
    # NDJSON: 1 JSON por línea
    with open(path, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")


def main() -> None:
    # Nota: además de flush=True, en Actions conviene setear PYTHONUNBUFFERED=1 en el workflow.
    queue_id = os.getenv("QUEUE_ID", "").strip()
    if not queue_id:
        raise SystemExit("QUEUE_ID is required (ej: QUEUE_ID: 'sarandonga').")

    poll_interval = int(float(os.getenv("POLL_INTERVAL_SECONDS", "3")))
    ttw = clamp(poll_interval, 1, 10)
    duration = int(float(os.getenv("DURATION_SECONDS", "90")))

    url = build_url(queue_id, ttw)
    base_dir = "redisq_kills"
    out_path = get_daily_ndjson_path(base_dir)

    seen: Set[str] = set()
    appended = 0
    errors = 0
    empties = 0
    dups = 0

    start = time.time()
    deadline = start + duration

    print(
        f"[{utc_now_iso()}] START redisq poll | queueID={queue_id} | ttw={ttw}s | duration={duration}s",
        flush=True,
    )
    print(f"[{utc_now_iso()}] URL {url}", flush=True)
    print(f"[{utc_now_iso()}] OUT {out_path}", flush=True)

    # Heartbeat para que siempre “veas vida” aunque no haya kills
    next_heartbeat = start + 10

    while time.time() < deadline:
        now = time.time()

        if now >= next_heartbeat:
            elapsed = int(now - start)
            remaining = max(0, int(deadline - now))
            print(
                f"[{utc_now_iso()}] HB elapsed={elapsed}s remaining={remaining}s kills={appended} idle={empties} dups={dups} errors={errors}",
                flush=True,
            )
            next_heartbeat = now + 10

        try:
            payload = http_get_json(url)
        except Exception as e:
            errors += 1
            print(f"[{utc_now_iso()}] WARN http/json error: {e}", flush=True)
            time.sleep(1)
            continue

        kill_id = extract_kill_id(payload)
        if kill_id:
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
            empties += 1
            print(f"[{utc_now_iso()}] IDLE no kill", flush=True)

        # RedisQ ya hace long-polling hasta ttw; micro-sleep evita bucle demasiado agresivo
        time.sleep(0.05)

    elapsed = int(time.time() - start)
    print(
        f"[{utc_now_iso()}] END elapsed={elapsed}s | appended={appended} | idle={empties} | dups={dups} | errors={errors} | out={out_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
