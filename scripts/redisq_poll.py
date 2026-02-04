#!/usr/bin/env python3
import json
import os
import time
import datetime as dt
import urllib.request
import urllib.parse
from typing import Any, Dict, Optional, Set


REDISQ_BASE = "https://redisq.zkillboard.com/listen.php"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def build_url(queue_id: str) -> str:
    # queueID es opcional en RedisQ; si lo pasas, te “separa” tu cola.
    if queue_id:
        qs = urllib.parse.urlencode({"queueID": queue_id})
        return f"{REDISQ_BASE}?{qs}"
    return REDISQ_BASE


def http_get_json(url: str, timeout_s: int = 20) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "gh-actions-redisq-poller/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def extract_kill_id(payload: Dict[str, Any]) -> Optional[str]:
    # RedisQ suele devolver algo como:
    # { "package": { "killID": 123, ... } }  (o a veces killmail_id según upstream)
    pkg = payload.get("package") or {}
    for key in ("killID", "killmail_id", "killmailID", "kill_id"):
        if key in pkg and pkg[key] is not None:
            return str(pkg[key])
    return None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_kill_file(base_dir: str, kill_id: str, payload: Dict[str, Any]) -> str:
    # Estructura: redisq_kills/YYYY/MM/DD/<kill_id>.json
    now = dt.datetime.now(dt.timezone.utc)
    out_dir = os.path.join(
        base_dir,
        f"{now.year:04d}",
        f"{now.month:02d}",
        f"{now.day:02d}",
    )
    ensure_dir(out_dir)

    out_path = os.path.join(out_dir, f"{kill_id}.json")
    record = {
        "meta": {
            "source": "redisq",
            "ingested_at_utc": utc_now_iso(),
            "kill_id": kill_id,
        },
        "raw": payload,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return out_path


def main() -> None:
    queue_id = os.getenv("QUEUE_ID", "").strip()
    poll_interval = int(float(os.getenv("POLL_INTERVAL_SECONDS", "3")))
    duration = int(float(os.getenv("DURATION_SECONDS", "180")))

    url = build_url(queue_id)
    base_dir = "redisq_kills"

    seen: Set[str] = set()
    wrote = 0

    start = time.time()
    deadline = start + duration

    print(f"[info] RedisQ URL: {url}")
    print(f"[info] Poll interval: {poll_interval}s | Duration: {duration}s")

    while time.time() < deadline:
        try:
            payload = http_get_json(url)
        except Exception as e:
            # No rompemos el run por un fallo de red puntual
            print(f"[warn] HTTP/JSON error: {e}")
            time.sleep(poll_interval)
            continue

        kill_id = extract_kill_id(payload)
        if kill_id:
            if kill_id not in seen:
                seen.add(kill_id)
                path = write_kill_file(base_dir, kill_id, payload)
                wrote += 1
                print(f"[ok] wrote kill {kill_id} -> {path}")
            else:
                print(f"[skip] duplicate kill {kill_id} (same run)")
        else:
            # Normal si no hay kill disponible: RedisQ puede devolver package null / vacío.
            print("[info] no kill in this poll")

        time.sleep(poll_interval)

    print(f"[done] wrote {wrote} file(s) in {duration}s")


if __name__ == "__main__":
    main()
