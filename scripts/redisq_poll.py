#!/usr/bin/env python3
import json
import os
import time
import datetime as dt
import urllib.request
import urllib.parse
from typing import Any, Dict, Optional, Set

# ✅ Endpoint actual (el viejo redisq.zkillboard.com ya no es el recomendado)
REDISQ_BASE = "https://zkillredisq.stream/listen.php"

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def build_url(queue_id: str, ttw: int) -> str:
    # RedisQ identifica al cliente por queueID (en el README lo marcan como required)
    # y permite ttw=1..10 para esperar menos tiempo por request.
    params = {
        "queueID": queue_id,
        "ttw": str(ttw),
    }
    return f"{REDISQ_BASE}?{urllib.parse.urlencode(params)}"

def http_get_json(url: str, timeout_s: int = 30) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "gh-actions-redisq-poller/1.1",
            "Accept": "application/json",
        },
        method="GET",
    )
    # urllib sigue redirects por defecto (importante desde el cambio de /listen.php → /object.php)
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))

def extract_kill_id(payload: Dict[str, Any]) -> Optional[str]:
    pkg = payload.get("package") or {}
    for key in ("killID", "killmail_id", "killmailID", "kill_id"):
        if key in pkg and pkg[key] is not None:
            return str(pkg[key])
    return None

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_kill_file(base_dir: str, kill_id: str, payload: Dict[str, Any]) -> str:
    now = dt.datetime.now(dt.timezone.utc)
    out_dir = os.path.join(base_dir, f"{now.year:04d}", f"{now.month:02d}", f"{now.day:02d}")
    ensure_dir(out_dir)

    out_path = os.path.join(out_dir, f"{kill_id}.json")
    record = {
        "meta": {"source": "redisq", "ingested_at_utc": utc_now_iso(), "kill_id": kill_id},
        "raw": payload,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return out_path

def main() -> None:
    queue_id = os.getenv("QUEUE_ID", "").strip()
    if not queue_id:
        raise SystemExit("QUEUE_ID is required (set it in workflow env).")

    poll_interval = int(float(os.getenv("POLL_INTERVAL_SECONDS", "3")))
    ttw = clamp(poll_interval, 1, 10)  # RedisQ ttw permitido 1..10
    duration = int(float(os.getenv("DURATION_SECONDS", "90")))

    url = build_url(queue_id, ttw)
    base_dir = "redisq_kills"

    seen: Set[str] = set()
    wrote = 0

    start = time.time()
    deadline = start + duration

    print(f"[info] RedisQ URL: {url}")
    print(f"[info] ttw: {ttw}s | Duration: {duration}s")

    while time.time() < deadline:
        try:
            payload = http_get_json(url)
        except Exception as e:
            print(f"[warn] HTTP/JSON error: {e}")
            time.sleep(1)
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
            # normal: {"package": null}
            print("[info] no kill in this poll")

        # Nota: NO hace falta dormir poll_interval aquí:
        # el propio RedisQ ya espera hasta ttw segundos por request.
        # Aun así, un micro-sleep evita bucles demasiado agresivos.
        time.sleep(0.1)

    print(f"[done] wrote {wrote} file(s) in {duration}s")

if __name__ == "__main__":
    main()
