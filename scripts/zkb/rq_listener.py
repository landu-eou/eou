import argparse
import json
import os
import time
import hashlib
from datetime import datetime, timezone

import requests

REDISQ_URL = "https://redisq.zkillboard.com/listen.php"


def sha256_id(killmail_id: int) -> str:
    return hashlib.sha256(str(killmail_id).encode("utf-8")).hexdigest()


def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue-id", required=True)
    ap.add_argument("--ttw", type=int, default=10)
    ap.add_argument("--max-packages", type=int, default=120)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    out_file = os.path.join(
        args.out_dir, f"rq_raw_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.ndjson"
    )

    got = 0
    with open(out_file, "w", encoding="utf-8") as f:
        for _ in range(args.max_packages):
            params = {"queueID": args.queue_id, "ttw": str(args.ttw)}

            try:
                r = requests.get(
                    REDISQ_URL,
                    params=params,
                    timeout=args.ttw + 10,
                    allow_redirects=True,
                )
            except requests.RequestException:
                # Respetuoso: si hay problemas de red, corta y que el próximo run reintente.
                break

            if r.status_code == 429:
                # RedisQ/Cloudflare rate limit: corta.
                break

            if r.status_code != 200:
                break

            payload = r.json()
            pkg = payload.get("package")
            if not pkg:
                # long-poll sin evento
                continue

            km_id = pkg.get("killmail_id")
            km_hash = pkg.get("hash")
            zkb = pkg.get("zkb", {}) or {}

            if km_id is None or km_hash is None:
                continue

            row = {
                "request_ts": now_ts(),
                "killmail_id": int(km_id),
                "killmail_hash": str(km_hash),
                "killmail_id_sha256": sha256_id(int(km_id)),
                "location_id": int(pkg.get("locationID")) if pkg.get("locationID") is not None else None,
                "labels": list(pkg.get("labels", []) or []),
                "zkb_npc": bool(zkb.get("npc")) if "npc" in zkb else None,
                "zkb_awox": bool(zkb.get("awox")) if "awox" in zkb else None,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            got += 1

    with open(os.path.join(args.out_dir, "raw_file.txt"), "w", encoding="utf-8") as wf:
        wf.write(out_file)

    print(f"RedisQ packages written: {got} -> {out_file}")


if __name__ == "__main__":
    main()
