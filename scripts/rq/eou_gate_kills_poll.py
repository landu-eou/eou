#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
import urllib.parse
import urllib.request
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
        status = e.code
        headers = {k: v for k, v in e.headers.items()}
        return status, headers, None
    except Exception:
        return 0, {}, None


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def _run(cmd: list[str]) -> Tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr)


def git_flush(raw_path: str, msg: str) -> None:
    rc, _ = _run(["git", "diff", "--quiet", "--", raw_path])
    if rc == 0:
        return

    _run(["git", "add", raw_path])
    rc, out = _run(["git", "commit", "-m", msg])
    if rc != 0 and "nothing to commit" in out.lower():
        return

    rc, out = _run(["git", "push"])
    if rc == 0:
        return

    _run(["git", "pull", "--rebase"])
    rc, out = _run(["git", "push"])
    if rc != 0:
        print(f"::warning:: git push failed after retry: {out[:500]}", file=sys.stderr)


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
    p.add_argument("--busy-429-max", type=int, default=8, help="si 429 consecutivos >= N => redisq_busy y se corta")
    p.add_argument("--no-git", action="store_true")
    return p.parse_args()


def minimal_raw_line(pkg: dict) -> Optional[dict]:
    if not isinstance(pkg, dict):
        return None
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

    labels_out: list[str] = []
    for x in labels:
        if isinstance(x, str):
            labels_out.append(x)

    return {
        "killID": kill_id,
        "zkb": {
            "hash": km_hash,
            "locationID": loc,
            "labels": labels_out,
        },
        "rearm": 0,
    }


def main() -> int:
    args = parse_args()

    os.makedirs(os.path.dirname(args.raw), exist_ok=True)
    if not os.path.exists(args.raw):
        open(args.raw, "a", encoding="utf-8").close()

    owner_repo = args.repo
    if "/" not in owner_repo:
        print("ERROR: --repo must be owner/repo", file=sys.stderr)
        return 2
    owner, repo = owner_repo.split("/", 1)
    wf_base = args.workflow_file[:-4] if args.workflow_file.endswith(".yml") else args.workflow_file
    queue_id = f"{owner}/{repo}/{wf_base}" + (f"/{args.queue_suffix}" if args.queue_suffix else "")

    start = time.monotonic()
    last_req = 0.0

    window_idx = 1
    window_end = start + args.flush_seconds
    print(f"::group::RedisQ poll window {window_idx}")
    print(f"[poll] queue_id={queue_id}")

    null_streak = 0
    received = 0
    accepted = 0
    discarded = 0
    flushes = 0

    http_429 = 0
    http_5xx = 0
    http_other = 0
    http_429_streak = 0
    http_429_streak_max = 0

    buffer: list[dict] = []
    poll_reason = "timeout"

    backoff = 0.0  # para 5xx / network / otros
    backoff_cap = 30.0

    def do_flush(label: str) -> None:
        nonlocal flushes
        if not buffer:
            return
        with open(args.raw, "a", encoding="utf-8") as f:
            for obj in buffer:
                f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        n = len(buffer)
        buffer.clear()
        flushes += 1
        print(f"[poll] flush #{flushes} ({label}) +{n}")
        if not args.no_git:
            git_flush(args.raw, f"rq: poll flush (+{n})")

    try:
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
                print("::warning:: poll stop: too many http_other/network errors")
                break
            if http_429_streak >= args.busy_429_max:
                poll_reason = "redisq_busy"
                print(f"::notice:: poll stop: redisq_busy (429 streak {http_429_streak}/{args.busy_429_max})")
                break

            # agrupación por minuto/ventana
            now = time.monotonic()
            if now >= window_end:
                do_flush("window_end")
                print("::endgroup::")
                window_idx += 1
                window_end += args.flush_seconds
                print(f"::group::RedisQ poll window {window_idx}")

            # ritmo conservador
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
                    print(f"[poll] package=null streak={null_streak}/{args.null_max}")
                elif isinstance(pkg, dict):
                    null_streak = 0
                    received += 1

                    # Descarte 1
                    zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
                    location_id = zkb.get("locationID")
                    labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

                    pass_loc = isinstance(location_id, int) and (50000000 <= location_id <= 60000000)
                    pass_pvp = any(isinstance(x, str) and x == "pvp" for x in labels)
                    pass_cat6 = any(isinstance(x, str) and x == "cat:6" for x in labels)

                    if pass_loc and pass_pvp and pass_cat6:
                        line = minimal_raw_line(pkg)
                        if line is None:
                            discarded += 1
                            print(f"[poll] killID={pkg.get('killID')} FAIL (minimalize)")
                        else:
                            buffer.append(line)
                            accepted += 1
                            print(f"[poll] killID={line['killID']} loc={line['zkb']['locationID']} PASS (buffer={len(buffer)})")
                    else:
                        discarded += 1
                        print(
                            f"[poll] killID={pkg.get('killID')} loc={location_id} FAIL "
                            f"(loc={pass_loc} pvp={pass_pvp} cat6={pass_cat6})"
                        )

                    backoff = 0.0
                else:
                    http_other += 1
                    print("[poll] WARN: unexpected package type", file=sys.stderr)

            elif status == 429:
                http_429 += 1
                http_429_streak += 1
                http_429_streak_max = max(http_429_streak_max, http_429_streak)

                ra = headers.get("Retry-After")
                base_sleep = int(ra) if (ra and ra.isdigit()) else 5
                sleep_s = base_sleep + random.uniform(0.0, 1.5)
                print(f"[poll] 429 rate limited; streak={http_429_streak}/{args.busy_429_max} sleep={sleep_s:.1f}s")
                time.sleep(sleep_s)

            elif status in (500, 502, 503, 504) or status == 0:
                http_5xx += 1
                http_429_streak = 0
                backoff = min(backoff_cap, backoff * 2.0 if backoff > 0.0 else 5.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                print(f"[poll] server/network error ({status}); backoff={sleep_s:.1f}s")
                time.sleep(sleep_s)

            else:
                http_other += 1
                http_429_streak = 0
                backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                print(f"[poll] HTTP {status}; backoff={sleep_s:.1f}s", file=sys.stderr)
                time.sleep(sleep_s)

        # flush final (durable)
        do_flush("final")

    finally:
        print("::endgroup::")

    duration = int(time.monotonic() - start)
    write_outputs(
        {
            "poll_reason": poll_reason,
            "queue_id": queue_id,
            "duration_seconds": duration,
            "received": received,
            "accepted": accepted,
            "discarded": discarded,
            "null_streak": null_streak,
            "flushes": flushes,
            "http_429": http_429,
            "http_429_streak_max": http_429_streak_max,
            "http_5xx": http_5xx,
            "http_other": http_other,
        }
    )
    print(f"[poll] done reason={poll_reason} dur={duration}s accepted={accepted} discarded={discarded} null_streak={null_streak}")

    # Si error “real”, falla el step para que el workflow marque failed y se vea claro
    return 1 if poll_reason == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
