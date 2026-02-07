#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Tuple, Optional


def _ua(repo: str) -> str:
    return f"EOU-RQ-Gate-Kills/1.0 (+{repo}; GitHub Actions)"


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


def git_flush(raw_path: str, msg: str) -> None:
    # Commit/push durable (sin artifacts). Si falla push: pull --rebase y reintenta.
    def run(cmd: list[str]) -> Tuple[int, str]:
        p = subprocess.run(cmd, capture_output=True, text=True)
        return p.returncode, (p.stdout + p.stderr)

    # ¿hay cambios?
    rc, _ = run(["git", "diff", "--quiet", "--", raw_path])
    if rc == 0:
        return

    run(["git", "add", raw_path])
    rc, out = run(["git", "commit", "-m", msg])
    if rc != 0 and "nothing to commit" in out.lower():
        return

    rc, out = run(["git", "push"])
    if rc == 0:
        return

    # retry suave
    run(["git", "pull", "--rebase"])
    rc, out = run(["git", "push"])
    if rc != 0:
        print("WARN: git push failed after retry:", out, file=sys.stderr)


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
    p.add_argument("--no-git", action="store_true")
    return p.parse_args()


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
    next_flush = start + args.flush_seconds
    last_req = 0.0

    null_streak = 0
    received = 0
    accepted = 0
    discarded = 0
    flushes = 0

    http_429 = 0
    http_5xx = 0
    http_other = 0

    buffer: list[dict] = []
    poll_reason = "timeout"

    print(f"[poll] queue_id={queue_id}")
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= args.poll_max_seconds:
            poll_reason = "timeout"
            break
        if null_streak >= args.null_max:
            poll_reason = "null_streak"
            break

        # ritmo conservador
        since = time.monotonic() - last_req
        if since < args.min_interval:
            time.sleep(args.min_interval - since)

        url = args.base + "?" + urllib.parse.urlencode({"queueID": queue_id, "ttw": str(args.ttw)})
        status, headers, data = http_get_json(url, timeout=args.timeout, user_agent=_ua(args.repo))
        last_req = time.monotonic()

        if status == 200 and isinstance(data, dict) and "package" in data:
            pkg = data.get("package")
            if pkg is None:
                null_streak += 1
                print(f"[poll] package=null streak={null_streak}/{args.null_max}")
            elif isinstance(pkg, dict):
                null_streak = 0
                received += 1

                zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
                location_id = zkb.get("locationID")
                labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

                # Descarte 1
                pass_loc = isinstance(location_id, int) and (50000000 <= location_id <= 60000000)
                pass_pvp = "pvp" in labels
                pass_cat6 = "cat:6" in labels

                if pass_loc and pass_pvp and pass_cat6:
                    pkg["rearm"] = 0
                    buffer.append(pkg)
                    accepted += 1
                    print(f"[poll] killID={pkg.get('killID')} loc={location_id} PASS (buffer={len(buffer)})")
                else:
                    discarded += 1
                    print(f"[poll] killID={pkg.get('killID')} loc={location_id} FAIL "
                          f"(loc={pass_loc} pvp={pass_pvp} cat6={pass_cat6})")
            else:
                http_other += 1
                print("[poll] WARN: unexpected package type", file=sys.stderr)
        elif status == 429:
            http_429 += 1
            ra = headers.get("Retry-After")
            sleep_s = int(ra) if (ra and ra.isdigit()) else 5
            print(f"[poll] 429 rate limited; sleep={sleep_s}s")
            time.sleep(sleep_s)
        elif status in (500, 502, 503, 504):
            http_5xx += 1
            print(f"[poll] {status} server error; backoff 5s")
            time.sleep(5)
        elif status == 0:
            http_other += 1
            print("[poll] network/timeout error; backoff 5s", file=sys.stderr)
            time.sleep(5)
        else:
            http_other += 1
            print(f"[poll] HTTP {status}; backoff 3s", file=sys.stderr)
            time.sleep(3)

        # flush cada minuto (durable)
        now = time.monotonic()
        if now >= next_flush:
            if buffer:
                with open(args.raw, "a", encoding="utf-8") as f:
                    for obj in buffer:
                        f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
                msg = f"rq: poll flush (+{len(buffer)})"
                buffer.clear()
                flushes += 1
                print(f"[poll] flush #{flushes} committed")
                if not args.no_git:
                    git_flush(args.raw, msg)
            next_flush = now + args.flush_seconds

        # salvaguarda: demasiados problemas
        if http_other >= 40:
            poll_reason = "error"
            break

    # flush final
    if buffer:
        with open(args.raw, "a", encoding="utf-8") as f:
            for obj in buffer:
                f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        flushes += 1
        if not args.no_git:
            git_flush(args.raw, f"rq: poll final flush (+{len(buffer)})")
        buffer.clear()

    duration = int(time.monotonic() - start)
    write_outputs({
        "poll_reason": poll_reason,
        "queue_id": queue_id,
        "duration_seconds": duration,
        "received": received,
        "accepted": accepted,
        "discarded": discarded,
        "null_streak": null_streak,
        "flushes": flushes,
        "http_429": http_429,
        "http_5xx": http_5xx,
        "http_other": http_other,
    })
    print(f"[poll] done reason={poll_reason} dur={duration}s accepted={accepted} discarded={discarded} null_streak={null_streak}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
