#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


VALID_STATUSES = {"queued", "in_progress", "completed"}


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def gh_get_json(url: str, token: str, timeout: int = 20) -> Tuple[int, Dict[str, str], Optional[dict]]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "EOU-Dispatch-Verify/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            headers = {k: v for k, v in resp.headers.items()}
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body) if body else None
            return status, headers, data
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        data = None
        try:
            data = json.loads(body) if body else None
        except Exception:
            data = None
        return e.code, {k: v for k, v in e.headers.items()}, data
    except Exception:
        return 0, {}, None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True, help="owner/repo")
    p.add_argument("--workflow-file", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--expect-event", default="workflow_dispatch")
    p.add_argument("--max-wait-seconds", type=int, default=60)
    p.add_argument("--poll-seconds", type=int, default=3)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        print("Missing GH_TOKEN", file=sys.stderr)
        return 2

    if "/" not in args.repo:
        print("Invalid --repo, expected owner/repo", file=sys.stderr)
        return 2

    owner, repo = args.repo.split("/", 1)
    run_id = args.run_id.strip()
    if not run_id.isdigit():
        print(f"Invalid run id: {run_id}", file=sys.stderr)
        return 2

    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}"

    deadline = time.monotonic() + args.max_wait_seconds
    last_status = ""
    last_event = ""
    last_name = ""
    last_path = ""

    while time.monotonic() < deadline:
        status_code, _, data = gh_get_json(url, token=token, timeout=20)

        if status_code == 200 and isinstance(data, dict):
            last_status = str(data.get("status") or "")
            last_event = str(data.get("event") or "")
            last_name = str(data.get("name") or "")
            last_path = str(data.get("path") or "")

            print(
                f"[verify-dispatch] run_id={run_id} "
                f"status={last_status} event={last_event} "
                f"name={last_name} path={last_path}"
            )

            if last_event != args.expect_event:
                print(
                    f"Unexpected event for dispatched workflow run: {last_event}",
                    file=sys.stderr,
                )
                return 1

            if last_status in VALID_STATUSES:
                write_outputs(
                    {
                        "verified": "true",
                        "verified_status": last_status,
                        "verified_event": last_event,
                        "verified_name": last_name,
                        "verified_path": last_path,
                    }
                )
                return 0

        time.sleep(args.poll_seconds)

    print(
        f"Dispatched workflow run was not observable in a valid state within "
        f"{args.max_wait_seconds}s "
        f"(run_id={run_id}, last_status={last_status}, last_event={last_event})",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
