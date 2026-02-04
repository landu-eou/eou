#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple


REDISQ_URL = "https://zkillredisq.stream/listen.php"
REDISQ_TTW = 1  # seconds


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def read_jsonl_gz(path: str) -> List[dict]:
    """Read gzipped JSONL. If file missing/empty, returns [] (no error)."""
    if not os.path.exists(path):
        return []
    items: List[dict] = []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                items.append(json.loads(line))
    except OSError:
        # Not a valid gzip? treat as empty to be resilient.
        return []
    except json.JSONDecodeError:
        # Corrupt line(s) -> treat as empty in this minimal bootstrap version.
        return []
    return items


def write_jsonl_gz(path: str, rows: Iterable[dict]) -> None:
    ensure_parent_dir(path)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")


def write_text(path: str, content: str) -> None:
    ensure_parent_dir(path)
    with open(path, "wt", encoding="utf-8") as f:
        f.write(content)


def build_queue_id() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    wf = os.environ.get("WORKFLOW_FILE", "")
    suffix = os.environ.get("QUEUE_SUFFIX", "")
    return f"rq-{repo}-{wf}{suffix}"


def redisq_listen(queue_id: str) -> Tuple[bool, Optional[dict], Optional[str]]:
    """Single RedisQ request. Returns (ok, payload_json, error_str)."""
    params = f"?queueID={urllib.request.quote(queue_id)}&ttw={REDISQ_TTW}"
    url = REDISQ_URL + params

    req = urllib.request.Request(
        url,
        headers={
            # RedisQ doesn't require UA, but it is good practice anyway.
            "User-Agent": "EOU Gate Kills (GitHub Actions)",
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw)
            return True, data, None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pending", required=True)
    ap.add_argument("--stargates", required=True)
    ap.add_argument("--types", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    metrics_path = os.path.join(out_dir, "metrics.json")
    insert_rows_path = os.path.join(out_dir, "insert_rows.ndjson")
    pending_keep_path = os.path.join(out_dir, "pending_keep.jsonl.gz")
    pending_with_ready_path = os.path.join(out_dir, "pending_with_ready.jsonl.gz")

    rq_time_iso = utc_now_iso()
    rq_ok = False
    rq_error = None

    try:
        # Read pending (may be empty/missing)
        pending_items = read_jsonl_gz(args.pending)

        # RedisQ 1 request
        queue_id = build_queue_id()
        ok, payload, err = redisq_listen(queue_id)
        rq_ok = ok
        rq_error = err

        # Minimal version: no enrich, no new inserts
        insert_rows_count = 0

        # Always write empty NDJSON (no rows)
        write_text(insert_rows_path, "")

        # Always write pending outputs (keep as-is)
        write_jsonl_gz(pending_keep_path, pending_items)
        write_jsonl_gz(pending_with_ready_path, pending_items)

        metrics = {
            "rq_ok": rq_ok,
            "rq_time_iso": rq_time_iso,
            "rq_error": rq_error,
            "queue_id": queue_id,
            "insert_rows_count": insert_rows_count,
        }

        # Always write metrics.json
        write_text(metrics_path, json.dumps(metrics, indent=2, ensure_ascii=False) + "\n")

        # Print metrics to stdout for log visibility
        print(json.dumps(metrics, ensure_ascii=False))

        return 0

    except Exception as e:
        # Hard fallback: still write minimal metrics + empty outputs
        try:
            write_text(insert_rows_path, "")
            write_jsonl_gz(pending_keep_path, [])
            write_jsonl_gz(pending_with_ready_path, [])
        except Exception:
            pass

        fallback = {
            "rq_ok": False,
            "rq_time_iso": rq_time_iso,
            "rq_error": f"FATAL {type(e).__name__}: {e}",
            "queue_id": build_queue_id(),
            "insert_rows_count": 0,
        }
        try:
            write_text(metrics_path, json.dumps(fallback, indent=2, ensure_ascii=False) + "\n")
        except Exception:
            pass

        print(json.dumps(fallback, ensure_ascii=False))
        # No hacemos fail hard para no romper Sheets finish.
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
