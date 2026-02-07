#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Set


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def run_cmd(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def date_of_snapshot(snapshot_ts: str) -> str:
    # "2026-02-02T12:34:56Z" -> "2026-02-02"
    if not snapshot_ts:
        return ""
    return snapshot_ts[:10]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--batch", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    target = f"{args.project}:{args.dataset}.{args.table}"

    # read batch
    rows: List[dict] = []
    ids: List[int] = []
    dates_set: Set[str] = set()

    if not os.path.exists(args.batch):
        write_outputs({
            "candidate_rows": 0,
            "duplicate_rows": 0,
            "inserted_rows": 0,
            "load_skipped": "true",
            "dedupe_dates": "",
            "stop_reason": "completed",
        })
        return 0

    with open(args.batch, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            kmid = obj.get("killmailID")
            snap = obj.get("snapshot_ts")
            if not isinstance(kmid, int) or not isinstance(snap, str) or not snap:
                continue
            rows.append(obj)
            ids.append(kmid)
            d = date_of_snapshot(snap)
            if d:
                dates_set.add(d)

    candidate_rows = len(rows)
    if candidate_rows == 0:
        write_outputs({
            "candidate_rows": 0,
            "duplicate_rows": 0,
            "inserted_rows": 0,
            "load_skipped": "true",
            "dedupe_dates": "",
            "stop_reason": "completed",
        })
        return 0

    # Dedupe query (1 query)
    ids_unique = sorted(set(ids))
    dates_sorted = sorted(dates_set)
    dedupe_dates = ",".join(dates_sorted)

    # Build SQL using array literals (evita líos de parámetros del CLI)
    ids_sql = ",".join(str(x) for x in ids_unique)
    dates_sql = ",".join(f"DATE '{d}'" for d in dates_sorted)

    sql = f"""
    SELECT killmailID
    FROM `{args.project}.{args.dataset}.{args.table}`
    WHERE killmailID IN UNNEST([{ids_sql}])
      AND DATE(snapshot_ts) IN UNNEST([{dates_sql}])
    """

    p = run_cmd([
        "bq", "query",
        "--quiet",
        "--use_legacy_sql=false",
        "--format=json",
        "--project_id", args.project,
        sql
    ])

    if p.returncode != 0:
        print(p.stdout, file=sys.stderr)
        print(p.stderr, file=sys.stderr)
        write_outputs({
            "candidate_rows": candidate_rows,
            "duplicate_rows": 0,
            "inserted_rows": 0,
            "load_skipped": "true",
            "dedupe_dates": dedupe_dates,
            "stop_reason": "bq_query_failed",
        })
        return 1

    existing: Set[int] = set()
    try:
        data = json.loads(p.stdout) if p.stdout.strip() else []
        for r in data:
            v = r.get("killmailID")
            if isinstance(v, str) and v.isdigit():
                existing.add(int(v))
            elif isinstance(v, int):
                existing.add(v)
    except Exception:
        # si falla parse, por seguridad: no cargues nada
        write_outputs({
            "candidate_rows": candidate_rows,
            "duplicate_rows": 0,
            "inserted_rows": 0,
            "load_skipped": "true",
            "dedupe_dates": dedupe_dates,
            "stop_reason": "bq_query_failed",
        })
        return 1

    new_rows = [r for r in rows if r["killmailID"] not in existing]
    duplicate_rows = candidate_rows - len(new_rows)

    if not new_rows:
        write_outputs({
            "candidate_rows": candidate_rows,
            "duplicate_rows": duplicate_rows,
            "inserted_rows": 0,
            "load_skipped": "true",
            "dedupe_dates": dedupe_dates,
            "stop_reason": "completed",
        })
        return 0

    load_file = "/tmp/gate_kills_to_load.ndjson"
    with open(load_file, "w", encoding="utf-8") as f:
        for r in new_rows:
            f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")

    p2 = run_cmd([
        "bq", "load",
        "--quiet",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--ignore_unknown_values=true",
        target,
        load_file
    ])

    if p2.returncode != 0:
        print(p2.stdout, file=sys.stderr)
        print(p2.stderr, file=sys.stderr)
        write_outputs({
            "candidate_rows": candidate_rows,
            "duplicate_rows": duplicate_rows,
            "inserted_rows": 0,
            "load_skipped": "false",
            "dedupe_dates": dedupe_dates,
            "stop_reason": "bq_load_failed",
        })
        return 1

    inserted_rows = len(new_rows)
    write_outputs({
        "candidate_rows": candidate_rows,
        "duplicate_rows": duplicate_rows,
        "inserted_rows": inserted_rows,
        "load_skipped": "false",
        "dedupe_dates": dedupe_dates,
        "stop_reason": "completed",
    })
    print(f"[load] inserted_rows={inserted_rows} duplicates={duplicate_rows} dates={dedupe_dates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
