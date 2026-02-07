#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Set, Tuple


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
    return snapshot_ts[:10] if snapshot_ts else ""


def extract_json(stdout: str) -> Any:
    """
    bq --format=json debería devolver JSON puro, pero a veces hay ruido.
    Extraemos desde el primer '[' o '{' y parseamos.
    """
    s = stdout.strip()
    if not s:
        return []
    i_list = s.find("[")
    i_obj = s.find("{")
    candidates = [i for i in [i_list, i_obj] if i != -1]
    if not candidates:
        raise ValueError("No JSON found in stdout")
    i = min(candidates)
    return json.loads(s[i:])


def bq_query_existing_ids(project: str, dataset: str, table: str, ids: List[int], dates: List[str]) -> Tuple[Set[int], str]:
    ids_sql = ",".join(str(x) for x in ids)
    dates_sql = ",".join(f"DATE '{d}'" for d in dates)

    sql = f"""
    SELECT killmailID
    FROM `{project}.{dataset}.{table}`
    WHERE killmailID IN UNNEST([{ids_sql}])
      AND DATE(snapshot_ts) IN UNNEST([{dates_sql}])
    """

    cmd = [
        "bq", "query",
        "--quiet",
        "--use_legacy_sql=false",
        "--format=json",
        "--project_id", project,
        sql
    ]

    p = run_cmd(cmd)
    if p.returncode != 0:
        return set(), (p.stdout + "\n" + p.stderr)

    data = extract_json(p.stdout)
    existing: Set[int] = set()
    if isinstance(data, list):
        for r in data:
            if not isinstance(r, dict):
                continue
            v = r.get("killmailID")
            if isinstance(v, int):
                existing.add(v)
            elif isinstance(v, str) and v.isdigit():
                existing.add(int(v))
    return existing, ""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--batch", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    target_cli = f"{args.project}:{args.dataset}.{args.table}"

    # Leer batch
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

    ids_unique = sorted(set(ids))
    dates_sorted = sorted(dates_set)
    dedupe_dates = ",".join(dates_sorted)

    # 1 query dedupe con 1 retry si es “not found” justo después de crear tabla
    existing: Set[int] = set()
    err = ""
    for attempt in [1, 2]:
        existing, err = bq_query_existing_ids(args.project, args.dataset, args.table, ids_unique, dates_sorted)
        if err and ("Not found" in err or "notFound" in err) and attempt == 1:
            print("[load] WARN: table not found right after create; retry in 3s", file=sys.stderr)
            time.sleep(3)
            continue
        if err:
            print("[load] ERROR: bq query failed", file=sys.stderr)
            print(err, file=sys.stderr)
            write_outputs({
                "candidate_rows": candidate_rows,
                "duplicate_rows": 0,
                "inserted_rows": 0,
                "load_skipped": "true",
                "dedupe_dates": dedupe_dates,
                "stop_reason": "bq_query_failed",
            })
            return 1
        break

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

    # 1 load job
    load_file = "/tmp/gate_kills_to_load.ndjson"
    with open(load_file, "w", encoding="utf-8") as f:
        for r in new_rows:
            f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")

    p2 = run_cmd([
        "bq", "load",
        "--quiet",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--ignore_unknown_values=true",
        target_cli,
        load_file
    ])

    if p2.returncode != 0:
        print("[load] ERROR: bq load failed", file=sys.stderr)
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
