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


SCHEMA_JSON = [
    {"name": "snapshot_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "killmailID", "type": "INT64", "mode": "REQUIRED"},
    {"name": "stargate", "type": "STRING", "mode": "REQUIRED"},
    {"name": "stargateGroup", "type": "STRING", "mode": "REQUIRED"},
    {"name": "solarSystem", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ship_class", "type": "STRING", "mode": "REQUIRED"},
    {"name": "smartBomb", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "attackers", "type": "INT64", "mode": "REQUIRED"},
    {
        "name": "corporationID",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [{"name": "corporation_id", "type": "INT64", "mode": "REQUIRED"}],
    },
]


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def run_cmd(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def extract_json(stdout: str) -> Any:
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


def date_of_snapshot(snapshot_ts: str) -> str:
    return snapshot_ts[:10] if snapshot_ts else ""


def ensure_dataset(project: str, dataset: str) -> bool:
    p = run_cmd(["bq", "show", "--format=prettyjson", f"{project}:{dataset}"])
    if p.returncode == 0:
        return True
    p2 = run_cmd(["bq", "mk", "--dataset", f"{project}:{dataset}"])
    return p2.returncode == 0


def ensure_table(project: str, dataset: str, table: str) -> bool:
    fq = f"{project}:{dataset}.{table}"
    p = run_cmd(["bq", "show", "--format=prettyjson", fq])
    if p.returncode == 0:
        return True

    schema_path = "/tmp/gate_kills_schema.json"
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(SCHEMA_JSON, f, ensure_ascii=False)

    p2 = run_cmd(
        [
            "bq",
            "mk",
            "--table",
            "--time_partitioning_field=snapshot_ts",
            "--time_partitioning_type=DAY",
            "--clustering_fields=stargate,solarSystem,ship_class",
            f"--schema={schema_path}",
            fq,
        ]
    )
    if p2.returncode != 0:
        print("[load] ERROR: bq mk table failed", file=sys.stderr)
        print(p2.stdout, file=sys.stderr)
        print(p2.stderr, file=sys.stderr)
        return False
    return True


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
        "bq",
        "query",
        "--quiet",
        "--use_legacy_sql=false",
        "--format=json",
        "--project_id",
        project,
        sql,
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

    if not os.path.exists(args.batch) or os.path.getsize(args.batch) == 0:
        write_outputs(
            {
                "candidate_rows": 0,
                "duplicate_rows": 0,
                "inserted_rows": 0,
                "load_skipped": "true",
                "dedupe_dates": "",
                "stop_reason": "completed",
            }
        )
        return 0

    rows: List[dict] = []
    ids: List[int] = []
    dates_set: Set[str] = set()

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
        write_outputs(
            {
                "candidate_rows": 0,
                "duplicate_rows": 0,
                "inserted_rows": 0,
                "load_skipped": "true",
                "dedupe_dates": "",
                "stop_reason": "completed",
            }
        )
        return 0

    # asegurar BQ solo si hay filas
    if not ensure_dataset(args.project, args.dataset) or not ensure_table(args.project, args.dataset, args.table):
        write_outputs(
            {
                "candidate_rows": candidate_rows,
                "duplicate_rows": 0,
                "inserted_rows": 0,
                "load_skipped": "true",
                "dedupe_dates": "",
                "stop_reason": "bq_ensure_failed",
            }
        )
        return 1

    ids_unique = sorted(set(ids))
    dates_sorted = sorted(dates_set)
    dedupe_dates = ",".join(dates_sorted)

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
            write_outputs(
                {
                    "candidate_rows": candidate_rows,
                    "duplicate_rows": 0,
                    "inserted_rows": 0,
                    "load_skipped": "true",
                    "dedupe_dates": dedupe_dates,
                    "stop_reason": "bq_query_failed",
                }
            )
            return 1
        break

    new_rows = [r for r in rows if r["killmailID"] not in existing]
    duplicate_rows = candidate_rows - len(new_rows)

    if not new_rows:
        write_outputs(
            {
                "candidate_rows": candidate_rows,
                "duplicate_rows": duplicate_rows,
                "inserted_rows": 0,
                "load_skipped": "true",
                "dedupe_dates": dedupe_dates,
                "stop_reason": "completed",
            }
        )
        return 0

    load_file = "/tmp/gate_kills_to_load.ndjson"
    with open(load_file, "w", encoding="utf-8") as f:
        for r in new_rows:
            f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")

    p2 = run_cmd(
        [
            "bq",
            "load",
            "--quiet",
            "--source_format=NEWLINE_DELIMITED_JSON",
            "--ignore_unknown_values=true",
            target_cli,
            load_file,
        ]
    )

    if p2.returncode != 0:
        print("::warning:: bq load failed", file=sys.stderr)
        print(p2.stdout, file=sys.stderr)
        print(p2.stderr, file=sys.stderr)
        write_outputs(
            {
                "candidate_rows": candidate_rows,
                "duplicate_rows": duplicate_rows,
                "inserted_rows": 0,
                "load_skipped": "false",
                "dedupe_dates": dedupe_dates,
                "stop_reason": "bq_load_failed",
            }
        )
        return 1

    inserted_rows = len(new_rows)
    write_outputs(
        {
            "candidate_rows": candidate_rows,
            "duplicate_rows": duplicate_rows,
            "inserted_rows": inserted_rows,
            "load_skipped": "false",
            "dedupe_dates": dedupe_dates,
            "stop_reason": "completed",
        }
    )
    print(f"[load] inserted_rows={inserted_rows} duplicates={duplicate_rows} dates={dedupe_dates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
