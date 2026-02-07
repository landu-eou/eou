#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def excel_serial_from_epoch(epoch: int) -> float:
    # Google Sheets usa serial tipo Excel (epoch/86400 + 25569) en UTC
    return (epoch / 86400.0) + 25569.0


def epoch_now_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def epoch_from_iso(iso: str) -> int:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp())


def post_json(url: str, token: str, payload: Dict[str, Any]) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        _ = resp.read()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--sheets-id", required=True)
    p.add_argument("--tab", required=True)
    p.add_argument("--row", required=True)
    p.add_argument("--status", default="")
    p.add_argument("--next-run-seconds", default="", help="segundos desde ahora; opcional")
    p.add_argument("--last-modified-iso", default="", help="ISO 8601 (Z); opcional")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    token = os.environ.get("SHEETS_ACCESS_TOKEN", "")
    if not token:
        raise SystemExit("Missing SHEETS_ACCESS_TOKEN")

    row = args.row
    tab = args.tab
    data_updates: List[Dict[str, Any]] = []

    if args.status:
        data_updates.append(
            {"range": f"{tab}!B{row}:B{row}", "values": [[args.status]]}
        )

    if args.next_run_seconds:
        delta = int(args.next_run_seconds)
        ep = epoch_now_utc() + delta
        serial = float(f"{excel_serial_from_epoch(ep):.10f}")
        data_updates.append(
            {"range": f"{tab}!D{row}:D{row}", "values": [[serial]]}
        )

    if args.last_modified_iso:
        ep = epoch_from_iso(args.last_modified_iso)
        serial = float(f"{excel_serial_from_epoch(ep):.10f}")
        data_updates.append(
            {"range": f"{tab}!I{row}:I{row}", "values": [[serial]]}
        )

    if not data_updates:
        return 0

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{args.sheets_id}/values:batchUpdate"
    payload = {
        "valueInputOption": "RAW",
        "data": data_updates,
    }
    post_json(url, token, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
