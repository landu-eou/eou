#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests


SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"

def must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing {name}")
    return v

def opt_env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def log(msg: str) -> None:
    print(msg, flush=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_sheets_serial(dt: datetime) -> float:
    """
    Google Sheets serial date:
    days since 1899-12-30 (Sheets / Excel compatible in practice for modern dates).
    """
    epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
    delta = dt - epoch
    return delta.total_seconds() / 86400.0

def sheets_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

def get_sheet_id(spreadsheet_id: str, token: str, sheet_name: str) -> int:
    url = f"{SHEETS_BASE}/{spreadsheet_id}?fields=sheets(properties(sheetId,title))"
    r = requests.get(url, headers=sheets_headers(token), timeout=30)
    r.raise_for_status()
    data = r.json()
    for sh in data.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == sheet_name:
            return int(props.get("sheetId"))
    raise RuntimeError(f"Sheet tab not found: {sheet_name}")

def update_cells(
    spreadsheet_id: str,
    token: str,
    range_a1: str,
    values: list,
) -> None:
    url = f"{SHEETS_BASE}/{spreadsheet_id}/values/{range_a1}?valueInputOption=USER_ENTERED"
    body = {"range": range_a1, "majorDimension": "ROWS", "values": values}
    r = requests.put(url, headers=sheets_headers(token), data=json.dumps(body), timeout=30)
    r.raise_for_status()

def main() -> int:
    if len(sys.argv) < 2:
        raise RuntimeError("Usage: sheets.py <init|finalize|fail>")

    mode = sys.argv[1].strip().lower()

    token = os.environ.get("GOOGLE_SHEETS_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("Missing GOOGLE_SHEETS_ACCESS_TOKEN")

    spreadsheet_id = must_env("SHEETS_ID")
    tab = must_env("SHEET_TAB")
    row_str = must_env("SHEETS_WORKFLOW_ROW")
    row = int(row_str)

    lock_time = int(must_env("LOCK_TIME"))

    # Columns:
    # B: status
    # D: next_run
    # I: last_modified
    col_status = "B"
    col_next_run = "D"
    col_last_mod = "I"

    if mode == "init":
        now = utc_now()
        next_run = now + timedelta(seconds=lock_time)
        # write status + next_run
        update_cells(spreadsheet_id, token, f"{tab}!{col_status}{row}", [["in progress"]])
        update_cells(spreadsheet_id, token, f"{tab}!{col_next_run}{row}", [[to_sheets_serial(next_run)]])
        log("sheets_init_done")
        return 0

    if mode == "finalize":
        # inputs from workflow outputs
        max_expires_epoch = int(opt_env("MAX_EXPIRES_EPOCH", "0"))
        max_last_modified_epoch = int(opt_env("MAX_LAST_MODIFIED_EPOCH", "0"))

        now = utc_now()
        if max_expires_epoch > 0:
            expires_dt = datetime.fromtimestamp(max_expires_epoch, tz=timezone.utc)
            # G) +5 seconds (not +5 minutes)
            next_run = expires_dt + timedelta(seconds=5)
        else:
            # fallback conservative
            next_run = now + timedelta(minutes=5)

        update_cells(spreadsheet_id, token, f"{tab}!{col_status}{row}", [["completed"]])
        update_cells(spreadsheet_id, token, f"{tab}!{col_next_run}{row}", [[to_sheets_serial(next_run)]])

        # last_modified only if not failed
        if max_last_modified_epoch > 0:
            lm_dt = datetime.fromtimestamp(max_last_modified_epoch, tz=timezone.utc)
            update_cells(spreadsheet_id, token, f"{tab}!{col_last_mod}{row}", [[to_sheets_serial(lm_dt)]])

        log("sheets_finalize_done")
        return 0

    if mode == "fail":
        now = utc_now()
        next_run = now + timedelta(minutes=5)
        update_cells(spreadsheet_id, token, f"{tab}!{col_status}{row}", [["failed"]])
        update_cells(spreadsheet_id, token, f"{tab}!{col_next_run}{row}", [[to_sheets_serial(next_run)]])
        log("sheets_fail_done")
        return 0

    raise RuntimeError(f"Unknown mode: {mode}")

if __name__ == "__main__":
    sys.exit(main())
