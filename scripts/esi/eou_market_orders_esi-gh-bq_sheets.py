#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

# Sheets:
# - status: column B
# - next_run: column D (as numeric serial UTC)
# - last_modified: column I (as numeric serial UTC)
#
# Uses GOOGLE_SHEETS_ACCESS_TOKEN (bearer), already provided by your infra.

SHEETS_ID = os.environ["SHEETS_ID"]
SHEET_TAB = os.environ["SHEET_TAB"]
ROW = int(os.environ["SHEETS_WORKFLOW_ROW"])
LOCK_TIME = int(os.environ.get("LOCK_TIME", "180"))

TOKEN = os.environ.get("GOOGLE_SHEETS_ACCESS_TOKEN", "").strip()
if not TOKEN:
    print("Missing GOOGLE_SHEETS_ACCESS_TOKEN", file=sys.stderr)
    sys.exit(1)

API = "https://sheets.googleapis.com/v4/spreadsheets"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_sheets_serial(dt: datetime) -> float:
    # Google Sheets serial date: days since 1899-12-30
    epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
    delta = dt.astimezone(timezone.utc) - epoch
    return delta.total_seconds() / 86400.0


def a1_for_row(col_letter: str) -> str:
    return f"{SHEET_TAB}!{col_letter}{ROW}"


def patch_values(ranges_to_values: dict[str, list[list[object]]]) -> None:
    url = f"{API}/{SHEETS_ID}/values:batchUpdate"
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": rng, "values": vals} for rng, vals in ranges_to_values.items()],
    }
    data = json.dumps(body).encode("utf-8")
    req = Request(
        url,
        method="POST",
        data=data,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    with urlopen(req, timeout=60) as resp:
        _ = resp.read()


def cmd_init() -> None:
    now = utc_now()
    next_run = now + timedelta(seconds=LOCK_TIME)

    patch_values(
        {
            a1_for_row("B"): [["in progress"]],
            a1_for_row("D"): [[dt_to_sheets_serial(next_run)]],
        }
    )
    print("sheets:init ok")


def cmd_finalize() -> None:
    now = utc_now()

    max_expires_epoch = int(os.environ.get("MAX_EXPIRES_EPOCH", "0") or "0")
    max_last_modified_epoch = int(os.environ.get("MAX_LAST_MODIFIED_EPOCH", "0") or "0")

    if max_expires_epoch <= 0:
        # Fallback: now+5m
        next_run_dt = now + timedelta(minutes=5)
    else:
        next_run_dt = datetime.fromtimestamp(max_expires_epoch, tz=timezone.utc) + timedelta(minutes=5)

    ranges = {
        a1_for_row("B"): [["completed"]],
        a1_for_row("D"): [[dt_to_sheets_serial(next_run_dt)]],
    }

    if max_last_modified_epoch > 0:
        lm = datetime.fromtimestamp(max_last_modified_epoch, tz=timezone.utc)
        ranges[a1_for_row("I")] = [[dt_to_sheets_serial(lm)]]

    patch_values(ranges)
    print("sheets:finalize ok")


def cmd_fail() -> None:
    now = utc_now()
    next_run = now + timedelta(minutes=5)
    patch_values(
        {
            a1_for_row("B"): [["failed"]],
            a1_for_row("D"): [[dt_to_sheets_serial(next_run)]],
        }
    )
    print("sheets:fail ok")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: sheets.py (init|finalize|fail)", file=sys.stderr)
        return 2
    cmd = sys.argv[1].strip()
    if cmd == "init":
        cmd_init()
    elif cmd == "finalize":
        cmd_finalize()
    elif cmd == "fail":
        cmd_fail()
    else:
        print("Unknown command", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
