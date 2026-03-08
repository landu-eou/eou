#!/usr/bin/env python3
"""
Escritura del estado del workflow en Google Sheets.

Escrituras:
- B{row} = status
- D{row} = next_run como serial de Google Sheets
- I{row} = last_modified como serial, solo cuando write_last_modified=true

Autenticación:
- usa Application Default Credentials disponibles tras google-github-actions/auth
- no lee tokens de ESI ni otra información sensible
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import google.auth
from googleapiclient.discovery import build


def parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def utc_to_sheets_serial(value: Optional[str]) -> Optional[float]:
    dt = parse_iso_utc(value)
    if dt is None:
        return None
    epoch_seconds = dt.timestamp()
    return epoch_seconds / 86400.0 + 25569.0


def write_workflow_status(
    *,
    sheets_id: str,
    sheet_tab: str,
    row: str,
    status: str,
    next_run_iso: Optional[str],
    last_modified_iso: Optional[str],
    write_last_modified: bool,
) -> None:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    data = [
        {
            "range": f"{sheet_tab}!B{row}",
            "values": [[status]],
        },
        {
            "range": f"{sheet_tab}!D{row}",
            "values": [[utc_to_sheets_serial(next_run_iso) if next_run_iso else ""]],
        },
    ]

    if write_last_modified:
        data.append({
            "range": f"{sheet_tab}!I{row}",
            "values": [[utc_to_sheets_serial(last_modified_iso) if last_modified_iso else ""]],
        })

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheets_id,
        body={
            "valueInputOption": "RAW",
            "data": data,
        },
    ).execute()


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("from-files")
    p.add_argument("--sheets-id", required=True)
    p.add_argument("--sheet-tab", required=True)
    p.add_argument("--row", required=True)
    p.add_argument("--tuning-path", required=True)
    p.add_argument("--run-metrics-path", required=True)
    p.add_argument("--write-last-modified", required=True)

    args = parser.parse_args()

    if args.cmd == "from-files":
        with open(args.tuning_path, "r", encoding="utf-8") as f:
            tuning = json.load(f)

        metrics = {}
        p_metrics = Path(args.run_metrics_path)
        if p_metrics.exists():
            with p_metrics.open("r", encoding="utf-8") as f:
                metrics = json.load(f)

        status = str(tuning.get("status", "unknown"))
        next_run = tuning.get("next_run")
        last_modified = metrics.get("maxLastModified")
        write_last_modified = str(args.write_last_modified).strip().lower() == "true"

        write_workflow_status(
            sheets_id=args.sheets_id,
            sheet_tab=args.sheet_tab,
            row=args.row,
            status=status,
            next_run_iso=next_run,
            last_modified_iso=last_modified,
            write_last_modified=write_last_modified,
        )
        return 0

    raise RuntimeError("Unsupported command")


if __name__ == "__main__":
    raise SystemExit(main())
