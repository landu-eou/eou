#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List


# =========================
# Utilidades de tiempo
# =========================
def excel_serial_from_epoch(epoch: int) -> float:
    # Google Sheets usa serial tipo Excel (epoch/86400 + 25569) en UTC
    return (epoch / 86400.0) + 25569.0


def epoch_now_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def epoch_from_iso(iso: str) -> int:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp())


# =========================
# Token refresh (gcloud)
# =========================
def refresh_access_token_with_gcloud() -> str:
    """
    Devuelve un access token nuevo usando las credenciales activas de gcloud.
    Requiere que setup-gcloud esté instalado y que auth@v2 ya haya configurado credenciales.
    """
    p = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        # stderr mínimo (no “bonito”)
        raise RuntimeError("gcloud auth print-access-token failed")
    token = (p.stdout or "").strip()
    if not token:
        raise RuntimeError("empty access token from gcloud")
    return token


# =========================
# HTTP Sheets write
# =========================
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


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--sheets-id", required=True)
    p.add_argument("--tab", required=True)
    p.add_argument("--row", required=True)
    p.add_argument("--status", default="")
    p.add_argument("--next-run-seconds", default="", help="segundos desde ahora; opcional")
    p.add_argument("--last-modified-iso", default="", help="ISO 8601 (Z); opcional")
    # Política anti-bucle: máximo 10 retries en caso de 401
    p.add_argument("--max-token-retries", type=int, default=10)
    p.add_argument("--token-retry-sleep-ms", type=int, default=5000)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Token inicial: el que inyecta el workflow (salida de auth@v2).
    token = os.environ.get("SHEETS_ACCESS_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing SHEETS_ACCESS_TOKEN")

    row = args.row
    tab = args.tab
    data_updates: List[Dict[str, Any]] = []

    # =========================
    # Construcción del batchUpdate (misma lógica)
    # =========================
    if args.status:
        data_updates.append({"range": f"{tab}!B{row}:B{row}", "values": [[args.status]]})

    if args.next_run_seconds:
        delta = int(args.next_run_seconds)
        ep = epoch_now_utc() + delta
        serial = float(f"{excel_serial_from_epoch(ep):.10f}")
        data_updates.append({"range": f"{tab}!D{row}:D{row}", "values": [[serial]]})

    if args.last_modified_iso:
        ep = epoch_from_iso(args.last_modified_iso)
        serial = float(f"{excel_serial_from_epoch(ep):.10f}")
        data_updates.append({"range": f"{tab}!I{row}:I{row}", "values": [[serial]]})

    if not data_updates:
        return 0

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{args.sheets_id}/values:batchUpdate"
    payload = {
        "valueInputOption": "RAW",
        "data": data_updates,
    }

    # =========================
    # Retry policy: solo para 401 (token inválido/caducado)
    # 401 -> sleep 5000ms -> refresh token -> retry
    # max 10 retries, si no: exit 1 (intervención humana)
    # =========================
    sleep_s = max(0.0, args.token_retry_sleep_ms / 1000.0)

    attempts_left = max(0, args.max_token_retries)
    while True:
        try:
            post_json(url, token, payload)
            return 0
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempts_left > 0:
                time.sleep(sleep_s)
                token = refresh_access_token_with_gcloud()
                attempts_left -= 1
                continue
            # error mínimo a stderr y falla
            raise SystemExit(f"Sheets API HTTPError: {e.code}") from e
        except Exception as e:
            # No hacemos retries aquí: tu política pedía retry por token inválido.
            raise SystemExit("Sheets write failed") from e


if __name__ == "__main__":
    raise SystemExit(main())
