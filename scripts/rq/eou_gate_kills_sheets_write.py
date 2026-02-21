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


# === Sheets scopes (Google) ===
# Usamos el scope de Sheets para escritura. (cloud-platform no es suficiente para Sheets)
SHEETS_SCOPES = "https://www.googleapis.com/auth/spreadsheets"


def excel_serial_from_epoch(epoch: int) -> float:
    # Google Sheets usa serial tipo Excel (epoch/86400 + 25569) en UTC
    return (epoch / 86400.0) + 25569.0


def epoch_now_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def epoch_from_iso(iso: str) -> int:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp())


def _gcloud_refresh_access_token() -> str:
    """
    Refresca el access token desde Application Default Credentials (ADC).
    En GitHub Actions, ADC queda configurado por google-github-actions/auth (vía cred file / env).
    Usamos gcloud para pedir un token NUEVO con scopes correctos (Sheets).
    """
    cmd = [
        "gcloud",
        "auth",
        "application-default",
        "print-access-token",
        f"--scopes={SHEETS_SCOPES}",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        # stderr mínimo: dejamos que el caller decida
        return ""
    return (p.stdout or "").strip()


def _post_json(url: str, token: str, payload: Dict[str, Any]) -> None:
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

    # Token inicial (rápido). Si caduca durante workflows largos, lo refrescamos con gcloud.
    token = os.environ.get("SHEETS_ACCESS_TOKEN", "").strip()
    if not token:
        # Si no viene, intentamos obtenerlo vía ADC directamente.
        token = _gcloud_refresh_access_token().strip()
        if not token:
            raise SystemExit("Missing SHEETS_ACCESS_TOKEN and unable to refresh via gcloud ADC")

    row = args.row
    tab = args.tab
    data_updates: List[Dict[str, Any]] = []

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
    payload = {"valueInputOption": "RAW", "data": data_updates}

    # Retry policy:
    # - 401/403 => asumimos token inválido/caducado/scope insuficiente.
    # - sleep 5000ms
    # - refrescar token
    # - retry hasta 10
    max_retries = 10
    for attempt in range(0, max_retries + 1):
        try:
            _post_json(url, token, payload)
            return 0
        except urllib.error.HTTPError as e:
            code = e.code
            # 401: token inválido/caducado
            # 403: a menudo scopes insuficientes o credencial no autorizada (también puede ser sharing)
            if code in (401, 403) and attempt < max_retries:
                time.sleep(5.0)
                new_tok = _gcloud_refresh_access_token().strip()
                if new_tok:
                    token = new_tok
                continue
            # error final: mínimo
            raise SystemExit(f"Sheets API HTTPError: {code}")
        except Exception:
            # No reintentamos “a ciegas” errores no-HTTP: fallo duro con ruido mínimo
            raise SystemExit("Sheets API error")

    raise SystemExit("Sheets API error: retries exceeded")


if __name__ == "__main__":
    raise SystemExit(main())
