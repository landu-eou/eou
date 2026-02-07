#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# Import mapping
sys.path.append(os.path.dirname(__file__))
from eou_gate_kills_ship_class import ship_class_from  # noqa: E402


ESI_BASE = "https://esi.evetech.net/v1/killmails/{killmail_id}/{killmail_hash}/"


def _ua(repo: str) -> str:
    return f"EOU-RQ-Gate-Kills/1.0 (+{repo}; GitHub Actions)"


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def http_get_json(url: str, timeout: int, user_agent: str) -> Tuple[int, Dict[str, str], Optional[dict]]:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    try:
        with opener.open(req, timeout=timeout) as resp:
            status = resp.getcode()
            headers = {k: v for k, v in resp.headers.items()}
            body = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body) if body else None
            except json.JSONDecodeError:
                data = None
            return status, headers, data
    except urllib.error.HTTPError as e:
        status = e.code
        headers = {k: v for k, v in e.headers.items()}
        return status, headers, None
    except Exception:
        return 0, {}, None


def parse_iso_to_epoch(iso: str) -> int:
    # "2026-02-02T12:34:56Z"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return 0


def load_stargates_map(path_gz: str) -> Dict[int, Dict[str, str]]:
    m: Dict[int, Dict[str, str]] = {}
    with gzip.open(path_gz, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            sid = obj.get("stargateID")
            if isinstance(sid, int):
                m[sid] = {
                    "stargate": obj.get("stargate", ""),
                    "stargateGroup": obj.get("stargateGroup", ""),
                    "solarSystem": obj.get("solarSystem", ""),
                }
    return m


def load_types_map(path_gz: str) -> Dict[int, Dict[str, Any]]:
    m: Dict[int, Dict[str, Any]] = {}
    with gzip.open(path_gz, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            tid = obj.get("typeID")
            if isinstance(tid, int):
                m[tid] = {
                    "group": obj.get("group"),
                    "category": obj.get("category"),
                    "is_gategank": bool(obj.get("is_gategank", False)),
                }
    return m


def is_self_destruction(km: dict) -> bool:
    victim = km.get("victim", {}) if isinstance(km.get("victim"), dict) else {}
    v_char = victim.get("character_id")
    if not isinstance(v_char, int):
        return False
    attackers = km.get("attackers", []) if isinstance(km.get("attackers"), list) else []
    for a in attackers:
        if not isinstance(a, dict):
            continue
        if a.get("final_blow") is True and a.get("character_id") == v_char:
            return True
    return False


def corporations_array(km: dict) -> list[dict]:
    attackers = km.get("attackers", []) if isinstance(km.get("attackers"), list) else []
    total = 0
    for a in attackers:
        if isinstance(a, dict):
            dd = a.get("damage_done", 0)
            if isinstance(dd, int):
                total += dd
    threshold = total * 0.25

    final_blow = None
    top_damage = None
    top_dd = -1
    corp_ids = set()

    for a in attackers:
        if not isinstance(a, dict):
            continue
        dd = a.get("damage_done", 0)
        if isinstance(dd, int) and dd > top_dd:
            top_dd = dd
            top_damage = a
        if a.get("final_blow") is True:
            final_blow = a

    def add_corp(att: Optional[dict]) -> None:
        if not att:
            return
        cid = att.get("corporation_id")
        if isinstance(cid, int) and cid > 0:
            corp_ids.add(cid)

    add_corp(final_blow)
    add_corp(top_damage)

    for a in attackers:
        if not isinstance(a, dict):
            continue
        dd = a.get("damage_done", 0)
        if isinstance(dd, int) and total > 0 and dd >= threshold:
            add_corp(a)

    return [{"corporation_id": cid} for cid in sorted(corp_ids)]


def smartbomb_flag(km: dict, types_map: Dict[int, Dict[str, Any]]) -> bool:
    attackers = km.get("attackers", []) if isinstance(km.get("attackers"), list) else []
    for a in attackers:
        if not isinstance(a, dict):
            continue
        wid = a.get("weapon_type_id")
        if isinstance(wid, int):
            info = types_map.get(wid)
            if info and bool(info.get("is_gategank")):
                return True
    return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True)
    p.add_argument("--sde-stargates", required=True)
    p.add_argument("--sde-types", required=True)
    p.add_argument("--mode", required=True, choices=["pending", "post_poll"])
    p.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "unknown/unknown"))
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--esi-error-remain-stop", type=int, default=50)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    out_path = f"/tmp/gate_kills_enrich_{args.mode}.ndjson"
    os.makedirs(os.path.dirname(args.raw), exist_ok=True)
    if not os.path.exists(args.raw):
        open(args.raw, "a", encoding="utf-8").close()

    stargates = load_stargates_map(args.sde_stargates)
    types_map = load_types_map(args.sde_types)

    # Counters
    processed = 0
    requeued = 0
    discarded = 0
    ndjson_rows = 0
    stop_reason = "completed"

    esi_requests = 0
    esi_200 = esi_429 = esi_4xx = esi_5xx = esi_304 = 0

    retry429_total = 0

    max_killmail_epoch = 0
    max_killmail_iso = ""

    requeue_objs: list[dict] = []

    # prep output file
    with open(out_path, "w", encoding="utf-8") as outf:
        pass

    def write_row(row: dict) -> None:
        nonlocal ndjson_rows, max_killmail_epoch, max_killmail_iso
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")
        ndjson_rows += 1
        iso = row.get("snapshot_ts", "")
        if isinstance(iso, str):
            ep = parse_iso_to_epoch(iso)
            if ep > max_killmail_epoch:
                max_killmail_epoch = ep
                max_killmail_iso = iso

    # Process raw file streaming
    with open(args.raw, "r", encoding="utf-8") as f:
        lines_iter = iter(f)
        for line in lines_iter:
            if not line.strip():
                continue
            processed += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                discarded += 1
                continue

            kill_id = obj.get("killID")
            zkb = obj.get("zkb") if isinstance(obj.get("zkb"), dict) else {}
            km_hash = zkb.get("hash")
            loc_id = zkb.get("locationID")
            rearm = obj.get("rearm", 0)
            if not isinstance(rearm, int):
                rearm = 0

            # sanity
            if not (isinstance(kill_id, int) and isinstance(km_hash, str) and km_hash):
                discarded += 1
                continue

            # ESI fetch with 429 retry policy
            url = ESI_BASE.format(killmail_id=kill_id, killmail_hash=km_hash)
            while True:
                esi_requests += 1
                status, headers, data = http_get_json(url, timeout=args.timeout, user_agent=_ua(args.repo))

                # Proteger por error-limit (si viene)
                remain = headers.get("X-ESI-Error-Limit-Remain")
                if remain and remain.isdigit() and int(remain) <= args.esi_error_remain_stop:
                    # requeue actual + stop subfase; resto de líneas sin tocar
                    obj["rearm"] = rearm + 1
                    requeue_objs.append(obj)
                    requeued += 1
                    stop_reason = "esi_error_limit_stop"
                    # meter el resto tal cual (sin incrementar rearm)
                    for rest in lines_iter:
                        if rest.strip():
                            try:
                                requeue_objs.append(json.loads(rest))
                                requeued += 1
                            except Exception:
                                pass
                    # salir de ambos bucles
                    lines_iter = iter(())  # type: ignore
                    break

                if status == 200 and isinstance(data, dict):
                    esi_200 += 1
                    km = data

                    # filtro2: war_id
                    war_id = km.get("war_id")
                    if war_id is not None and isinstance(war_id, int) and war_id > 0:
                        discarded += 1
                        break

                    # filtro2: self-destruction (heurística conservadora)
                    if is_self_destruction(km):
                        discarded += 1
                        break

                    # Debe ser stargate conocido (si no, descartar)
                    if not (isinstance(loc_id, int) and loc_id in stargates):
                        discarded += 1
                        break

                    sg = stargates[loc_id]

                    victim = km.get("victim", {}) if isinstance(km.get("victim"), dict) else {}
                    ship_type_id = victim.get("ship_type_id")
                    tinfo = types_map.get(ship_type_id) if isinstance(ship_type_id, int) else None
                    category = tinfo.get("category") if tinfo else None
                    group = tinfo.get("group") if tinfo else None
                    ship_class = ship_class_from(category, group)

                    attackers = km.get("attackers", []) if isinstance(km.get("attackers"), list) else []
                    row = {
                        "snapshot_ts": km.get("killmail_time"),
                        "killmailID": kill_id,
                        "stargate": sg["stargate"],
                        "stargateGroup": sg["stargateGroup"],
                        "solarSystem": sg["solarSystem"],
                        "ship_class": ship_class,
                        "smartBomb": bool(smartbomb_flag(km, types_map)),
                        "attackers": int(len(attackers)),
                        "corporationID": corporations_array(km),
                    }

                    # snapshot_ts obligatorio
                    if not isinstance(row["snapshot_ts"], str) or not row["snapshot_ts"]:
                        # si falta, descarta (evita cargar basura)
                        discarded += 1
                        break

                    write_row(row)
                    break

                if status in (400, 404, 420):
                    esi_4xx += 1
                    discarded += 1
                    break

                if status == 304:
                    esi_304 += 1
                    # requeue si rearm < 10, si no descarta
                    if rearm < 10:
                        obj["rearm"] = rearm + 1
                        requeue_objs.append(obj)
                        requeued += 1
                    else:
                        discarded += 1
                    break

                if status in (500, 502, 503, 504) or status == 0:
                    esi_5xx += 1
                    if rearm < 10:
                        obj["rearm"] = rearm + 1
                        requeue_objs.append(obj)
                        requeued += 1
                    else:
                        discarded += 1
                    break

                if status == 429:
                    esi_429 += 1
                    retry429_total += 1
                    ra = headers.get("Retry-After")
                    sleep_s = int(ra) if (ra and ra.isdigit()) else 5

                    if retry429_total < 3:
                        time.sleep(sleep_s + 2)
                        continue

                    # retrys == 3 => requeue y STOP subfase (y conserva resto)
                    obj["rearm"] = rearm + 1
                    requeue_objs.append(obj)
                    requeued += 1
                    stop_reason = "esi_429_stop"
                    for rest in lines_iter:
                        if rest.strip():
                            try:
                                requeue_objs.append(json.loads(rest))
                                requeued += 1
                            except Exception:
                                pass
                    lines_iter = iter(())  # type: ignore
                    break

                # Otros 4xx: trátalo como 4xx => descarta
                if 400 <= status < 500:
                    esi_4xx += 1
                    discarded += 1
                    break

                # fallback: requeue si se puede
                if rearm < 10:
                    obj["rearm"] = rearm + 1
                    requeue_objs.append(obj)
                    requeued += 1
                else:
                    discarded += 1
                break

            # si stop_reason cambió a stop y vaciamos el iter, salimos
            if stop_reason in ("esi_429_stop", "esi_error_limit_stop"):
                break

    # Reescritura RAW: solo requeueados
    tmp_raw = args.raw + ".tmp"
    with open(tmp_raw, "w", encoding="utf-8") as wf:
        for obj in requeue_objs:
            wf.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
    os.replace(tmp_raw, args.raw)

    write_outputs({
        "ndjson_path": out_path,
        "ndjson_rows": ndjson_rows,
        "max_killmail_time_iso": max_killmail_iso,
        "processed": processed,
        "requeued": requeued,
        "discarded": discarded,
        "stop_reason": stop_reason,
        "esi_requests": esi_requests,
        "esi_200": esi_200,
        "esi_429": esi_429,
        "esi_4xx": esi_4xx,
        "esi_5xx": esi_5xx,
        "esi_304": esi_304,
    })

    print(f"[enrich] mode={args.mode} rows={ndjson_rows} requeued={requeued} discarded={discarded} stop={stop_reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
