#!/usr/bin/env python3
"""
ESI enricher (Phase 1)

- Lee candidatos desde BigQuery:
  - first_seen recientes (últimos 7 días)
  - pvp label presente
  - no npc, no awox
  - no SUCCESS/DISCARD previo
  - <= 10 FAIL
- Pide el killmail a ESI (latest) usando killmail_id + killmail_hash.
- Aplica descartes básicos (sin atacantes jugadores, autodestrucción probable).
- Emite:
  - out/ea.ndjson  -> zkb_enrichment_attempts
  - out/ekf.ndjson -> zkb_esi_killmail_facts
  - out/partitions.txt -> fechas request_date tocadas (para refrescar particiones en tabla final)

Robusto:
- `bq query --format=json` puede venir “ensuciado” por warnings/progress -> parsing defensivo.
- Si no hay candidatos, sale limpio y crea ficheros vacíos.
"""

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

ESI_BASE = "https://esi.evetech.net/latest"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def bq_query_json(project: str, sql: str) -> List[Dict[str, Any]]:
    """
    Ejecuta `bq query --format=json` y devuelve lista[dict].
    Robusto contra texto basura/warnings en stdout.
    """
    cmd = [
        "bq",
        f"--project_id={project}",
        "query",
        "--use_legacy_sql=false",
        "--format=json",
        "--quiet",
        sql,
    ]
    p = subprocess.run(cmd, text=True, capture_output=True)

    if p.returncode != 0:
        raise RuntimeError(
            f"bq query failed (code {p.returncode})\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )

    out = (p.stdout or "").strip()
    if not out:
        return []

    # El JSON del CLI es un array que empieza por '['. Si hay texto antes, lo saltamos.
    i = out.find("[")
    if i == -1:
        return []

    out_json = out[i:]
    try:
        data = json.loads(out_json)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def pick_cat(labels: List[str]) -> Optional[int]:
    """
    Extrae el número de la etiqueta 'cat:X' en labels de RedisQ.
    Devuelve int(X) o None si no existe / no es parseable.
    """
    for l in labels or []:
        if isinstance(l, str) and l.startswith("cat:"):
            try:
                return int(l.split(":", 1)[1])
            except Exception:
                return None
    return None


def esi_get(session: requests.Session, url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", 20)
    return session.get(url, **kw)


def esi_get_killmail(session: requests.Session, killmail_id: int, killmail_hash: str) -> requests.Response:
    url = f"{ESI_BASE}/killmails/{killmail_id}/{killmail_hash}/?datasource=tranquility"
    return esi_get(session, url)


def is_stargate(location_id: Optional[int]) -> bool:
    return location_id is not None and 50000000 <= location_id <= 59999999


def compute_attacker_corp_ids(km: Dict[str, Any]) -> Tuple[int, List[int]]:
    """
    Devuelve:
      - attackers_count total
      - lista deduplicada de corp_id relevantes (final blow, top dmg, corp más atacantes, corp >=25%)
    Nota: en fase 1 no resolvemos nombres; aquí solo devolvemos ids para futuro.
    """
    attackers = km.get("attackers", []) or []
    total = len(attackers)
    if total == 0:
        return 0, []

    def corp_id(a: Dict[str, Any]) -> Optional[int]:
        v = a.get("corporation_id")
        return int(v) if v is not None else None

    fb = next((a for a in attackers if a.get("final_blow") is True), None)
    fb_c = corp_id(fb) if fb else None

    top = max(attackers, key=lambda a: int(a.get("damage_done", 0) or 0))
    top_c = corp_id(top)

    counts: Dict[int, int] = {}
    for a in attackers:
        c = corp_id(a)
        if c is None:
            continue
        counts[c] = counts.get(c, 0) + 1

    corp_most = max(counts.items(), key=lambda kv: kv[1])[0] if counts else None
    corp_ge_25 = [c for c, n in counts.items() if n >= max(1, int(0.25 * total))]

    ids: List[int] = []
    for c in [fb_c, top_c, corp_most]:
        if c is not None:
            ids.append(c)
    ids.extend(corp_ge_25)

    seen = set()
    out: List[int] = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return total, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--max-enrich", type=int, default=80)
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Selección de candidatos:
    # - últimos 7 días
    # - pendientes (sin SUCCESS/DISCARD)
    # - <=10 FAIL
    # - pvp label
    # - no npc, no awox
    sql = f"""
    WITH att AS (
      SELECT
        killmail_id,
        SUM(IF(result='FAIL', 1, 0)) AS fail_count,
        MAX(IF(result='SUCCESS', 1, 0)) AS has_success,
        MAX(IF(result='DISCARD', 1, 0)) AS has_discard
      FROM `{args.project}.{args.dataset}.zkb_enrichment_attempts`
      GROUP BY killmail_id
    )
    SELECT
      fs.killmail_id,
      fs.killmail_hash,
      fs.location_id,
      fs.labels,
      fs.first_request_ts,
      fs.request_date,
      COALESCE(att.fail_count,0) AS fail_count
    FROM `{args.project}.{args.dataset}.zkb_kills_first_seen` fs
    LEFT JOIN att USING (killmail_id)
    WHERE fs.request_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND COALESCE(att.has_success,0)=0
      AND COALESCE(att.has_discard,0)=0
      AND COALESCE(att.fail_count,0) <= 10
      AND COALESCE(fs.zkb_npc,false)=false
      AND COALESCE(fs.zkb_awox,false)=false
      AND EXISTS (SELECT 1 FROM UNNEST(fs.labels) l WHERE l='pvp')
    ORDER BY fs.first_request_ts DESC
    LIMIT {int(args.max_enrich)}
    """

    candidates = bq_query_json(args.project, sql)

    # Si no hay candidatos, salida limpia y ficheros vacíos para que el workflow continúe.
    if not candidates:
        open(os.path.join(args.out_dir, "partitions.txt"), "w", encoding="utf-8").close()
        open(os.path.join(args.out_dir, "ea.ndjson"), "w", encoding="utf-8").close()
        open(os.path.join(args.out_dir, "ekf.ndjson"), "w", encoding="utf-8").close()
        return

    sess = requests.Session()
    sess.headers.update({"User-Agent": args.user_agent, "Accept": "application/json"})

    ea_rows: List[Dict[str, Any]] = []
    ekf_rows: List[Dict[str, Any]] = []
    touched_dates = set()

    def error_remain(resp: requests.Response) -> Optional[int]:
        h = resp.headers.get("X-ESI-Error-Limit-Remain")
        try:
            return int(h) if h is not None else None
        except Exception:
            return None

    for c in candidates:
        km_id = int(c["killmail_id"])
        km_hash = str(c["killmail_hash"])
        location_id = int(c["location_id"]) if c.get("location_id") is not None else None
        labels = list(c.get("labels") or [])
        touched_dates.add(str(c["request_date"]))

        resp = esi_get_killmail(sess, km_id, km_hash)

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            ea_rows.append(
                {
                    "attempt_ts": utc_now_iso(),
                    "killmail_id": km_id,
                    "result": "FAIL",
                    "stage": "killmail",
                    "reason": "RATE_LIMIT",
                    "http_status": 429,
                    "retry_after_sec": int(ra) if ra and ra.isdigit() else None,
                    "error_limit_remain": error_remain(resp),
                }
            )
            break

        if resp.status_code == 404:
            ea_rows.append(
                {
                    "attempt_ts": utc_now_iso(),
                    "killmail_id": km_id,
                    "result": "FAIL",
                    "stage": "killmail",
                    "reason": "NOT_READY",
                    "http_status": 404,
                    "retry_after_sec": None,
                    "error_limit_remain": error_remain(resp),
                }
            )
            continue

        if resp.status_code != 200:
            ea_rows.append(
                {
                    "attempt_ts": utc_now_iso(),
                    "killmail_id": km_id,
                    "result": "FAIL",
                    "stage": "killmail",
                    "reason": f"HTTP_{resp.status_code}",
                    "http_status": resp.status_code,
                    "retry_after_sec": None,
                    "error_limit_remain": error_remain(resp),
                }
            )
            rem = error_remain(resp)
            if rem is not None and rem <= 5:
                break
            continue

        km = resp.json()
        attackers_count, _corp_ids = compute_attacker_corp_ids(km)

        # Autodestrucción (heurística conservadora):
        # - si hay 1 atacante jugador y coincide con la víctima -> autodestruct probable
        victim = km.get("victim", {}) or {}
        victim_char = victim.get("character_id")
        attackers = km.get("attackers", []) or []
        player_attackers = [a for a in attackers if a.get("character_id") is not None]

        if len(player_attackers) == 0:
            ea_rows.append(
                {
                    "attempt_ts": utc_now_iso(),
                    "killmail_id": km_id,
                    "result": "DISCARD",
                    "stage": "filter",
                    "reason": "NO_PLAYER_ATTACKERS",
                    "http_status": 200,
                    "retry_after_sec": None,
                    "error_limit_remain": error_remain(resp),
                }
            )
            continue

        if len(player_attackers) == 1 and victim_char is not None:
            try:
                if int(player_attackers[0].get("character_id")) == int(victim_char):
                    ea_rows.append(
                        {
                            "attempt_ts": utc_now_iso(),
                            "killmail_id": km_id,
                            "result": "DISCARD",
                            "stage": "filter",
                            "reason": "SELF_DESTRUCT_LIKELY",
                            "http_status": 200,
                            "retry_after_sec": None,
                            "error_limit_remain": error_remain(resp),
                        }
                    )
                    continue
            except Exception:
                pass

        # Clasificación fase 1 (con labels cat:X de RedisQ)
        cat = pick_cat(labels)
        victim_bucket: Optional[str] = None
        is_freighter: Optional[bool] = None
        is_capsule: Optional[bool] = None

        # Nota: capsule requiere ship_type_id y lookup -> se deja None en fase 1
        if cat == 6:
            victim_bucket = "freighter"
            is_freighter = True
            is_capsule = False
        elif cat == 8:
            victim_bucket = "structure"
            is_freighter = False
            is_capsule = False
        elif cat is not None and cat <= 5:
            victim_bucket = "subcapital"
            is_freighter = False
            is_capsule = False
        elif cat is not None and 6 <= cat <= 7:
            victim_bucket = "capital"
            is_freighter = False
            is_capsule = False

        ekf_rows.append(
            {
                "fetch_ts": utc_now_iso(),
                "killmail_id": km_id,
                "killmail_time": km.get("killmail_time"),
                "solar_system_name": None,
                "stargate_route": None if not is_stargate(location_id) else None,
                "victim_ship_bucket": victim_bucket,
                "is_freighter": is_freighter,
                "is_capsule": is_capsule,
                "attackers_count": attackers_count,
                "attacker_corp_names": [],
                "smartbomb_damage": False,
                "war_related": True if int(km.get("war_id") or 0) > 0 else False,
            }
        )

        ea_rows.append(
            {
                "attempt_ts": utc_now_iso(),
                "killmail_id": km_id,
                "result": "SUCCESS",
                "stage": "killmail",
                "reason": None,
                "http_status": 200,
                "retry_after_sec": None,
                "error_limit_remain": error_remain(resp),
            }
        )

        rem = error_remain(resp)
        if rem is not None and rem <= 5:
            break

    ea_path = os.path.join(args.out_dir, "ea.ndjson")
    ekf_path = os.path.join(args.out_dir, "ekf.ndjson")

    with open(ea_path, "w", encoding="utf-8") as f:
        for r in ea_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with open(ekf_path, "w", encoding="utf-8") as f:
        for r in ekf_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with open(os.path.join(args.out_dir, "partitions.txt"), "w", encoding="utf-8") as f:
        for d in sorted(touched_dates):
            f.write(d + "\n")


if __name__ == "__main__":
    main()
