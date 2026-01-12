import os
import json
import time
import hashlib
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

REDISQ_BASE = "https://zkillredisq.stream/listen.php"
ESI_BASE = "https://esi.evetech.net/latest"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def run_id() -> str:
    # compacto y ordenable
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + os.urandom(4).hex()

def sh(cmd: List[str], capture: bool = False) -> str:
    if capture:
        p = subprocess.run(cmd, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return p.stdout
    subprocess.run(cmd, check=True)
    return ""

def bq_table(project: str, dataset: str, table: str) -> str:
    return f"{project}:{dataset}.{table}"

def bq_query(sql: str, project: str) -> None:
    sh(["bq", f"--project_id={project}", "query", "--use_legacy_sql=false", "--quiet", sql])

def bq_query_json(sql: str, project: str) -> Any:
    out = sh(["bq", f"--project_id={project}", "query", "--use_legacy_sql=false", "--quiet", "--format=json", sql], capture=True)
    if not out.strip():
        return []
    return json.loads(out)

def bq_load_ndjson(project: str, dataset: str, table: str, ndjson_path: str, schema_path: str, append: bool = True) -> None:
    # Batch load (no streaming). En sandbox streaming no está soportado. 
    # bq load soporta WRITE_APPEND por defecto (sin --replace) según docs de carga batch. :contentReference[oaicite:17]{index=17}
    target = bq_table(project, dataset, table)
    cmd = [
        "bq", f"--project_id={project}", "load",
        "--source_format=NEWLINE_DELIMITED_JSON",
        f"--schema={schema_path}",
    ]
    if not append:
        cmd.append("--replace")
    cmd += [target, ndjson_path]
    sh(cmd)

def bq_exists(project: str, dataset: str, table: str) -> bool:
    target = bq_table(project, dataset, table)
    try:
        sh(["bq", f"--project_id={project}", "show", "--format=none", target])
        return True
    except subprocess.CalledProcessError:
        return False

def bq_mk_table(project: str, dataset: str, table: str, schema_path: str, partition_field: Optional[str] = None, cluster_fields: Optional[List[str]] = None) -> None:
    target = bq_table(project, dataset, table)
    cmd = ["bq", f"--project_id={project}", "mk", "--table", f"--schema={schema_path}"]
    if partition_field:
        cmd.append(f"--time_partitioning_field={partition_field}")
    if cluster_fields:
        cmd.append(f"--clustering_fields={','.join(cluster_fields)}")
    cmd.append(target)
    sh(cmd)

def sha256_int(i: int) -> str:
    return hashlib.sha256(str(i).encode("utf-8")).hexdigest()

class LedgerClient:
    def __init__(self, url: str, secret: str) -> None:
        self.url = url
        self.secret = secret

    def append(self, stream: str, run_id: str, lines: List[str]) -> None:
        payload = {"secret": self.secret, "action": "append", "stream": stream, "run_id": run_id, "lines": lines}
        for attempt in range(3):
            r = requests.post(self.url, json=payload, timeout=30)
            if r.status_code == 200:
                resp = r.json()
                if resp.get("ok") is True:
                    return
            time.sleep(2 ** attempt)
        raise RuntimeError(f"Ledger append failed for stream={stream}: {r.status_code} {r.text}")

class ESI:
    def __init__(self, user_agent: str) -> None:
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": user_agent})
        # cache simple in-memory por run (ETag + data)
        self.etag: Dict[str, str] = {}
        self.cache: Dict[str, Any] = {}

    def _respect_error_limit(self, headers: Dict[str, str]) -> None:
        # ESI headers: X-ESI-Error-Limit-Remain / Reset 
        remain = headers.get("X-ESI-Error-Limit-Remain")
        reset = headers.get("X-ESI-Error-Limit-Reset")
        try:
            if remain is not None and reset is not None:
                r = int(remain)
                s = int(reset)
                if r <= 10:
                    time.sleep(min(s + 1, 30))
        except Exception:
            pass

    def get_json(self, path: str) -> Any:
        url = ESI_BASE + path
        if url in self.cache:
            return self.cache[url]

        headers = {}
        if url in self.etag:
            headers["If-None-Match"] = self.etag[url]

        r = self.s.get(url, headers=headers, timeout=30)
        self._respect_error_limit(r.headers)

        if r.status_code == 304:
            return self.cache[url]

        r.raise_for_status()

        if "ETag" in r.headers:
            self.etag[url] = r.headers["ETag"]
        data = r.json()
        self.cache[url] = data
        return data

    def killmail(self, kill_id: int, kill_hash: str) -> Any:
        # RedisQ ya no trae killmail embebido; hay que usar ESI con id/hash 
        return self.get_json(f"/killmails/{kill_id}/{kill_hash}/")

    def system_name(self, system_id: int) -> str:
        data = self.get_json(f"/universe/systems/{system_id}/")
        return data.get("name")

    def stargate(self, gate_id: int) -> Any:
        return self.get_json(f"/universe/stargates/{gate_id}/")

    def corp_name(self, corp_id: int) -> str:
        data = self.get_json(f"/corporations/{corp_id}/")
        return data.get("name")

    def type_info(self, type_id: int) -> Any:
        return self.get_json(f"/universe/types/{type_id}/")

    def group_info(self, group_id: int) -> Any:
        return self.get_json(f"/universe/groups/{group_id}/")


def redisq_listen(queue_id: str, ttw: int, max_polls: int, user_agent: str) -> List[Dict[str, Any]]:
    """
    Respeta limitaciones de RedisQ:
    - 1 request en vuelo por queueID, si no -> 429
    - 2 req/s por IP, si no -> 429
    - ttw 1..10, null package si no hay kill
    - redirect /listen.php -> /object.php
    
    """
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    out: List[Dict[str, Any]] = []
    for _ in range(max_polls):
        r = s.get(REDISQ_BASE, params={"queueID": queue_id, "ttw": str(ttw)}, allow_redirects=True, timeout=30)
        if r.status_code == 429:
            time.sleep(1.0)
            continue
        r.raise_for_status()
        j = r.json()
        pkg = j.get("package")
        if not pkg:
            break
        out.append(pkg)
        # rate-limit soft
        time.sleep(0.55)
    return out


def ensure_bq_objects(project: str, dataset: str) -> None:
    base = os.path.dirname(__file__)
    schemas = os.path.join(base, "schemas")

    # LOG tables (append-only)
    if not bq_exists(project, dataset, "redisq_raw_log"):
        bq_mk_table(project, dataset, "redisq_raw_log", os.path.join(schemas, "redisq_raw_log.json"),
                    partition_field="redisq_request_ts", cluster_fields=["killmail_id"])
    if not bq_exists(project, dataset, "enrich_attempts_log"):
        bq_mk_table(project, dataset, "enrich_attempts_log", os.path.join(schemas, "enrich_attempts_log.json"),
                    partition_field="attempt_ts", cluster_fields=["killmail_id"])
    if not bq_exists(project, dataset, "kills_enriched_log"):
        bq_mk_table(project, dataset, "kills_enriched_log", os.path.join(schemas, "kills_enriched_log.json"),
                    partition_field="killmail_time", cluster_fields=["killmail_id"])
    if not bq_exists(project, dataset, "kills_discarded_log"):
        bq_mk_table(project, dataset, "kills_discarded_log", os.path.join(schemas, "kills_discarded_log.json"),
                    partition_field="discard_ts", cluster_fields=["killmail_id"])

    # View de estado (worked/attempts) - sin DML (sandbox no soporta DML) 
    view_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.v_kills_raw_state` AS
WITH raw AS (
  SELECT
    killmail_id,
    ANY_VALUE(killmail_hash) AS killmail_hash,
    MIN(redisq_request_ts) AS first_seen_ts,
    MAX(redisq_request_ts) AS last_seen_ts,
    -- nos quedamos con el último package_json por timestamp
    (SELECT AS STRUCT package_json, href, location_id, labels, npc, awox, solo
     FROM UNNEST(ARRAY_AGG(STRUCT(package_json, href, location_id, labels, npc, awox, solo, redisq_request_ts)
                           ORDER BY redisq_request_ts DESC LIMIT 1))
    ).*
  FROM `{project}.{dataset}.redisq_raw_log`
  GROUP BY killmail_id
),
att AS (
  SELECT
    killmail_id,
    COUNT(1) AS attempts,
    MAX(attempt_ts) AS last_attempt_ts,
    (SELECT AS STRUCT ok, error
     FROM UNNEST(ARRAY_AGG(STRUCT(ok, error, attempt_ts) ORDER BY attempt_ts DESC LIMIT 1))
    ).*
  FROM `{project}.{dataset}.enrich_attempts_log`
  GROUP BY killmail_id
),
done_enriched AS (
  SELECT DISTINCT killmail_id FROM `{project}.{dataset}.kills_enriched_log`
),
done_discarded AS (
  SELECT DISTINCT killmail_id FROM `{project}.{dataset}.kills_discarded_log`
)
SELECT
  raw.first_seen_ts AS redisq_first_seen_ts,
  raw.last_seen_ts AS redisq_last_seen_ts,
  raw.killmail_id,
  raw.killmail_hash,
  raw.location_id,
  raw.labels,
  raw.npc,
  raw.awox,
  raw.solo,
  raw.href,
  raw.package_json,
  IF(raw.killmail_id IN (SELECT killmail_id FROM done_enriched)
     OR raw.killmail_id IN (SELECT killmail_id FROM done_discarded), TRUE, FALSE) AS worked,
  IFNULL(att.attempts, 0) AS attempts,
  att.last_attempt_ts,
  att.ok AS last_attempt_ok,
  att.error AS last_error
FROM raw
LEFT JOIN att USING(killmail_id)
"""
    bq_query(view_sql, project)


def choose_pending(project: str, dataset: str, limit_n: int) -> List[Dict[str, Any]]:
    sql = f"""
SELECT killmail_id, killmail_hash, href, location_id, labels, npc, awox, solo, attempts
FROM `{project}.{dataset}.v_kills_raw_state`
WHERE worked = FALSE
  AND attempts <= 10
ORDER BY redisq_first_seen_ts ASC
LIMIT {limit_n}
"""
    rows = bq_query_json(sql, project)
    return rows

def is_self_destruct(km: Dict[str, Any]) -> bool:
    """
    Heurística conservadora (evita falsos positivos):
    - si hay atacante con mismo character_id que la víctima y solo 1 atacante => self-destruct
    (Se puede refinar más adelante; por ahora evitamos descartar PvP real.)
    """
    victim = km.get("victim", {})
    v_char = victim.get("character_id")
    attackers = km.get("attackers") or []
    if not v_char or len(attackers) != 1:
        return False
    a = attackers[0]
    return a.get("character_id") == v_char

def is_pvp(km: Dict[str, Any]) -> bool:
    attackers = km.get("attackers") or []
    # PvP si hay al menos un attacker con character_id
    return any(a.get("character_id") for a in attackers)

def smartbomb_flag(km: Dict[str, Any], esi: ESI) -> bool:
    """
    Detecta smartbomb por weapon_type_id -> group name contiene 'Smartbomb'
    (cacheado en este run).
    """
    attackers = km.get("attackers") or []
    for a in attackers:
        wt = a.get("weapon_type_id")
        if not wt:
            continue
        ti = esi.type_info(int(wt))
        gid = ti.get("group_id")
        if not gid:
            continue
        gi = esi.group_info(int(gid))
        gname = (gi.get("name") or "").lower()
        if "smartbomb" in gname:
            return True
    return False

def victim_ship_class(km: Dict[str, Any], labels: List[str], esi: ESI) -> Tuple[str, bool, bool]:
    """
    Orden exacto first-match:
      1 capsule
      2 freighter (cat:6 o grupo Freighter/Jump Freighter)
      3 hauler (grupo Industrial/Transport/Blockade Runner/Deep Space Transport)
      4 subcapital (cat<=5 y no hauler)
      5 capital (cat 6..7 excepto freighter)
      6 structure (cat:8)
    """
    # labels
    lab = set(labels or [])
    if "cat:8" in lab:
        return ("structure", False, False)

    victim = km.get("victim", {})
    ship_type_id = victim.get("ship_type_id")
    is_capsule = False
    is_freighter = False
    if ship_type_id:
        ti = esi.type_info(int(ship_type_id))
        gid = ti.get("group_id")
        if gid:
            gi = esi.group_info(int(gid))
            gname = (gi.get("name") or "").lower()
            if gname == "capsule":
                is_capsule = True
            if gname in ("freighter", "jump freighter"):
                is_freighter = True

    if is_capsule:
        return ("capsule", False, True)

    if "cat:6" in lab or is_freighter:
        return ("freighter", True, False)

    # hauler: grupos típicos
    if ship_type_id:
        ti = esi.type_info(int(ship_type_id))
        gid = ti.get("group_id")
        if gid:
            gi = esi.group_info(int(gid))
            gname = (gi.get("name") or "").lower()
            if gname in ("industrial", "transport ship", "blockade runner", "deep space transport"):
                return ("hauler", False, False)

    # capital (cat:7 típicamente; aceptamos 6..7 excepto freighter)
    if any(x.startswith("cat:") for x in lab):
        for x in lab:
            if x.startswith("cat:"):
                try:
                    n = int(x.split(":")[1])
                    if 6 <= n <= 7 and ("cat:6" not in lab):
                        return ("capital", False, False)
                    if n <= 5:
                        return ("subcapital", False, False)
                except Exception:
                    pass

    return ("subcapital", False, False)

def stargate_route(location_id: Optional[int], esi: ESI) -> Optional[str]:
    """
    Si locationID BETWEEN 50000000 AND 59999999 => stargate
    """
    if not location_id:
        return None
    if not (50000000 <= int(location_id) <= 59999999):
        return None

    g1 = esi.stargate(int(location_id))
    sys1 = int(g1.get("system_id"))
    dest = g1.get("destination", {})
    g2_id = dest.get("stargate_id")
    if not g2_id:
        return None
    g2 = esi.stargate(int(g2_id))
    sys2 = int(g2.get("system_id"))

    n1 = esi.system_name(sys1)
    n2 = esi.system_name(sys2)
    return f"{n1} \u2192 {n2}"

def attacker_corp_names(km: Dict[str, Any], esi: ESI) -> List[str]:
    attackers = km.get("attackers") or []
    if not attackers:
        return []

    # corp_id -> count
    corp_counts: Dict[int, int] = {}
    for a in attackers:
        cid = a.get("corporation_id")
        if cid:
            corp_counts[int(cid)] = corp_counts.get(int(cid), 0) + 1

    # final blow corp
    final_corp = None
    topdmg_corp = None
    topdmg = -1
    for a in attackers:
        if a.get("final_blow") and a.get("corporation_id"):
            final_corp = int(a["corporation_id"])
        dmg = int(a.get("damage_done") or 0)
        if dmg > topdmg and a.get("corporation_id"):
            topdmg = dmg
            topdmg_corp = int(a["corporation_id"])

    # corp with max attackers
    max_corp = None
    if corp_counts:
        max_corp = max(corp_counts.items(), key=lambda kv: kv[1])[0]

    # corp with >=25% attackers
    threshold = max(1, int(len(attackers) * 0.25))
    quarter_corps = [cid for cid, cnt in corp_counts.items() if cnt >= threshold]

    chosen = []
    for cid in [final_corp, topdmg_corp, max_corp] + quarter_corps:
        if cid and cid not in chosen:
            chosen.append(cid)

    names = []
    for cid in chosen:
        try:
            names.append(esi.corp_name(int(cid)))
        except Exception:
            continue
    # sin duplicados
    out = []
    for n in names:
        if n and n not in out:
            out.append(n)
    return out

def main() -> None:
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "eou")
    mode = os.environ.get("MODE", "tick")

    ledger_url = os.environ.get("APS_LEDGER_URL", "")
    ledger_secret = os.environ.get("APS_LEDGER_SECRET", "")
    user_agent = os.environ.get("EVE_USER_AGENT", "EOU/0.1 (contact: unknown)")

    queue_id = os.environ.get("REDISQ_QUEUE_ID", "eou_orch_main")
    max_seconds = int(os.environ.get("MAX_SECONDS", "50"))
    max_polls = int(os.environ.get("MAX_REDISQ_POLLS", "4"))
    max_enrich = int(os.environ.get("MAX_ENRICH_PER_RUN", "10"))

    rid = run_id()
    start = time.time()

    ensure_bq_objects(project, dataset)

    ledger = None
    if ledger_url and ledger_secret:
        ledger = LedgerClient(ledger_url, ledger_secret)

    if mode == "maintenance":
        maintenance(project, dataset)
        print("maintenance done")
        return

    # --- TICK ---
    # 1) RedisQ poll (rápido)
    pkgs = redisq_listen(queue_id=queue_id, ttw=10, max_polls=max_polls, user_agent=user_agent)

    raw_rows = []
    for pkg in pkgs:
        zkb = pkg.get("zkb", {})
        kill_id = int(pkg.get("killID"))
        raw_rows.append({
            "run_id": rid,
            "redisq_request_ts": utc_now_iso(),
            "killmail_id": kill_id,
            "killmail_hash": zkb.get("hash"),
            "location_id": zkb.get("locationID"),
            "labels": zkb.get("labels") or [],
            "npc": bool(zkb.get("npc")),
            "awox": bool(zkb.get("awox")),
            "solo": bool(zkb.get("solo")),
            "href": zkb.get("href"),
            "package_json": json.dumps({"package": pkg}, separators=(",", ":")),
        })

    # 2) Ledger primero (no aceptamos datos sin ledger)
    if raw_rows and ledger:
        lines = [json.dumps(r, separators=(",", ":")) for r in raw_rows]
        ledger.append("redisq_raw", rid, lines)

    # 3) Load a BigQuery raw_log (batch)
    if raw_rows:
        nd = f"/tmp/redisq_raw_{rid}.ndjson"
        with open(nd, "w", encoding="utf-8") as f:
            for r in raw_rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        bq_load_ndjson(project, dataset, "redisq_raw_log", nd, "scripts/schemas/redisq_raw_log.json", append=True)

    # 4) Selecciona pendientes (incluye backlog) y enriquece
    pending = choose_pending(project, dataset, limit_n=max_enrich)
    esi = ESI(user_agent=user_agent)

    enriched_rows = []
    discarded_rows = []
    attempt_rows = []

    for row in pending:
        if time.time() - start > max_seconds:
            break

        kill_id = int(row["killmail_id"])
        kill_hash = row.get("killmail_hash") or ""
        labels = row.get("labels") or []
        location_id = row.get("location_id")

        try:
            km = esi.killmail(kill_id, kill_hash)

            # reglas de descarte (Nota 3)
            # - pvp
            if not is_pvp(km) or bool(row.get("npc")):
                discarded_rows.append({
                    "run_id": rid, "discard_ts": utc_now_iso(), "killmail_id": kill_id,
                    "reason": "npc_only_or_not_pvp", "details": ""
                })
                attempt_rows.append({"run_id": rid, "attempt_ts": utc_now_iso(), "killmail_id": kill_id, "stage": "filter", "ok": True, "error": ""})
                continue

            # - no friendly fire (awox de redisq)
            if bool(row.get("awox")):
                discarded_rows.append({
                    "run_id": rid, "discard_ts": utc_now_iso(), "killmail_id": kill_id,
                    "reason": "awox_friendly_fire", "details": ""
                })
                attempt_rows.append({"run_id": rid, "attempt_ts": utc_now_iso(), "killmail_id": kill_id, "stage": "filter", "ok": True, "error": ""})
                continue

            # - no autodestrucción (heurística)
            if is_self_destruct(km):
                discarded_rows.append({
                    "run_id": rid, "discard_ts": utc_now_iso(), "killmail_id": kill_id,
                    "reason": "self_destruct", "details": ""
                })
                attempt_rows.append({"run_id": rid, "attempt_ts": utc_now_iso(), "killmail_id": kill_id, "stage": "filter", "ok": True, "error": ""})
                continue

            # Enriquecimiento
            kill_time = km.get("killmail_time")  # ISO
            solar_system_id = int(km.get("solar_system_id"))
            solar_system_name = esi.system_name(solar_system_id)

            gate = stargate_route(location_id, esi)

            ship_class, is_freighter, is_capsule = victim_ship_class(km, labels, esi)

            corp_names = attacker_corp_names(km, esi)
            atk_count = len(km.get("attackers") or [])

            is_ganked = "ganked" in set(labels or [])
            has_sb = smartbomb_flag(km, esi)
            war_id = km.get("war_id") or 0

            enriched_rows.append({
                "run_id": rid,
                "enriched_ts": utc_now_iso(),
                "killmail_id": kill_id,
                "killmail_id_hash": sha256_int(kill_id),
                "killmail_hash": kill_hash,
                "killmail_time": kill_time,
                "solar_system_name": solar_system_name,
                "stargate_route": gate,
                "victim_ship_class": ship_class,
                "is_freighter": bool(is_freighter),
                "is_capsule": bool(is_capsule),
                "attacker_count": atk_count,
                "attacker_corp_names": corp_names,
                "is_ganked": bool(is_ganked),
                "has_smartbomb": bool(has_sb),
                "is_war_related": bool(int(war_id) > 0),
                "raw_esi_json": json.dumps(km, separators=(",", ":")),
            })
            attempt_rows.append({"run_id": rid, "attempt_ts": utc_now_iso(), "killmail_id": kill_id, "stage": "enrich", "ok": True, "error": ""})

        except Exception as ex:
            attempt_rows.append({"run_id": rid, "attempt_ts": utc_now_iso(), "killmail_id": kill_id, "stage": "enrich", "ok": False, "error": str(ex)[:500]})

    # 5) Ledger (intentos + resultados) antes de BQ
    if ledger:
        if attempt_rows:
            ledger.append("enrich_attempts", rid, [json.dumps(r, separators=(",", ":")) for r in attempt_rows])
        if enriched_rows:
            ledger.append("enriched", rid, [json.dumps(r, separators=(",", ":")) for r in enriched_rows])
        if discarded_rows:
            ledger.append("discarded", rid, [json.dumps(r, separators=(",", ":")) for r in discarded_rows])

    # 6) Load BQ logs
    if attempt_rows:
        nd = f"/tmp/attempts_{rid}.ndjson"
        with open(nd, "w", encoding="utf-8") as f:
            for r in attempt_rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        bq_load_ndjson(project, dataset, "enrich_attempts_log", nd, "scripts/schemas/enrich_attempts_log.json", append=True)

    if enriched_rows:
        nd = f"/tmp/enriched_{rid}.ndjson"
        with open(nd, "w", encoding="utf-8") as f:
            for r in enriched_rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        bq_load_ndjson(project, dataset, "kills_enriched_log", nd, "scripts/schemas/kills_enriched_log.json", append=True)

    if discarded_rows:
        nd = f"/tmp/discarded_{rid}.ndjson"
        with open(nd, "w", encoding="utf-8") as f:
            for r in discarded_rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        bq_load_ndjson(project, dataset, "kills_discarded_log", nd, "scripts/schemas/kills_discarded_log.json", append=True)

    print("tick done", {"run_id": rid, "raw": len(raw_rows), "pending": len(pending), "enriched": len(enriched_rows), "discarded": len(discarded_rows)})

def maintenance(project: str, dataset: str) -> None:
    """
    - Poda > 1 año en la tabla final (CREATE OR REPLACE)
    - Re-materializa tablas/vistas para refrescar expiración (sandbox expira a 60 días sí o sí). 
    - Compacta logs (opcional) para no pasar 10GB.
    """
    # Tabla final “de trabajo”: último año, dedupe por killmail_id (último enriched_ts)
    sql_enriched = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.kills_enriched` AS
WITH x AS (
  SELECT *
  FROM `{project}.{dataset}.kills_enriched_log`
  WHERE TIMESTAMP(killmail_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
),
d AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT x.*, ROW_NUMBER() OVER(PARTITION BY killmail_id ORDER BY enriched_ts DESC) rn
    FROM x
  )
  WHERE rn = 1
)
SELECT * FROM d
"""
    bq_query(sql_enriched, project)

    # Materialización opcional de estado raw (si quieres tabla, no solo view)
    sql_raw_state = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.kills_raw_state` AS
SELECT * FROM `{project}.{dataset}.v_kills_raw_state`
"""
    bq_query(sql_raw_state, project)

    # Compactación de logs (mantener ~400 días para margen)
    sql_compact_raw = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.redisq_raw_log` AS
SELECT *
FROM `{project}.{dataset}.redisq_raw_log`
WHERE redisq_request_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 400 DAY)
"""
    bq_query(sql_compact_raw, project)

    sql_compact_attempts = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.enrich_attempts_log` AS
SELECT *
FROM `{project}.{dataset}.enrich_attempts_log`
WHERE attempt_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 400 DAY)
"""
    bq_query(sql_compact_attempts, project)

    sql_compact_enriched = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.kills_enriched_log` AS
SELECT *
FROM `{project}.{dataset}.kills_enriched_log`
WHERE TIMESTAMP(killmail_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 400 DAY)
"""
    bq_query(sql_compact_enriched, project)

    sql_compact_discarded = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.kills_discarded_log` AS
SELECT *
FROM `{project}.{dataset}.kills_discarded_log`
WHERE discard_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 400 DAY)
"""
    bq_query(sql_compact_discarded, project)


if __name__ == "__main__":
    main()
