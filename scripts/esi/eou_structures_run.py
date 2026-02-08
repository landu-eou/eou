#!/usr/bin/env python3
"""
EOU · ESI Structures runner

- Interfaz por env vars (workflow YAML):
  STRUCTURE_LIST_JSON (puede venir vacío)
  STRUCTURES_URL
  STATE_FILE, DATA_FILE, TYPES_FILE, SOLARSYSTEMS_FILE
  STRUCTURES_LIST_ETAG, PRIOR_LIST_ETAG (pueden venir vacíos)
  EOU_ACCESS_TOKENS_1, EOU_ACCESS_TOKENS_2
  GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE
  ESI_USER_AGENT

Correcciones aplicadas (sin cambiar lógica funcional):
- Si no existe STRUCTURE_LIST_JSON ni fichero intermedio, obtiene el listado directo de STRUCTURES_URL con If-None-Match (etag de state).
- Observabilidad extra en el tramo de 401/420: muestra char_id usado, rotaciones, headers relevantes.
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.cloud import bigquery


PRIMARY_CHAR_ID = 2124070822

ESI_BASE = "https://esi.evetech.net"
STRUCTURE_DETAIL_URL = ESI_BASE + "/universe/structures/{structure_id}/"

MIN_REQUEST_INTERVAL_SECS = 0.05
ERROR_LIMIT_SLEEP_CAP_SECS = 180


@dataclass
class EsiErrorLimit:
    remain: Optional[int]
    reset: Optional[int]


def _env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _env_optional(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _env_json_dict(name: str) -> Dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            raise ValueError("not a dict")
        return obj
    except Exception as e:
        raise RuntimeError(f"Invalid JSON in env var {name}: {e}") from e


def _normalize_etag(etag: Optional[str]) -> Optional[str]:
    if not etag:
        return None
    t = etag.strip()
    if t.startswith("W/"):
        t = t[2:].strip()
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        t = t[1:-1]
    return t or None


def _etag_header_value(etag: Optional[str]) -> Optional[str]:
    t = _normalize_etag(etag)
    if not t:
        return None
    return f'"{t}"'


def _parse_error_limit(headers: requests.structures.CaseInsensitiveDict) -> EsiErrorLimit:
    remain = headers.get("X-ESI-Error-Limit-Remain")
    reset = headers.get("X-ESI-Error-Limit-Reset")
    try:
        remain_i = int(remain) if remain is not None else None
    except Exception:
        remain_i = None
    try:
        reset_i = int(reset) if reset is not None else None
    except Exception:
        reset_i = None
    return EsiErrorLimit(remain=remain_i, reset=reset_i)


def _sleep_for_error_limit(limit: EsiErrorLimit) -> None:
    if limit.remain is None or limit.reset is None:
        return
    if limit.remain <= 1 and limit.reset > 0:
        secs = min(limit.reset + 1, ERROR_LIMIT_SLEEP_CAP_SECS)
        print(f"[esi] Error-limit low (remain={limit.remain}). Sleeping {secs}s (reset={limit.reset})")
        time.sleep(secs)


def _select_access_tokens() -> List[Tuple[int, str]]:
    m1 = _env_json_dict("EOU_ACCESS_TOKENS_1")
    m2 = _env_json_dict("EOU_ACCESS_TOKENS_2")

    combined: Dict[int, str] = {}
    for m in (m1, m2):
        for k, v in m.items():
            try:
                cid = int(k)
            except Exception:
                continue
            if isinstance(v, str) and v.strip():
                combined[cid] = v.strip()

    if not combined:
        raise RuntimeError("No access tokens found in EOU_ACCESS_TOKENS_1/2")

    ordered: List[Tuple[int, str]] = []
    if PRIMARY_CHAR_ID in combined:
        ordered.append((PRIMARY_CHAR_ID, combined[PRIMARY_CHAR_ID]))

    for cid in sorted(combined.keys(), reverse=True):
        if cid == PRIMARY_CHAR_ID:
            continue
        ordered.append((cid, combined[cid]))

    # Log seguro: solo IDs, no tokens
    ids = [cid for cid, _ in ordered]
    print(f"[auth] Available ESI access tokens for character IDs (priority order): {ids}")
    if PRIMARY_CHAR_ID not in ids:
        print(f"[auth] WARNING: primary character id {PRIMARY_CHAR_ID} token not present.")

    return ordered


def _read_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_gz_jsonl(path: str) -> Dict[int, Dict[str, Any]]:
    records: Dict[int, Dict[str, Any]] = {}
    p = Path(path)
    if not p.exists():
        return records

    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sid = int(obj["stationID"])
            obj["etag"] = _normalize_etag(obj.get("etag"))
            records[sid] = obj
    return records


def _write_text_atomic(path: str, content: str) -> None:
    dst = Path(path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(dst)


def _update_fields(record: Dict[str, Any], updates: Dict[str, Any]) -> bool:
    changed = False
    for k, v in updates.items():
        if record.get(k) != v:
            record[k] = v
            changed = True
    return changed


def _load_solarsystems_map(solarsystems_gz: str) -> Dict[int, str]:
    m: Dict[int, str] = {}
    with gzip.open(solarsystems_gz, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sid = int(obj["solarSystemID"])
            name = obj.get("solarSystem")
            if isinstance(name, str):
                m[sid] = name
    return m


def _load_types_subset(types_gz: str, needed_type_ids: Iterable[int]) -> Dict[int, str]:
    need = set(needed_type_ids)
    if not need:
        return {}
    out: Dict[int, str] = {}
    with gzip.open(types_gz, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            tid = int(obj["typeID"])
            if tid in need:
                tname = obj.get("type")
                if isinstance(tname, str):
                    out[tid] = tname
                if len(out) == len(need):
                    break
    return out


def _bq_rewrite_table_and_load(project_id: str, dataset: str, table: str, ndjson_path: str) -> None:
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    client.delete_table(table_id, not_found_ok=True)

    schema = [
        bigquery.SchemaField("stationID", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("station", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stationType", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("solarSystem", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dock", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("market", "BOOLEAN", mode="REQUIRED"),
    ]
    t = bigquery.Table(table_id, schema=schema)
    t.clustering_fields = ["solarSystem"]
    client.create_table(t)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    with open(ndjson_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()


class EsiClient:
    def __init__(self, tokens: List[Tuple[int, str]], user_agent: str):
        self.tokens = tokens
        self.ua = user_agent
        self.s = requests.Session()
        self.s.headers.update(
            {
                "Accept": "application/json",
                "Accept-Language": "en",
                "User-Agent": self.ua,
            }
        )
        self._token_index = 0
        self._last_request_ts = 0.0
        self.retry_budget_401_420 = 3

    def _throttle(self) -> None:
        now = time.time()
        dt = now - self._last_request_ts
        if dt < MIN_REQUEST_INTERVAL_SECS:
            time.sleep(MIN_REQUEST_INTERVAL_SECS - dt)
        self._last_request_ts = time.time()

    def _current_token(self) -> Tuple[int, str]:
        return self.tokens[self._token_index]

    def _rotate_token(self) -> bool:
        if len(self.tokens) <= 1:
            return False
        self._token_index = (self._token_index + 1) % len(self.tokens)
        return True

    def get_structure(
        self,
        structure_id: int,
        if_none_match_etag: Optional[str],
        progress: Optional[str] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]], Optional[str], EsiErrorLimit]:
        """
        Returns: (status_code, json_or_none, etag_new_or_none, error_limit_headers)
        """
        url = STRUCTURE_DETAIL_URL.format(structure_id=structure_id)

        attempt = 0
        while True:
            attempt += 1
            self._throttle()

            char_id, token = self._current_token()
            headers = {"Authorization": f"Bearer {token}"}
            inm = _etag_header_value(if_none_match_etag)
            if inm:
                headers["If-None-Match"] = inm

            # Log de request (sin token)
            pfx = f"[esi]{' '+progress if progress else ''}"
            print(f"{pfx} GET structure {structure_id} using char_id={char_id} attempt={attempt} inm={'yes' if inm else 'no'}")

            r = self.s.get(url, headers=headers, timeout=(5, 30))
            limit = _parse_error_limit(r.headers)
            etag_new = _normalize_etag(r.headers.get("ETag"))

            # Log respuesta básica
            www = r.headers.get("WWW-Authenticate")
            if r.status_code in (401, 420) or r.status_code >= 500:
                print(
                    f"{pfx} status={r.status_code} "
                    f"error_limit(remain={limit.remain}, reset={limit.reset}) "
                    f"{'WWW-Authenticate='+www if www else ''}"
                )

            if r.status_code >= 400:
                _sleep_for_error_limit(limit)

            if r.status_code in (401, 420):
                if self.retry_budget_401_420 <= 0:
                    # Más detalle antes de lanzar
                    ids = [cid for cid, _ in self.tokens]
                    raise RuntimeError(
                        f"Retry budget exhausted for 401/420 (last status={r.status_code}). "
                        f"structure_id={structure_id}, last_char_id={char_id}, tokens_available={ids}"
                    )

                self.retry_budget_401_420 -= 1

                sleep_secs = 30
                if r.status_code == 420 and limit.reset is not None and limit.reset > sleep_secs:
                    sleep_secs = min(limit.reset + 1, ERROR_LIMIT_SLEEP_CAP_SECS)

                rotated = False
                if r.status_code == 401:
                    rotated = self._rotate_token()

                print(
                    f"{pfx} retrying after {sleep_secs}s; "
                    f"retry_budget_remaining={self.retry_budget_401_420}; "
                    f"rotated_token={'yes' if rotated else 'no'}"
                )
                time.sleep(sleep_secs)
                continue

            if r.status_code == 200:
                try:
                    return r.status_code, r.json(), etag_new, limit
                except Exception:
                    raise RuntimeError(f"Invalid JSON in 200 response for structure {structure_id}")

            return r.status_code, None, etag_new, limit


def _try_find_list_file_from_env_or_common_paths() -> Optional[str]:
    v = _env_optional("STRUCTURE_LIST_JSON")
    if v and Path(v).exists():
        return v

    candidates: List[str] = []
    rt = _env_optional("RUNNER_TEMP")
    if rt:
        candidates.append(str(Path(rt) / "structure_list.json"))

    ws = _env_optional("GITHUB_WORKSPACE")
    if ws:
        ws_p = Path(ws)
        candidates.append(str(ws_p / "structure_list.json"))
        try:
            work_root = ws_p.parents[1]
            candidates.append(str(work_root / "_temp" / "structure_list.json"))
        except Exception:
            pass

    candidates.append("/tmp/structure_list.json")
    for c in candidates:
        if c and Path(c).exists():
            return c
    return None


def _fetch_structure_list_fallback(structures_url: str, state_file: str, user_agent: str) -> Tuple[List[int], Optional[str]]:
    prior_etag = None
    if Path(state_file).exists():
        try:
            obj = _read_json_file(state_file)
            if isinstance(obj, dict):
                prior_etag = _normalize_etag(obj.get("etag"))
        except Exception:
            prior_etag = None

    headers = {"Accept": "application/json", "User-Agent": user_agent}
    inm = _etag_header_value(prior_etag)
    if inm:
        headers["If-None-Match"] = inm

    print(f"[fallback] Fetching structures list directly (inm={'yes' if inm else 'no'})")
    r = requests.get(structures_url, headers=headers, timeout=(5, 60))
    new_etag = _normalize_etag(r.headers.get("ETag"))
    limit = _parse_error_limit(r.headers)
    if r.status_code >= 400:
        _sleep_for_error_limit(limit)

    if r.status_code == 304:
        print("[fallback] structures list 304 Not Modified; exiting without work.")
        return [], None

    if r.status_code == 200:
        try:
            data = r.json()
        except Exception:
            raise RuntimeError("[fallback] 200 but invalid JSON for structures list")
        if not isinstance(data, list):
            raise RuntimeError("[fallback] structures list JSON is not a list")
        print(f"[fallback] structures list 200 OK; count={len(data)} etag={'present' if new_etag else 'missing'}")
        return [int(x) for x in data], new_etag

    if r.status_code == 420:
        raise RuntimeError("[fallback] structures list returned 420 (error limited)")
    if r.status_code >= 500:
        raise RuntimeError(f"[fallback] structures list returned {r.status_code} (server error)")
    raise RuntimeError(f"[fallback] structures list returned unexpected status {r.status_code}")


def main() -> int:
    structures_url = _env_required("STRUCTURES_URL")
    state_file = _env_required("STATE_FILE")
    data_file = _env_required("DATA_FILE")
    types_file = _env_required("TYPES_FILE")
    solarsystems_file = _env_required("SOLARSYSTEMS_FILE")

    project_id = _env_required("GCP_PROJECT_ID")
    bq_dataset = _env_required("BQ_DATASET")
    bq_table = _env_required("BQ_TABLE")

    user_agent = os.getenv("ESI_USER_AGENT", "landu-eou/eou (GitHub Actions; ESI Structures)")

    prior_list_etag = _normalize_etag(_env_optional("PRIOR_LIST_ETAG"))
    new_list_etag = _normalize_etag(_env_optional("STRUCTURES_LIST_ETAG"))

    # 1) Structure list
    list_file = _try_find_list_file_from_env_or_common_paths()
    if list_file:
        print(f"[list] Using structure list file: {list_file}")
        structure_ids_raw = _read_json_file(list_file)
        if not isinstance(structure_ids_raw, list):
            raise RuntimeError("STRUCTURE_LIST_JSON is not a JSON list")
        structure_ids = [int(x) for x in structure_ids_raw]
    else:
        structure_ids, fetched_etag = _fetch_structure_list_fallback(structures_url, state_file, user_agent)
        if not structure_ids and fetched_etag is None:
            return 0
        if not new_list_etag and fetched_etag:
            new_list_etag = fetched_etag

    # 2) Load existing records
    records = _read_gz_jsonl(data_file)
    desired_ids = set(structure_ids)

    data_changed = False

    # 3) Reconciliación
    for sid in list(records.keys()):
        if sid not in desired_ids:
            del records[sid]
            data_changed = True

    for sid in desired_ids:
        if sid not in records:
            records[sid] = {
                "stationID": sid,
                "station": None,
                "stationType": None,
                "solarSystem": None,
                "dock": None,
                "market": True,
                "etag": None,
            }
            data_changed = True
        else:
            if records[sid].get("market") is not True:
                records[sid]["market"] = True
                data_changed = True

    # 4) Load maps
    solar_map = _load_solarsystems_map(solarsystems_file)
    pending_type_ids: Dict[int, int] = {}

    # 5) Enriquecimiento
    tokens = _select_access_tokens()
    esi = EsiClient(tokens=tokens, user_agent=user_agent)

    sorted_ids = sorted(records.keys())
    total = len(sorted_ids)
    print(f"[run] structures in list: {total} (existing file had {len(records)})")

    for idx, sid in enumerate(sorted_ids, start=1):
        if idx == 1 or idx % 10 == 0 or idx == total:
            print(f"[progress] enriching {idx}/{total} (retry_budget_remaining={esi.retry_budget_401_420})")

        rec = records.get(sid)
        if rec is None:
            continue

        progress = f"{idx}/{total}"
        status, payload, etag_new, _limit = esi.get_structure(sid, rec.get("etag"), progress=progress)

        if status == 304:
            continue

        if status == 200 and payload is not None:
            name = payload.get("name")
            solar_system_id = payload.get("solar_system_id")
            type_id = payload.get("type_id")

            dock = True

            solar_name = None
            if isinstance(solar_system_id, int) or (
                isinstance(solar_system_id, str) and solar_system_id.isdigit()
            ):
                solar_name = solar_map.get(int(solar_system_id))

            updates = {
                "station": name if isinstance(name, str) else None,
                "solarSystem": solar_name,
                "dock": dock,
                "market": True,
                "etag": etag_new if etag_new is not None else rec.get("etag"),
            }
            if _update_fields(rec, updates):
                data_changed = True

            if type_id is not None:
                try:
                    pending_type_ids[sid] = int(type_id)
                except Exception:
                    pending_type_ids[sid] = -1
            else:
                pending_type_ids[sid] = -1

            continue

        if status == 403:
            updates = {
                "station": None,
                "stationType": None,
                "solarSystem": None,
                "dock": None,
                "market": True,
                "etag": etag_new if etag_new is not None else rec.get("etag"),
            }
            if _update_fields(rec, updates):
                data_changed = True
            continue

        if status == 404:
            del records[sid]
            data_changed = True
            continue

        if status >= 500:
            continue

        continue

    # 6) Resolver stationType
    needed_type_ids = [tid for tid in pending_type_ids.values() if tid and tid > 0]
    type_map = _load_types_subset(types_file, needed_type_ids)

    for sid, type_id in pending_type_ids.items():
        rec = records.get(sid)
        if rec is None:
            continue
        station_type = type_map.get(type_id)
        if rec.get("stationType") != station_type:
            rec["stationType"] = station_type
            data_changed = True

    # 7) Preparar volcado BQ
    rows_to_load: List[Dict[str, Any]] = []
    if data_changed:
        for rec in records.values():
            if (
                rec.get("station") is not None
                and rec.get("stationType") is not None
                and rec.get("solarSystem") is not None
                and rec.get("dock") is not None
            ):
                rows_to_load.append(
                    {
                        "stationID": int(rec["stationID"]),
                        "station": rec["station"],
                        "stationType": rec["stationType"],
                        "solarSystem": rec["solarSystem"],
                        "dock": bool(rec["dock"]),
                        "market": bool(rec.get("market", True)),
                    }
                )

    bq_should_write = bool(data_changed and len(rows_to_load) > 0)

    # 8) Volcados
    data_tmp_path: Optional[str] = None
    if data_changed:
        dst = Path(data_file)
        dst.parent.mkdir(parents=True, exist_ok=True)
        data_tmp_path = str(dst.with_suffix(dst.suffix + ".tmpbuild"))
        with gzip.open(data_tmp_path, "wt", encoding="utf-8", newline="\n") as f:
            for sid in sorted(records.keys()):
                obj = records[sid]
                clean = {k: v for k, v in obj.items() if not k.startswith("_")}
                f.write(json.dumps(clean, ensure_ascii=False, separators=(",", ":")) + "\n")

    ndjson_root = _env_optional("RUNNER_TEMP") or _env_optional("GITHUB_WORKSPACE") or "/tmp"
    ndjson_tmp: Optional[str] = None

    if bq_should_write:
        ndjson_tmp = str(Path(ndjson_root) / "eou_structures_bq.ndjson")
        with open(ndjson_tmp, "w", encoding="utf-8", newline="\n") as f:
            for row in rows_to_load:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

        print(f"[bq] Rewriting {project_id}.{bq_dataset}.{bq_table} with {len(rows_to_load)} rows...")
        _bq_rewrite_table_and_load(project_id, bq_dataset, bq_table, ndjson_tmp)
        print("[bq] OK")

    if data_changed and data_tmp_path:
        Path(data_tmp_path).replace(Path(data_file))
        print(f"[data] Updated {data_file} ({len(records)} records).")
    else:
        print("[data] No changes; data file not rewritten.")

    # 9) Update state etag (estricto)
    if new_list_etag and new_list_etag != prior_list_etag:
        state_payload = json.dumps({"etag": new_list_etag}, ensure_ascii=False, separators=(",", ":")) + "\n"
        _write_text_atomic(state_file, state_payload)
        print(f"[state] Updated list etag in {state_file}: {prior_list_etag} -> {new_list_etag}")
    else:
        print("[state] List etag unchanged (or missing); state not updated.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"[fatal] {e}", file=sys.stderr)
        raise
