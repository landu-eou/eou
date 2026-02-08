#!/usr/bin/env python3
"""
EOU · ESI Structures runner
- Inputs vía env vars (interfaz exacta esperada por el workflow YAML)
- Reconciliación + enriquecimiento por estructura con ETag por registro
- Retries globales para 401/420 (máx 3 en todo el run; sleep >= 30s)
- Manejo 403 (nulls + market=true), 404 (borrar), 5xx (mantener)
- Volcado "de golpe" (atomic replace) a data/esi/structures.jsonl.gz
- Reescritura BQ (delete+create+load) solo si hay cambios y hay filas completas a volcar
- Actualiza states/structures.json (ETag del listado) solo si:
    * ETag del listado cambió
    * data file se reescribió OK (o no se reescribió porque no había cambios)
    * BQ se reescribió OK (o no se reescribió porque no había nada que volcar / no tocaba)
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

# Cortesía + sostenibilidad: muy pequeño; el throttle real viene por error-limit headers y 420.
MIN_REQUEST_INTERVAL_SECS = 0.05

# En ESI, 4xx/5xx cuentan para el "error limit". Para evitar 420, si el remain cae bajo,
# dormimos hasta reset (cap conservador para no eternizar el job).
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
    """Value for If-None-Match header."""
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
    """If remain is critically low, sleep until reset to avoid 420."""
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
            # Normaliza ETag por si el fichero lo guardó con comillas.
            obj["etag"] = _normalize_etag(obj.get("etag"))
            records[sid] = obj
    return records


def _write_gz_jsonl_atomic(path: str, records: Iterable[Dict[str, Any]]) -> None:
    dst = Path(path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")

    with gzip.open(tmp, "wt", encoding="utf-8", newline="\n") as f:
        for obj in records:
            # Nunca escribimos campos internos (p.ej. _type_id) si existieran.
            clean = {k: v for k, v in obj.items() if not k.startswith("_")}
            f.write(json.dumps(clean, ensure_ascii=False, separators=(",", ":")) + "\n")

    tmp.replace(dst)


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
                # early exit si ya los tenemos todos
                if len(out) == len(need):
                    break
    return out


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

    def _rotate_token(self) -> None:
        if len(self.tokens) <= 1:
            return
        self._token_index = (self._token_index + 1) % len(self.tokens)

    def get_structure(
        self, structure_id: int, if_none_match_etag: Optional[str]
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
            headers = {
                "Authorization": f"Bearer {token}",
            }
            inm = _etag_header_value(if_none_match_etag)
            if inm:
                headers["If-None-Match"] = inm

            r = self.s.get(url, headers=headers, timeout=(5, 30))
            limit = _parse_error_limit(r.headers)
            etag_new = _normalize_etag(r.headers.get("ETag"))

            # Ser buen ciudadano con el error limit
            if r.status_code >= 400:
                _sleep_for_error_limit(limit)

            if r.status_code in (401, 420):
                # Retry global (máx 3 para todo el run)
                if self.retry_budget_401_420 <= 0:
                    raise RuntimeError(
                        f"Retry budget exhausted for 401/420 (last status={r.status_code})"
                    )
                self.retry_budget_401_420 -= 1

                sleep_secs = 30
                # Para 420, si Reset viene mayor, esperamos (>=30) para no rebotar.
                if r.status_code == 420 and limit.reset is not None and limit.reset > sleep_secs:
                    sleep_secs = min(limit.reset + 1, ERROR_LIMIT_SLEEP_CAP_SECS)

                # Para 401, probamos rotar token por si este está caducado.
                if r.status_code == 401:
                    self._rotate_token()

                print(
                    f"[esi] {r.status_code} on structure {structure_id} (attempt={attempt}). "
                    f"Sleeping {sleep_secs}s. Remaining retry_budget={self.retry_budget_401_420}"
                )
                time.sleep(sleep_secs)
                continue

            if r.status_code == 200:
                try:
                    return r.status_code, r.json(), etag_new, limit
                except Exception:
                    # JSON inválido: cuenta como error práctico; tratamos como 5xx para no romper datos,
                    # pero abortamos antes de escribir archivos (run fallará) para evitar incoherencias.
                    raise RuntimeError(f"Invalid JSON in 200 response for structure {structure_id}")

            # 304 / 403 / 404 / 5xx / otros
            return r.status_code, None, etag_new, limit


def _bq_rewrite_table_and_load(
    project_id: str, dataset: str, table: str, ndjson_path: str
) -> None:
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    # Delete + create (tabla sin particiones, con clustering)
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
    job.result()  # raise on failure


def main() -> int:
    structure_list_json = _env_required("STRUCTURE_LIST_JSON")
    data_file = _env_required("DATA_FILE")
    state_file = _env_required("STATE_FILE")
    types_file = _env_required("TYPES_FILE")
    solarsystems_file = _env_required("SOLARSYSTEMS_FILE")

    project_id = _env_required("GCP_PROJECT_ID")
    bq_dataset = _env_required("BQ_DATASET")
    bq_table = _env_required("BQ_TABLE")

    user_agent = os.getenv("ESI_USER_AGENT", "landu-eou/eou (GitHub Actions; ESI Structures)")

    prior_list_etag = _normalize_etag(os.getenv("PRIOR_LIST_ETAG", "") or None)
    new_list_etag = _normalize_etag(os.getenv("STRUCTURES_LIST_ETAG", "") or None)

    tokens = _select_access_tokens()
    esi = EsiClient(tokens=tokens, user_agent=user_agent)

    # 1) Load structure_list (IDs públicos de mercado)
    structure_ids = _read_json_file(structure_list_json)
    if not isinstance(structure_ids, list):
        raise RuntimeError("STRUCTURE_LIST_JSON is not a JSON list")

    desired_ids = set(int(x) for x in structure_ids)

    # 2) Load existing records
    records = _read_gz_jsonl(data_file)

    data_changed = False

    # 3) Reconciliación (delete missing, add new)
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
            # Asegura market=true (lista filtrada por mercado)
            if records[sid].get("market") is not True:
                records[sid]["market"] = True
                data_changed = True

    # 4) Load solar systems (pequeño) y prepara type-id pending
    solar_map = _load_solarsystems_map(solarsystems_file)
    pending_type_ids: Dict[int, int] = {}  # stationID -> type_id

    # 5) Enriquecimiento por estructura
    # Orden estable para diffs mínimos y reproducibilidad.
    sorted_ids = sorted(records.keys())
    print(f"[run] structures in list: {len(sorted_ids)} (existing file had {len(records)})")

    for i, sid in enumerate(sorted_ids, start=1):
        rec = records.get(sid)
        if rec is None:
            continue  # pudo borrarse por 404 previo

        status, payload, etag_new, _limit = esi.get_structure(sid, rec.get("etag"))

        if status == 304:
            # Mantener registro actual para este run
            continue

        if status == 200 and payload is not None:
            # ESI devuelve name, solar_system_id, type_id, owner_id (según docs).
            name = payload.get("name")
            solar_system_id = payload.get("solar_system_id")
            type_id = payload.get("type_id")

            # Dock: interpretación práctica alineada con el propio requisito y la semántica del endpoint:
            # si podemos leer /universe/structures/{id}/ con token authed, la estructura es "dockable" (para ese contexto).
            # En 403 explícitamente quieren dock=null.
            dock = True

            solar_name = None
            if isinstance(solar_system_id, int) or (isinstance(solar_system_id, str) and solar_system_id.isdigit()):
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
            # Actualizar registro con nulls + market=true + nuevo etag
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
            # Borrar registro
            del records[sid]
            data_changed = True
            continue

        if status >= 500:
            # Mantener registro actual para este run
            continue

        # Otros códigos 4xx no especificados: mantener registro (conservador).
        # Cuentan como error para ESI, pero no tocamos datos para no generar ruido.
        continue

    # 6) Resolver stationType (solo los type_id que hemos visto en 200)
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

    # 7) Preparar volcado a BQ (solo filas completas)
    # Regla de volcado: solo si hay algo que volcar Y ha habido cambios.
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

    # 8) Volcados (de golpe, evitando estados parciales):
    #    - Escribimos data a .tmp (sin reemplazar) y NDJSON a temp.
    #    - Si BQ toca, reescribimos BQ.
    #    - Si todo OK, reemplazamos data file y (si corresponde) state file.
    data_tmp_path = None
    if data_changed:
        # Pre-escribe el gzip final a tmp (para no modificar el repo si BQ falla luego)
        dst = Path(data_file)
        dst.parent.mkdir(parents=True, exist_ok=True)
        data_tmp_path = str(dst.with_suffix(dst.suffix + ".tmpbuild"))
        with gzip.open(data_tmp_path, "wt", encoding="utf-8", newline="\n") as f:
            for sid in sorted(records.keys()):
                obj = records[sid]
                clean = {k: v for k, v in obj.items() if not k.startswith("_")}
                f.write(json.dumps(clean, ensure_ascii=False, separators=(",", ":")) + "\n")

    ndjson_tmp = None
    if bq_should_write:
        ndjson_tmp = str(Path(os.getenv("RUNNER_TEMP", "/tmp")) / "eou_structures_bq.ndjson")
        with open(ndjson_tmp, "w", encoding="utf-8", newline="\n") as f:
            for row in rows_to_load:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

        print(f"[bq] Rewriting {project_id}.{bq_dataset}.{bq_table} with {len(rows_to_load)} rows...")
        _bq_rewrite_table_and_load(project_id, bq_dataset, bq_table, ndjson_tmp)
        print("[bq] OK")

    # Si llegamos aquí, BQ o se escribió OK o no tocaba.
    if data_changed and data_tmp_path:
        Path(data_tmp_path).replace(Path(data_file))
        print(f"[data] Updated {data_file} ({len(records)} records).")
    else:
        print("[data] No changes; data file not rewritten.")

    # 9) Actualizar states/structures.json (etag del listado) SOLO si:
    #    - etag cambió
    #    - data OK (reescrito o no tocaba)  -> aquí siempre OK si no se lanzó excepción
    #    - bq OK (reescrito o no tocaba)    -> aquí siempre OK si no se lanzó excepción
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
        # Fallo “duro”: no tocará el state etag, y el workflow quedará failed (next_run corto).
        print(f"[fatal] {e}", file=sys.stderr)
        raise
