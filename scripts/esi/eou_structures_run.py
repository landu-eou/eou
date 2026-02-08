#!/usr/bin/env python3
"""
EOU · ESI Structures (ESI → GH → BQ)

Objetivo
--------
Pipeline "silencioso" (sin logs para usuario) para:
- Calcular SDE_NEXT_RUN (HEAD a SDE_ZIP_URL sin etag).
- Consultar listado de estructuras (ESI_STRUCTURES_LIST_URL) usando ETag global almacenado en states/structures.json.
- Mantener un cache local en data/esi/structures.jsonl.gz (JSONL comprimido, sobrescritura atómica cuando corresponde).
- Enriquecer estructura por estructura (ESI_STRUCTURE_DETAIL_URL_TEMPLATE) autenticado, usando ETag por registro, con retries globales para 401/420.
- Volcar a BigQuery Sandbox (tabla eou.structures) SOLO si corresponde (cambios + filas válidas).
- Actualizar states/structures.json (ETag global del listado) SOLO si corresponde:
    * etag cambió
    * y las fases "data" y "bq" no fallaron (pueden estar SKIPPED porque "no correspondía reescribir")

Contrato (inputs por env vars)
-----------------------------
Requeridos:
  - GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE
  - STATE_ETAG_PATH, DATA_STRUCTURES_PATH
  - SDE_SOLARSYSTEMS_PATH, SDE_TYPES_PATH
  - SDE_ZIP_URL
  - ESI_STRUCTURES_LIST_URL
  - ESI_STRUCTURE_DETAIL_URL_TEMPLATE  (usa {structure_id})
  - PRIMARY_CHAR_ID
  - RETRY_BUDGET
  - EOU_ACCESS_TOKENS_1, EOU_ACCESS_TOKENS_2  (JSON dict {char_id: access_token})

Controles operativos (sostenibilidad ESI; no alteran decisiones, solo pacing):
  - ESI_MAX_RPM (default 60)
  - ESI_ERR_REMAIN_SOFT (default 20)
  - ESI_ERR_REMAIN_HARD (default 5)

Outputs (a $GITHUB_OUTPUT)
--------------------------
  - status: completed|failed
  - next_run_epoch: int (UTC epoch seconds)
  - write_last_modified: true|false
  - last_modified_epoch: int|""  (UTC epoch seconds del header Last-Modified del listado)
  - repo_dirty: true|false       (si el repo debe commitear data/states)

Notas de diseño
---------------
- No imprime métricas, no escribe Step Summary, no imprime tracebacks.
- El workflow decide estado/next_run escribiendo en Sheets usando estos outputs.
- ESI: aplica limitación por RPM y se vuelve conservador si X-ESI-Error-Limit-Remain baja,
  para reducir riesgo de 420 (error limiting).
"""

from __future__ import annotations

import dataclasses
import email.utils
import gzip
import hashlib
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

UTC = timezone.utc


# ------------------------- helpers: env + GHA outputs -------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise KeyError(f"Missing required env var: {name}")
        return default
    return v


def gha_set_output(key: str, value: str) -> None:
    """
    Escribe outputs para consumo del workflow (step.id.outputs.*).
    No imprime nada: en modo "silencioso" la señal es únicamente exit code + outputs.
    """
    path = os.getenv("GITHUB_OUTPUT")
    if not path:
        return
    value = value.replace("\n", " ").replace("\r", " ")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def now_epoch() -> int:
    return int(datetime.now(tz=UTC).timestamp())


def parse_http_date_to_epoch(http_date: Optional[str]) -> Optional[int]:
    if not http_date:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(http_date)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.astimezone(UTC).timestamp())
    except Exception:
        return None


def sleep_with_jitter(seconds: int, jitter_max: int = 3) -> None:
    if seconds <= 0:
        return
    jitter = random.SystemRandom().randint(0, jitter_max)
    time.sleep(seconds + jitter)


# ------------------------- stage states (para condición "strict state update") -------------------------

SKIPPED = "SKIPPED"
DONE_OK = "DONE_OK"
FAILED = "FAILED"


# ------------------------- ESI throttle / pacing policy -------------------------

class RateLimiter:
    """
    Limitador por ventana deslizante (max RPM) + ralentización dinámica.

    - Base: max_rpm requests/min
    - Dynamic: si X-ESI-Error-Limit-Remain baja, se reduce el ritmo hasta el reset.
    """

    def __init__(self, max_rpm: int) -> None:
        self.base_rpm = max(1, max_rpm)
        self.window: Deque[float] = deque()
        self.dynamic_rpm: Optional[int] = None

    def _effective_rpm(self) -> int:
        if self.dynamic_rpm is None:
            return self.base_rpm
        return max(1, min(self.base_rpm, self.dynamic_rpm))

    def before_request(self) -> None:
        rpm = self._effective_rpm()
        interval = 60.0 / float(rpm)

        now = time.monotonic()
        while self.window and (now - self.window[0]) > 60.0:
            self.window.popleft()

        if self.window:
            next_allowed = self.window[-1] + interval
            if now < next_allowed:
                time.sleep(next_allowed - now)

        self.window.append(time.monotonic())

    def observe_error_limit_headers(self, headers_lc: Dict[str, str], soft: int, hard: int) -> None:
        """
        ESI error limit headers:
          - X-ESI-Error-Limit-Remain
          - X-ESI-Error-Limit-Reset
        Se usan para volverse más conservador y evitar 420.
        """
        remain_s = headers_lc.get("x-esi-error-limit-remain")
        try:
            if remain_s is None:
                return
            remain = int(remain_s)

            if remain < hard:
                self.dynamic_rpm = max(1, self.base_rpm // 6)
            elif remain < soft:
                self.dynamic_rpm = max(1, self.base_rpm // 2)
            else:
                self.dynamic_rpm = None
        except Exception:
            return

    def backoff_on_420_or_429(self, headers_lc: Dict[str, str]) -> None:
        """
        Backoff fuerte para 420/429.
        Si hay headers de retry/reset se respetan; si no, se usa un mínimo conservador.
        """
        reset_s = headers_lc.get("x-esi-error-limit-reset")
        retry_after = headers_lc.get("retry-after")
        wait = 60
        for cand in (retry_after, reset_s):
            try:
                if cand is not None:
                    wait = max(wait, int(cand))
            except Exception:
                pass
        sleep_with_jitter(wait + 2, jitter_max=5)


# ------------------------- HTTP (headers normalized to lowercase) -------------------------

def http_request(
    limiter: RateLimiter,
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Tuple[int, Dict[str, str], bytes]:
    limiter.before_request()
    req = urllib.request.Request(url=url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", resp.getcode()))
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            body = resp.read() if method != "HEAD" else b""
            return status, hdrs, body
    except urllib.error.HTTPError as e:
        hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        body = e.read() if method != "HEAD" else b""
        return int(e.code), hdrs, body
    except Exception as e:
        # Fallos de red/timeout se tratan como 503-like para decisiones de "5xx".
        return 503, {}, (str(e).encode("utf-8")[:200])


# ------------------------- ETag normalization (global y por registro) -------------------------

def normalize_etag(etag: Optional[str]) -> Optional[str]:
    if not etag:
        return None
    s = etag.strip()
    if s.startswith("W/"):
        s = s[2:].strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    s = s.strip()
    return s or None


def etag_to_if_none_match(stored_etag: Optional[str]) -> Optional[str]:
    n = normalize_etag(stored_etag)
    if not n:
        return None
    return f"\"{n}\""


# ------------------------- data model: structures cache (jsonl.gz) -------------------------

@dataclasses.dataclass
class StructureRecord:
    stationID: int
    station: Optional[str] = None
    stationType: Optional[str] = None
    solarSystem: Optional[str] = None
    dock: Optional[bool] = None
    market: bool = True
    etag: Optional[str] = None

    # Campos transitorios (para resolver nombres SDE "on-demand")
    _type_id: Optional[int] = None
    _solar_system_id: Optional[int] = None

    def to_json_obj(self) -> Dict[str, Any]:
        # Orden estable (útil para diffs/hashes)
        return {
            "stationID": self.stationID,
            "station": self.station,
            "stationType": self.stationType,
            "solarSystem": self.solarSystem,
            "dock": self.dock,
            "market": self.market,
            "etag": self.etag,
        }

    @staticmethod
    def from_json_obj(obj: Dict[str, Any]) -> "StructureRecord":
        return StructureRecord(
            stationID=int(obj["stationID"]),
            station=obj.get("station"),
            stationType=obj.get("stationType"),
            solarSystem=obj.get("solarSystem"),
            dock=obj.get("dock"),
            market=bool(obj.get("market", True)),
            etag=obj.get("etag"),
        )


def read_structures_gz(path: str) -> Dict[int, StructureRecord]:
    records: Dict[int, StructureRecord] = {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                rec = StructureRecord.from_json_obj(obj)
                records[rec.stationID] = rec
    except FileNotFoundError:
        return {}
    return records


def canonical_hash_records(records: Dict[int, StructureRecord]) -> str:
    h = hashlib.sha256()
    for sid in sorted(records.keys()):
        line = json.dumps(records[sid].to_json_obj(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def write_structures_gz(path: str, records: Dict[int, StructureRecord]) -> None:
    """
    Sobrescritura atómica: escribe tmp y luego os.replace.
    No deja rastros de la versión anterior.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="tmp_structures_", suffix=".jsonl.gz", dir=os.path.dirname(path))
    os.close(fd)
    try:
        with open(tmp_path, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0, filename="structures.jsonl") as gz:
                for sid in sorted(records.keys()):
                    line = json.dumps(records[sid].to_json_obj(), separators=(",", ":"), ensure_ascii=False)
                    gz.write(line.encode("utf-8"))
                    gz.write(b"\n")
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def load_json_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def atomic_write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="tmp_", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


# ------------------------- SDE name resolution (on-demand) -------------------------

def resolve_names_on_demand(
    sde_solarsystems_path: str,
    sde_types_path: str,
    needed_solarsystems: Iterable[int],
    needed_types: Iterable[int],
) -> Tuple[Dict[int, str], Dict[int, str]]:
    """
    Resuelve únicamente IDs necesarios leyendo los .jsonl.gz del SDE.
    Evita cargar SDE completo en memoria.
    """
    need_ss = set(int(x) for x in needed_solarsystems if x is not None)
    need_ty = set(int(x) for x in needed_types if x is not None)

    ss_map: Dict[int, str] = {}
    ty_map: Dict[int, str] = {}

    if need_ss:
        with gzip.open(sde_solarsystems_path, "rt", encoding="utf-8") as f:
            for line in f:
                if not need_ss:
                    break
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                sid = int(obj["solarSystemID"])
                if sid in need_ss:
                    ss_map[sid] = str(obj["solarSystem"])
                    need_ss.discard(sid)

    if need_ty:
        with gzip.open(sde_types_path, "rt", encoding="utf-8") as f:
            for line in f:
                if not need_ty:
                    break
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                tid = int(obj["typeID"])
                if tid in need_ty:
                    ty_map[tid] = str(obj["type"])
                    need_ty.discard(tid)

    return ss_map, ty_map


# ------------------------- token selection (PRIMARY + fallback high->low) -------------------------

def select_access_tokens(primary_char_id: str, json1: str, json2: str) -> List[Tuple[str, str]]:
    def parse(j: str) -> Dict[str, str]:
        if not j:
            return {}
        try:
            obj = json.loads(j)
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items() if v}
        except Exception:
            return {}
        return {}

    tokens: Dict[str, str] = {}
    tokens.update(parse(json1))
    tokens.update(parse(json2))

    ordered: List[Tuple[str, str]] = []
    if primary_char_id in tokens:
        ordered.append((primary_char_id, tokens[primary_char_id]))

    others = [cid for cid in tokens.keys() if cid != primary_char_id]
    others.sort(key=lambda x: int(x), reverse=True)
    for cid in others:
        ordered.append((cid, tokens[cid]))

    return ordered


# ------------------------- SDE next run computation -------------------------

def compute_sde_next_run_epoch(limiter: RateLimiter, sde_url: str) -> Tuple[str, Optional[int]]:
    """
    Petición sin etag, sólo HEAD al ZIP.
    - 200: calcula SDE_NEXT_RUN a partir de Last-Modified hh:mm:ss.
    - 420: fail + now+5m
    - 5xx: fail + now+30m
    """
    st, hdr, _ = http_request(
        limiter,
        "HEAD",
        sde_url,
        headers={"User-Agent": "landu-eou/eou (EOU structures)"},
        timeout=60,
    )

    if st == 200:
        lm = hdr.get("last-modified")
        lm_epoch = parse_http_date_to_epoch(lm)
        if lm_epoch is None:
            return "err_other", None

        lm_dt = datetime.fromtimestamp(lm_epoch, tz=UTC)
        t = lm_dt.timetz().replace(tzinfo=UTC)

        now_dt = datetime.now(tz=UTC)
        candidate = now_dt.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
        if candidate <= now_dt:
            candidate = candidate + timedelta(days=1)

        jitter = random.SystemRandom().randint(300, 900)
        next_dt = candidate + timedelta(seconds=1800 + jitter)
        return "ok", int(next_dt.timestamp())

    if st == 420:
        return "err_420", None
    if st >= 500:
        return "err_5xx", None
    return "err_other", None


# ------------------------- BigQuery helpers (minimizar churn) -------------------------

def bq_table_exists(project_id: str, dataset: str, table: str) -> bool:
    table_ref = f"{dataset}.{table}"
    r = subprocess.run(
        ["bq", f"--project_id={project_id}", "show", "-t", table_ref],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return r.returncode == 0


def bq_create_clustered(project_id: str, dataset: str, table: str) -> None:
    table_ref = f"{dataset}.{table}"
    schema = "stationID:INTEGER,station:STRING,stationType:STRING,solarSystem:STRING,dock:BOOLEAN,market:BOOLEAN"
    subprocess.run(
        [
            "bq",
            f"--project_id={project_id}",
            "mk",
            "-t",
            f"--schema={schema}",
            "--clustering_fields=solarSystem",
            table_ref,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def bq_load_replace(project_id: str, dataset: str, table: str, ndjson_path: str) -> None:
    table_ref = f"{dataset}.{table}"
    subprocess.run(
        [
            "bq",
            f"--project_id={project_id}",
            "load",
            "--replace",
            "--source_format=NEWLINE_DELIMITED_JSON",
            table_ref,
            ndjson_path,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# ------------------------- main -------------------------

def main() -> int:
    # Outputs por defecto (para que el workflow pueda finalizar Sheets siempre)
    out_status = "failed"
    out_next = now_epoch() + 1800
    out_write_lm = "false"
    out_lm_epoch: Optional[int] = None
    repo_dirty = False

    # Estado de fases relevantes para la condición strict state update
    stage_data = SKIPPED
    stage_bq = SKIPPED

    def publish_outputs() -> None:
        gha_set_output("status", out_status)
        gha_set_output("next_run_epoch", str(out_next))
        gha_set_output("write_last_modified", out_write_lm)
        gha_set_output("last_modified_epoch", "" if out_lm_epoch is None else str(out_lm_epoch))
        gha_set_output("repo_dirty", "true" if repo_dirty else "false")

    try:
        # --- configuración base ---
        project_id = _env("GCP_PROJECT_ID")
        bq_dataset = _env("BQ_DATASET")
        bq_table = _env("BQ_TABLE")

        state_path = _env("STATE_ETAG_PATH")
        data_path = _env("DATA_STRUCTURES_PATH")
        sde_solarsystems_path = _env("SDE_SOLARSYSTEMS_PATH")
        sde_types_path = _env("SDE_TYPES_PATH")

        sde_url = _env("SDE_ZIP_URL")
        list_url = _env("ESI_STRUCTURES_LIST_URL")
        detail_tpl = _env("ESI_STRUCTURE_DETAIL_URL_TEMPLATE")

        primary_char_id = _env("PRIMARY_CHAR_ID")
        retry_budget = int(_env("RETRY_BUDGET"))

        max_rpm = int(_env("ESI_MAX_RPM", "60"))
        err_soft = int(_env("ESI_ERR_REMAIN_SOFT", "20"))
        err_hard = int(_env("ESI_ERR_REMAIN_HARD", "5"))

        limiter = RateLimiter(max_rpm=max_rpm)

        tokens = select_access_tokens(primary_char_id, _env("EOU_ACCESS_TOKENS_1"), _env("EOU_ACCESS_TOKENS_2"))
        if not tokens:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        ua = "landu-eou/eou (EOU structures; GitHub Actions)"

        # -----------------------------------------------------------------
        # Fase: SDE_NEXT_RUN
        # -----------------------------------------------------------------
        kind, sde_next = compute_sde_next_run_epoch(limiter, sde_url)
        if kind != "ok" or sde_next is None:
            out_status = "failed"
            if kind == "err_420":
                out_next = now_epoch() + 300
            else:
                out_next = now_epoch() + 1800
            out_write_lm = "false"
            out_lm_epoch = None
            publish_outputs()
            return 1

        # -----------------------------------------------------------------
        # Fase: listado de estructuras (ETag global en states/structures.json)
        # -----------------------------------------------------------------
        old_state = load_json_file(state_path) or {}
        old_etag_norm = normalize_etag(old_state.get("etag") if isinstance(old_state.get("etag"), str) else None)

        list_headers = {"User-Agent": ua, "Accept": "application/json"}
        inm = etag_to_if_none_match(old_etag_norm)
        if inm:
            list_headers["If-None-Match"] = inm

        st, hdr, body = http_request(limiter, "GET", list_url, headers=list_headers, timeout=120)
        limiter.observe_error_limit_headers(hdr, soft=err_soft, hard=err_hard)

        out_lm_epoch = parse_http_date_to_epoch(hdr.get("last-modified"))
        out_write_lm = "true" if out_lm_epoch is not None else "false"

        new_etag_norm = normalize_etag(hdr.get("etag"))

        if st == 304:
            out_status = "completed"
            out_next = sde_next
            publish_outputs()
            return 0

        if st == 420:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            return 1

        if st >= 500:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        if st != 200:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        try:
            structure_list: List[int] = json.loads(body.decode("utf-8"))
        except Exception:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        # -----------------------------------------------------------------
        # Fase: reconciliación en memoria del cache .jsonl.gz
        # -----------------------------------------------------------------
        old_records = read_structures_gz(data_path)

        list_set = set(int(x) for x in structure_list)
        temp_records: Dict[int, StructureRecord] = {sid: rec for sid, rec in old_records.items() if sid in list_set}
        for sid in list_set:
            if sid not in temp_records:
                temp_records[sid] = StructureRecord(
                    stationID=sid,
                    station=None,
                    stationType=None,
                    solarSystem=None,
                    dock=None,
                    market=True,
                    etag=None,
                )

        # -----------------------------------------------------------------
        # Fase: enriquecimiento (authed + ETag por registro + retries globales 401/420)
        # -----------------------------------------------------------------
        token_idx = 0

        def current_token() -> str:
            nonlocal token_idx
            if token_idx >= len(tokens):
                token_idx = len(tokens) - 1
            return tokens[token_idx][1]

        def rotate_token() -> None:
            nonlocal token_idx
            if token_idx + 1 < len(tokens):
                token_idx += 1

        need_type_ids: List[int] = []
        need_ss_ids: List[int] = []
        fatal = False

        for sid in sorted(list(temp_records.keys())):
            rec = temp_records.get(sid)
            if rec is None:
                continue

            url = detail_tpl.format(structure_id=sid)
            req_headers = {"User-Agent": ua, "Accept": "application/json", "Authorization": f"Bearer {current_token()}"}
            rec_inm = etag_to_if_none_match(rec.etag)
            if rec_inm:
                req_headers["If-None-Match"] = rec_inm

            while True:
                st2, hdr2, b2 = http_request(limiter, "GET", url, headers=req_headers, timeout=60)
                limiter.observe_error_limit_headers(hdr2, soft=err_soft, hard=err_hard)

                # 420/429: backoff fuerte (además de la lógica de retry_budget para 420)
                if st2 in (420, 429):
                    limiter.backoff_on_420_or_429(hdr2)

                # 401: retry global con rotación de token
                if st2 == 401:
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    rotate_token()
                    req_headers["Authorization"] = f"Bearer {current_token()}"
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                # 420: retry global con backoff
                if st2 == 420:
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                if st2 == 304:
                    break

                if st2 == 200:
                    try:
                        obj = json.loads(b2.decode("utf-8"))
                    except Exception:
                        break

                    rec.station = str(obj.get("name")) if obj.get("name") is not None else rec.station
                    rec.market = True
                    rec.dock = True

                    ssid = obj.get("solar_system_id")
                    tid = obj.get("type_id")
                    rec._solar_system_id = int(ssid) if ssid is not None else None
                    rec._type_id = int(tid) if tid is not None else None

                    if rec._solar_system_id is not None:
                        need_ss_ids.append(rec._solar_system_id)
                    if rec._type_id is not None:
                        need_type_ids.append(rec._type_id)

                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 403:
                    rec.station = None
                    rec.stationType = None
                    rec.solarSystem = None
                    rec.dock = None
                    rec.market = True
                    rec._solar_system_id = None
                    rec._type_id = None
                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 404:
                    temp_records.pop(sid, None)
                    break

                if st2 >= 500:
                    sleep_with_jitter(5, jitter_max=5)
                    break

                break

            if fatal:
                break

        if fatal:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            return 1

        # -----------------------------------------------------------------
        # Fase: resolver nombres SDE (on-demand) y completar stationType/solarSystem
        # -----------------------------------------------------------------
        ss_map, ty_map = resolve_names_on_demand(
            sde_solarsystems_path=sde_solarsystems_path,
            sde_types_path=sde_types_path,
            needed_solarsystems=need_ss_ids,
            needed_types=need_type_ids,
        )

        for r in temp_records.values():
            if r._solar_system_id is not None:
                r.solarSystem = ss_map.get(r._solar_system_id, r.solarSystem)
            if r._type_id is not None:
                r.stationType = ty_map.get(r._type_id, r.stationType)
            r._solar_system_id = None
            r._type_id = None

        # -----------------------------------------------------------------
        # Fase: detección de cambios (data + BQ) para evitar trabajo/cuotas
        # -----------------------------------------------------------------
        def rows_for_bq(records: Dict[int, StructureRecord]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for sid in sorted(records.keys()):
                r = records[sid]
                if r.station is None and r.solarSystem is None and r.dock is None:
                    continue
                if r.station is None or r.stationType is None or r.solarSystem is None or r.dock is None:
                    continue
                rows.append(
                    {
                        "stationID": r.stationID,
                        "station": r.station,
                        "stationType": r.stationType,
                        "solarSystem": r.solarSystem,
                        "dock": bool(r.dock),
                        "market": bool(r.market),
                    }
                )
            return rows

        def hash_rows(rows: List[Dict[str, Any]]) -> str:
            h = hashlib.sha256()
            for row in rows:
                line = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                h.update(line.encode("utf-8"))
                h.update(b"\n")
            return h.hexdigest()

        old_data_hash = canonical_hash_records(old_records)
        new_data_hash = canonical_hash_records(temp_records)
        data_changed = old_data_hash != new_data_hash

        old_bq_rows = rows_for_bq(old_records)
        new_bq_rows = rows_for_bq(temp_records)
        bq_changed = hash_rows(old_bq_rows) != hash_rows(new_bq_rows)

        # -----------------------------------------------------------------
        # Fase: BigQuery rewrite (solo si cambió y hay filas válidas)
        # -----------------------------------------------------------------
        stage_bq = SKIPPED
        if bq_changed and len(new_bq_rows) > 0:
            stage_bq = FAILED
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", delete=False, prefix="eou_structures_", suffix=".jsonl"
            ) as tmpf:
                ndjson_path = tmpf.name
                for row in new_bq_rows:
                    tmpf.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    tmpf.write("\n")

            try:
                if not bq_table_exists(project_id, bq_dataset, bq_table):
                    bq_create_clustered(project_id, bq_dataset, bq_table)
                bq_load_replace(project_id, bq_dataset, bq_table, ndjson_path)
                stage_bq = DONE_OK
            except Exception:
                stage_bq = FAILED
            finally:
                try:
                    os.remove(ndjson_path)
                except Exception:
                    pass

            if stage_bq == FAILED:
                out_status = "failed"
                out_next = now_epoch() + 1800
                publish_outputs()
                return 1

        # -----------------------------------------------------------------
        # Fase: escribir data/esi/structures.jsonl.gz (solo si cambió)
        # -----------------------------------------------------------------
        stage_data = SKIPPED
        if data_changed:
            stage_data = FAILED
            try:
                write_structures_gz(data_path, temp_records)
                repo_dirty = True
                stage_data = DONE_OK
            except Exception:
                stage_data = FAILED
                out_status = "failed"
                out_next = now_epoch() + 1800
                publish_outputs()
                return 1

        # -----------------------------------------------------------------
        # Strict state update: states/structures.json (etag global del listado)
        # -----------------------------------------------------------------
        etag_changed = bool(new_etag_norm) and (new_etag_norm != old_etag_norm)

        # STRICT: actualizar sólo si cambió etag y (data/bq no fallaron).
        # SKIPPED significa "no correspondía reescribir", no es error.
        if etag_changed and stage_data != FAILED and stage_bq != FAILED:
            try:
                atomic_write_text(
                    state_path,
                    json.dumps({"etag": str(new_etag_norm)}, ensure_ascii=False, separators=(",", ":")) + "\n",
                )
                repo_dirty = True
            except Exception:
                out_status = "failed"
                out_next = now_epoch() + 1800
                publish_outputs()
                return 1

        # -----------------------------------------------------------------
        # Fase final
        # -----------------------------------------------------------------
        out_status = "completed"
        out_next = sde_next
        publish_outputs()
        return 0

    except Exception:
        out_status = "failed"
        out_next = now_epoch() + 1800
        publish_outputs()
        return 1


if __name__ == "__main__":
    sys.exit(main())
