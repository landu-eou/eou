#!/usr/bin/env python3
"""
EOU · ESI Structures (ESI → GH → BQ)

Contrato (inputs por env vars):
  - GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE
  - STATE_ETAG_PATH, DATA_STRUCTURES_PATH
  - SDE_SOLARSYSTEMS_PATH, SDE_TYPES_PATH
  - SDE_ZIP_URL
  - ESI_STRUCTURES_LIST_URL
  - ESI_STRUCTURE_DETAIL_URL_TEMPLATE
  - PRIMARY_CHAR_ID
  - RETRY_BUDGET
  - EOU_ACCESS_TOKENS_1, EOU_ACCESS_TOKENS_2 (JSON {char_id: access_token})
  - ESI_BASE_RPM, ESI_MIN_RPM, ESI_SOFT_REMAIN  (pacing operacional)

Outputs (a $GITHUB_OUTPUT) — MISMO CONTRATO:
  - status: completed|failed
  - next_run_epoch: int
  - write_last_modified: true|false
  - last_modified_epoch: int|"" (optional)
  - repo_dirty: true|false

Observabilidad:
  - Escribe un informe en $GITHUB_STEP_SUMMARY (sin artifacts).
  - Mantiene estados internos explícitos por etapa:
      SKIPPED_BY_DESIGN | DONE_OK | FAILED

Sostenibilidad ESI:
  - Rate limiter: máximo N req/min (BASE_RPM), mínimo MIN_RPM.
  - Pausa dinámica por X-ESI-Error-Limit-Remain/Reset y degradación de RPM.
  - Objetivo operacional: evitar 420 (error limiting). (ESI best practices) :contentReference[oaicite:6]{index=6}
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
import traceback
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

UTC = timezone.utc

STAGE_SKIPPED = "SKIPPED_BY_DESIGN"
STAGE_OK = "DONE_OK"
STAGE_FAILED = "FAILED"


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise KeyError(f"Missing required env var: {name}")
        return default
    return v


def gha_set_output(key: str, value: str) -> None:
    path = os.getenv("GITHUB_OUTPUT")
    if not path:
        return
    value = value.replace("\n", " ").replace("\r", " ")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def write_step_summary(md: str) -> None:
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(md)
        if not md.endswith("\n"):
            f.write("\n")


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


def sleep_with_jitter(seconds: float, jitter_max: float = 1.5) -> None:
    if seconds <= 0:
        return
    j = random.SystemRandom().random() * jitter_max
    time.sleep(seconds + j)


def http_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Tuple[int, Dict[str, str], bytes]:
    req = urllib.request.Request(url=url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", resp.getcode()))
            hdrs = {k.lower(): v for k, v in resp.headers.items()}  # case-insensitive
            body = resp.read() if method != "HEAD" else b""
            return status, hdrs, body
    except urllib.error.HTTPError as e:
        hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        body = e.read() if method != "HEAD" else b""
        return int(e.code), hdrs, body
    except Exception as e:
        # Network/timeout -> treat as 503-like
        return 503, {}, (str(e).encode("utf-8")[:200])


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


@dataclass
class RateLimiter:
    """Simple pacing: enforce minimum interval + dynamic slowdowns."""
    base_rpm: int
    min_rpm: int
    soft_remain: int

    current_rpm: int = 60
    _min_interval: float = 1.0
    _next_allowed: float = 0.0
    min_remain_seen: Optional[int] = None
    last_reset_seen: Optional[int] = None
    degraded: bool = False

    def __post_init__(self) -> None:
        self.current_rpm = max(self.min_rpm, self.base_rpm)
        self._min_interval = 60.0 / max(1, self.current_rpm)
        self._next_allowed = time.monotonic()

    def _set_rpm(self, rpm: int) -> None:
        rpm = max(self.min_rpm, min(self.base_rpm, rpm))
        if rpm != self.current_rpm:
            self.current_rpm = rpm
            self._min_interval = 60.0 / max(1, self.current_rpm)

    def pre_request(self) -> None:
        now = time.monotonic()
        if now < self._next_allowed:
            time.sleep(self._next_allowed - now)
        # reserve the next slot
        self._next_allowed = time.monotonic() + self._min_interval

    def observe_headers(self, headers: Dict[str, str], was_error: bool) -> None:
        # ESI error limit headers (case-insensitive) :contentReference[oaicite:7]{index=7}
        remain_s = headers.get("x-esi-error-limit-remain")
        reset_s = headers.get("x-esi-error-limit-reset")

        remain = None
        reset = None
        try:
            if remain_s is not None:
                remain = int(remain_s)
                self.min_remain_seen = remain if self.min_remain_seen is None else min(self.min_remain_seen, remain)
            if reset_s is not None:
                reset = int(reset_s)
                self.last_reset_seen = reset
        except Exception:
            remain = None
            reset = None

        # Si bajamos de soft_remain, degradamos rpm hacia min_rpm gradualmente.
        if remain is not None and remain < self.soft_remain:
            self.degraded = True
            # cuanto más bajo el remain, más frenamos
            # e.g. remain 14 -> rpm 50; remain 5 -> rpm 25; remain 0 -> rpm 20
            target = int(self.min_rpm + (self.base_rpm - self.min_rpm) * (max(0, remain) / max(1, self.soft_remain)))
            self._set_rpm(max(self.min_rpm, target))

        # Si la respuesta fue error (4xx/5xx), es señal de riesgo: baja un poco.
        if was_error:
            self.degraded = True
            self._set_rpm(max(self.min_rpm, int(self.current_rpm * 0.85)))

        # Si estamos peligrosamente cerca (remain < 5), pausa hasta reset.
        if remain is not None and reset is not None and remain < 5 and reset > 0:
            # backoff fuerte para evitar 420
            sleep_with_jitter(reset + 1, jitter_max=3.0)

    def observe_420(self) -> None:
        # 420 => ya estamos limitados; backoff aún más fuerte
        self.degraded = True
        self._set_rpm(self.min_rpm)


@dataclasses.dataclass
class StructureRecord:
    stationID: int
    station: Optional[str] = None
    stationType: Optional[str] = None
    solarSystem: Optional[str] = None
    dock: Optional[bool] = None
    market: bool = True
    etag: Optional[str] = None

    # internos (no se serializan)
    _typeID: Optional[int] = None
    _solarSystemID: Optional[int] = None

    def to_json_obj(self) -> Dict[str, Any]:
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
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="tmp_structures_", suffix=".jsonl.gz", dir=os.path.dirname(path))
    os.close(fd)
    try:
        with open(tmp_path, "wb") as raw:
            with gzip.GzipFile(
                fileobj=raw, mode="wb", compresslevel=9, mtime=0, filename="structures.jsonl"
            ) as gz:
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


def compute_sde_next_run_epoch(sde_url: str) -> Tuple[str, Optional[int]]:
    status, headers, _ = http_request("HEAD", sde_url, headers={"User-Agent": "landu-eou/eou (EOU structures)"})
    if status == 200:
        lm = headers.get("last-modified")
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

    if status == 420:
        return "err_420", None
    if status >= 500:
        return "err_5xx", None
    return "err_other", None


def stream_sde_map_on_demand(gz_path: str, key_field: str, val_field: str, wanted: Iterable[int]) -> Dict[int, str]:
    wanted_set = set(int(x) for x in wanted)
    if not wanted_set:
        return {}
    out: Dict[int, str] = {}
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            k = int(obj[key_field])
            if k in wanted_set:
                out[k] = str(obj[val_field])
                if len(out) == len(wanted_set):
                    break
    return out


def run_bq_rewrite(project_id: str, dataset: str, table: str, ndjson_path: str) -> None:
    table_ref = f"{dataset}.{table}"
    schema = "stationID:INTEGER,station:STRING,stationType:STRING,solarSystem:STRING,dock:BOOLEAN,market:BOOLEAN"

    subprocess.run(["bq", f"--project_id={project_id}", "rm", "-f", "-t", table_ref], check=False)
    subprocess.run(
        ["bq", f"--project_id={project_id}", "mk", "-t", f"--schema={schema}", "--clustering_fields=solarSystem", table_ref],
        check=True,
    )
    subprocess.run(
        ["bq", f"--project_id={project_id}", "load", "--source_format=NEWLINE_DELIMITED_JSON", table_ref, ndjson_path],
        check=True,
    )


def main() -> int:
    # Outputs requeridos por workflow (mismo contrato)
    out_status = "failed"
    out_next = now_epoch() + 1800
    out_write_lm = "false"
    out_lm_epoch: Optional[int] = None
    repo_dirty = False

    # Estados internos explícitos
    stage_data = STAGE_SKIPPED
    stage_bq = STAGE_SKIPPED
    stage_state = STAGE_SKIPPED

    # Métricas
    t0 = time.monotonic()
    phase_time: Dict[str, float] = {}
    list_status = None
    list_etag_old = None
    list_etag_new = None
    list_count = 0
    detail_codes = Counter()
    retries_401 = 0
    retries_420 = 0
    token_rotations = 0

    def publish_outputs() -> None:
        gha_set_output("status", out_status)
        gha_set_output("next_run_epoch", str(out_next))
        gha_set_output("write_last_modified", out_write_lm)
        gha_set_output("last_modified_epoch", "" if out_lm_epoch is None else str(out_lm_epoch))
        gha_set_output("repo_dirty", "true" if repo_dirty else "false")

    try:
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

        base_rpm = int(_env("ESI_BASE_RPM", "60"))
        min_rpm = int(_env("ESI_MIN_RPM", "20"))
        soft_remain = int(_env("ESI_SOFT_REMAIN", "15"))
        limiter = RateLimiter(base_rpm=base_rpm, min_rpm=min_rpm, soft_remain=soft_remain)

        tokens = select_access_tokens(primary_char_id, _env("EOU_ACCESS_TOKENS_1"), _env("EOU_ACCESS_TOKENS_2"))
        if not tokens:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        ua = "landu-eou/eou (EOU structures; GitHub Actions)"

        # ---- Fase inicio: SDE_NEXT_RUN
        p = time.monotonic()
        kind, sde_next = compute_sde_next_run_epoch(sde_url)
        phase_time["SDE_HEAD"] = time.monotonic() - p

        if kind != "ok" or sde_next is None:
            out_status = "failed"
            out_next = now_epoch() + (300 if kind == "err_420" else 1800)
            out_write_lm = "false"
            out_lm_epoch = None
            publish_outputs()
            return 1

        # ---- Listado de estructuras (ETag global)
        p = time.monotonic()
        old_state = load_json_file(state_path) or {}
        list_etag_old = normalize_etag(old_state.get("etag") if isinstance(old_state.get("etag"), str) else None)

        headers = {"User-Agent": ua, "Accept": "application/json"}
        inm = etag_to_if_none_match(list_etag_old)
        if inm:
            headers["If-None-Match"] = inm

        limiter.pre_request()
        st, hdr, body = http_request("GET", list_url, headers=headers, timeout=120)
        list_status = st

        is_error = st >= 400
        limiter.observe_headers(hdr, was_error=is_error)

        lm = hdr.get("last-modified")
        out_lm_epoch = parse_http_date_to_epoch(lm)
        out_write_lm = "true" if out_lm_epoch is not None else "false"

        if st == 304:
            out_status = "completed"
            out_next = sde_next
            phase_time["LIST"] = time.monotonic() - p
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 0

        if st == 420:
            limiter.observe_420()
            out_status = "failed"
            out_next = now_epoch() + 300
            phase_time["LIST"] = time.monotonic() - p
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        if st >= 500:
            out_status = "failed"
            out_next = now_epoch() + 1800
            phase_time["LIST"] = time.monotonic() - p
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        if st != 200:
            out_status = "failed"
            out_next = now_epoch() + 1800
            phase_time["LIST"] = time.monotonic() - p
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        list_etag_new = normalize_etag(hdr.get("etag"))
        try:
            structure_list: List[int] = json.loads(body.decode("utf-8"))
        except Exception:
            out_status = "failed"
            out_next = now_epoch() + 1800
            phase_time["LIST"] = time.monotonic() - p
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        list_count = len(structure_list)
        phase_time["LIST"] = time.monotonic() - p

        # ---- Reconciliación en memoria
        p = time.monotonic()
        old_records = read_structures_gz(data_path)
        list_set = set(int(x) for x in structure_list)

        temp_records: Dict[int, StructureRecord] = {sid: rec for sid, rec in old_records.items() if sid in list_set}
        for sid in list_set:
            if sid not in temp_records:
                temp_records[sid] = StructureRecord(stationID=sid, market=True)

        phase_time["RECONCILE"] = time.monotonic() - p

        # ---- Enriquecimiento (con pacing)
        p = time.monotonic()
        token_idx = 0

        def current_token() -> str:
            nonlocal token_idx
            if token_idx >= len(tokens):
                token_idx = len(tokens) - 1
            return tokens[token_idx][1]

        def rotate_token() -> None:
            nonlocal token_idx, token_rotations
            if token_idx + 1 < len(tokens):
                token_idx += 1
                token_rotations += 1

        fatal = False

        for sid in sorted(temp_records.keys()):
            rec = temp_records.get(sid)
            if rec is None:
                continue

            url = detail_tpl.format(structure_id=sid)
            h = {"User-Agent": ua, "Accept": "application/json", "Authorization": f"Bearer {current_token()}"}
            inm2 = etag_to_if_none_match(rec.etag)
            if inm2:
                h["If-None-Match"] = inm2

            while True:
                limiter.pre_request()
                st2, hdr2, b2 = http_request("GET", url, headers=h, timeout=60)

                detail_codes[st2] += 1
                limiter.observe_headers(hdr2, was_error=(st2 >= 400))

                if st2 == 401:
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    retries_401 += 1
                    rotate_token()
                    h["Authorization"] = f"Bearer {current_token()}"
                    sleep_with_jitter(30, jitter_max=3.0)
                    continue

                if st2 == 420:
                    limiter.observe_420()
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    retries_420 += 1
                    sleep_with_jitter(45, jitter_max=5.0)  # más conservador
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

                    # Guardamos IDs; resolveremos nombres on-demand después
                    ssid = obj.get("solar_system_id")
                    tid = obj.get("type_id")
                    rec._solarSystemID = int(ssid) if ssid is not None else rec._solarSystemID
                    rec._typeID = int(tid) if tid is not None else rec._typeID

                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 403:
                    rec.station = None
                    rec.stationType = None
                    rec.solarSystem = None
                    rec.dock = None
                    rec.market = True
                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 404:
                    temp_records.pop(sid, None)
                    break

                # 5xx => mantener registro actual para este run
                break

            if fatal:
                break

        phase_time["DETAILS"] = time.monotonic() - p

        if fatal:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        # ---- Resolver nombres on-demand (SDE)
        p = time.monotonic()
        needed_ss = {r._solarSystemID for r in temp_records.values() if r._solarSystemID is not None and not r.solarSystem}
        needed_ty = {r._typeID for r in temp_records.values() if r._typeID is not None and not r.stationType}

        ss_map = stream_sde_map_on_demand(sde_solarsystems_path, "solarSystemID", "solarSystem", needed_ss)
        ty_map = stream_sde_map_on_demand(sde_types_path, "typeID", "type", needed_ty)

        for r in temp_records.values():
            if r._solarSystemID is not None and not r.solarSystem:
                r.solarSystem = ss_map.get(r._solarSystemID)
            if r._typeID is not None and not r.stationType:
                r.stationType = ty_map.get(r._typeID)

        phase_time["SDE_RESOLVE"] = time.monotonic() - p

        # ---- Volcados: data + BQ
        p = time.monotonic()
        old_data_hash = canonical_hash_records(old_records)
        new_data_hash = canonical_hash_records(temp_records)
        data_changed = old_data_hash != new_data_hash

        def rows_for_bq(records: Dict[int, StructureRecord]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for sid in sorted(records.keys()):
                r = records[sid]
                # regla: solo volcamos si hay info real
                if r.station is None and r.solarSystem is None and r.dock is None:
                    continue
                # tabla exige not null en esas columnas
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

        old_bq = rows_for_bq(old_records)
        new_bq = rows_for_bq(temp_records)

        def hash_rows(rows: List[Dict[str, Any]]) -> str:
            h = hashlib.sha256()
            for row in rows:
                h.update(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
                h.update(b"\n")
            return h.hexdigest()

        bq_changed = hash_rows(old_bq) != hash_rows(new_bq)
        bq_should_rewrite = bq_changed and len(new_bq) > 0

        # BQ
        if bq_should_rewrite:
            stage_bq = STAGE_FAILED
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, prefix="eou_structures_", suffix=".jsonl") as tmpf:
                ndjson_path = tmpf.name
                for row in new_bq:
                    tmpf.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    tmpf.write("\n")
            try:
                run_bq_rewrite(project_id, bq_dataset, bq_table, ndjson_path)
                stage_bq = STAGE_OK
            finally:
                try:
                    os.remove(ndjson_path)
                except Exception:
                    pass
        else:
            stage_bq = STAGE_SKIPPED

        if stage_bq == STAGE_FAILED:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            phase_time["DUMPS"] = time.monotonic() - p
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        # DATA
        if data_changed:
            stage_data = STAGE_FAILED
            try:
                write_structures_gz(data_path, temp_records)
                repo_dirty = True
                stage_data = STAGE_OK
            except Exception:
                stage_data = STAGE_FAILED
        else:
            stage_data = STAGE_SKIPPED

        if stage_data == STAGE_FAILED:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            phase_time["DUMPS"] = time.monotonic() - p
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        # ---- Update states/structures.json (STRICT):
        # etag cambió AND (data no FAILED) AND (bq no FAILED)
        etag_changed = bool(list_etag_new) and (list_etag_new != list_etag_old)
        if etag_changed and stage_data != STAGE_FAILED and stage_bq != STAGE_FAILED:
            stage_state = STAGE_FAILED
            try:
                atomic_write_text(state_path, json.dumps({"etag": str(list_etag_new)}, ensure_ascii=False, separators=(",", ":")) + "\n")
                repo_dirty = True
                stage_state = STAGE_OK
            except Exception:
                stage_state = STAGE_FAILED
        else:
            stage_state = STAGE_SKIPPED

        if stage_state == STAGE_FAILED:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            phase_time["DUMPS"] = time.monotonic() - p
            _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
            return 1

        phase_time["DUMPS"] = time.monotonic() - p

        # ---- Final OK
        out_status = "completed"
        out_next = sde_next
        publish_outputs()
        _write_summary(phase_time, limiter, list_status, list_count, detail_codes, retries_401, retries_420,
                       token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
        return 0

    except Exception:
        traceback.print_exc()
        out_status = "failed"
        out_next = now_epoch() + 1800
        publish_outputs()
        # best-effort summary
        try:
            _write_summary(phase_time, None, list_status, list_count, detail_codes, retries_401, retries_420,
                           token_rotations, stage_data, stage_bq, stage_state, list_etag_old, list_etag_new)
        except Exception:
            pass
        return 1


def _write_summary(
    phase_time: Dict[str, float],
    limiter: Optional[RateLimiter],
    list_status: Optional[int],
    list_count: int,
    detail_codes: Counter,
    retries_401: int,
    retries_420: int,
    token_rotations: int,
    stage_data: str,
    stage_bq: str,
    stage_state: str,
    etag_old: Optional[str],
    etag_new: Optional[str],
) -> None:
    # Job summaries oficiales: escribir markdown a $GITHUB_STEP_SUMMARY :contentReference[oaicite:8]{index=8}
    lines: List[str] = []
    lines.append("## ESI Structures · Run Summary\n")

    lines.append("### Outcome\n")
    lines.append(f"- List status: `{list_status}`\n")
    lines.append(f"- Data stage: `{stage_data}`\n")
    lines.append(f"- BQ stage: `{stage_bq}`\n")
    lines.append(f"- State(etag) stage: `{stage_state}`\n")

    lines.append("\n### ETag (list)\n")
    lines.append(f"- Old: `{etag_old}`\n")
    lines.append(f"- New: `{etag_new}`\n")

    lines.append("\n### Counts\n")
    lines.append(f"- Structures in list: **{list_count}**\n")
    if detail_codes:
        top = sorted(detail_codes.items(), key=lambda kv: (-kv[1], kv[0]))
        lines.append("- Detail HTTP codes:\n")
        for code, n in top:
            lines.append(f"  - `{code}`: {n}\n")
    lines.append(f"- Retries used: 401={retries_401}, 420={retries_420}\n")
    lines.append(f"- Token rotations: {token_rotations}\n")

    lines.append("\n### Pacing (operational)\n")
    if limiter is None:
        lines.append("- (no limiter info)\n")
    else:
        lines.append(f"- Base RPM: {limiter.base_rpm}, Min RPM: {limiter.min_rpm}, Soft remain: {limiter.soft_remain}\n")
        lines.append(f"- Current RPM: **{limiter.current_rpm}** (degraded={limiter.degraded})\n")
        lines.append(f"- Min remain seen: {limiter.min_remain_seen}\n")
        lines.append(f"- Last reset seen (s): {limiter.last_reset_seen}\n")
        lines.append("\n> Policy: enforce min interval = 60/RPM; if `X-ESI-Error-Limit-Remain` drops, slow down and/or sleep until reset to avoid 420.\n")

    lines.append("\n### Timings (s)\n")
    for k in ["SDE_HEAD", "LIST", "RECONCILE", "DETAILS", "SDE_RESOLVE", "DUMPS"]:
        if k in phase_time:
            lines.append(f"- {k}: {phase_time[k]:.2f}\n")

    write_step_summary("".join(lines))


if __name__ == "__main__":
    sys.exit(main())
