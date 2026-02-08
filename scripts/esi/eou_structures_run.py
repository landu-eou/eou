#!/usr/bin/env python3
"""EOU · ESI Structures (ESI → GH → BQ)

Inputs via env vars (as required by workflow):
  - GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE
  - STATE_ETAG_PATH, DATA_STRUCTURES_PATH
  - SDE_SOLARSYSTEMS_PATH, SDE_TYPES_PATH
  - SDE_ZIP_URL
  - ESI_STRUCTURES_LIST_URL
  - ESI_STRUCTURE_DETAIL_URL_TEMPLATE
  - PRIMARY_CHAR_ID
  - RETRY_BUDGET
  - EOU_ACCESS_TOKENS_1, EOU_ACCESS_TOKENS_2 (JSON: {char_id: access_token})

Outputs (to $GITHUB_OUTPUT):
  - status: completed|failed
  - next_run_epoch: int
  - write_last_modified: true|false
  - last_modified_epoch: int|"" (optional)
  - repo_dirty: true|false

Implementation-only fixes (no logic changes):
  - Response headers stored as lowercase keys (HTTP headers are case-insensitive),
    fixing missed ETag/Last-Modified when coming as "Etag"/"Last-modified".
  - Gzip internal filename forced to "structures.jsonl".
  - JSONL key order forced exactly per spec (no sort_keys in output).
  - ETag normalization + robust If-None-Match formatting.
  - Print traceback on unexpected exceptions for debuggability.

Refs:
  - HTTP field names are case-insensitive (RFC 9110 §5.1). :contentReference[oaicite:3]{index=3}
  - ESI best practices: ETag + If-None-Match and Last-Modified. :contentReference[oaicite:4]{index=4}
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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


UTC = timezone.utc


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
        print(f"::notice::{key}={value}")
        return
    value = value.replace("\n", " ").replace("\r", " ")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


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


def now_epoch() -> int:
    return int(datetime.now(tz=UTC).timestamp())


def sleep_with_jitter(seconds: int, jitter_max: int = 3) -> None:
    if seconds <= 0:
        return
    jitter = random.SystemRandom().randint(0, jitter_max)
    time.sleep(seconds + jitter)


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
            # IMPORTANT: store headers in lower-case keys (HTTP header names are case-insensitive)
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            body = resp.read() if method != "HEAD" else b""
            return status, hdrs, body
    except urllib.error.HTTPError as e:
        hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        body = e.read() if method != "HEAD" else b""
        return int(e.code), hdrs, body
    except Exception as e:
        # Network/timeout -> treat as 503-like
        return 503, {}, (str(e).encode("utf-8")[:200])


def maybe_throttle_on_error_limit(headers: Dict[str, str]) -> None:
    # Best-practice: respect X-ESI-Error-Limit-* headers to avoid bans.
    remain = headers.get("x-esi-error-limit-remain")
    reset = headers.get("x-esi-error-limit-reset")
    try:
        if remain is None or reset is None:
            return
        remain_i = int(remain)
        reset_i = int(reset)
        if remain_i < 5 and reset_i > 0:
            sleep_with_jitter(reset_i + 1, jitter_max=5)
    except Exception:
        return


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


def normalize_etag(etag: Optional[str]) -> Optional[str]:
    """Normalize for compare/store:
    - strip spaces
    - remove weak prefix W/
    - remove surrounding quotes
    """
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
    """Build a robust If-None-Match value from stored etag (which may be unquoted)."""
    n = normalize_etag(stored_etag)
    if not n:
        return None
    return f"\"{n}\""


@dataclasses.dataclass
class StructureRecord:
    stationID: int
    station: Optional[str] = None
    stationType: Optional[str] = None
    solarSystem: Optional[str] = None
    dock: Optional[bool] = None
    market: bool = True
    etag: Optional[str] = None

    def to_json_obj(self) -> Dict[str, Any]:
        # ORDER REQUIRED BY SPEC (do not change)
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
    # Hash must be stable even if field order changes in future.
    h = hashlib.sha256()
    for sid in sorted(records.keys()):
        obj = records[sid].to_json_obj()
        line = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def write_structures_gz(path: str, records: Dict[int, StructureRecord]) -> None:
    """Write gzip with:
    - internal filename = structures.jsonl
    - JSON keys in required order
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="tmp_structures_", suffix=".jsonl.gz", dir=os.path.dirname(path))
    os.close(fd)

    try:
        with open(tmp_path, "wb") as raw:
            with gzip.GzipFile(
                fileobj=raw,
                mode="wb",
                compresslevel=9,
                mtime=0,
                filename="structures.jsonl",
            ) as gz:
                for sid in sorted(records.keys()):
                    line = json.dumps(
                        records[sid].to_json_obj(),
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    gz.write(line.encode("utf-8"))
                    gz.write(b"\n")

        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def load_sde_solarsystems(path: str) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sid = int(obj["solarSystemID"])
            mapping[sid] = str(obj["solarSystem"])
    return mapping


def load_sde_types(path: str) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            tid = int(obj["typeID"])
            mapping[tid] = str(obj["type"])
    return mapping


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
    maybe_throttle_on_error_limit(headers)

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


def run_bq_rewrite(project_id: str, dataset: str, table: str, ndjson_path: str) -> None:
    table_ref = f"{dataset}.{table}"
    schema = "stationID:INTEGER,station:STRING,stationType:STRING,solarSystem:STRING,dock:BOOLEAN,market:BOOLEAN"

    subprocess.run(
        ["bq", f"--project_id={project_id}", "rm", "-f", "-t", table_ref],
        check=False,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

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
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    subprocess.run(
        [
            "bq",
            f"--project_id={project_id}",
            "load",
            "--source_format=NEWLINE_DELIMITED_JSON",
            table_ref,
            ndjson_path,
        ],
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def main() -> int:
    out_status = "failed"
    out_next = now_epoch() + 1800
    out_write_lm = "false"
    out_lm_epoch: Optional[int] = None
    repo_dirty = False

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

        tokens_1 = _env("EOU_ACCESS_TOKENS_1")
        tokens_2 = _env("EOU_ACCESS_TOKENS_2")
        tokens = select_access_tokens(primary_char_id, tokens_1, tokens_2)
        if not tokens:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        ua = "landu-eou/eou (EOU structures; GitHub Actions)"

        # ------------------------------------------------------------
        # Fase inicio: SDE_NEXT_RUN
        # ------------------------------------------------------------
        kind, sde_next = compute_sde_next_run_epoch(sde_url)
        if kind != "ok" or sde_next is None:
            out_status = "failed"
            if kind == "err_420":
                out_next = now_epoch() + 300
            elif kind == "err_5xx":
                out_next = now_epoch() + 1800
            else:
                out_next = now_epoch() + 1800
            out_write_lm = "false"
            out_lm_epoch = None
            publish_outputs()
            return 1

        # ------------------------------------------------------------
        # Fase listado de estructuras (ETag global en states/structures.json)
        # ------------------------------------------------------------
        old_state = load_json_file(state_path) or {}
        old_list_etag_raw = old_state.get("etag")
        old_list_etag_norm = normalize_etag(old_list_etag_raw if isinstance(old_list_etag_raw, str) else None)

        list_headers = {
            "User-Agent": ua,
            "Accept": "application/json",
        }
        inm = etag_to_if_none_match(old_list_etag_raw if isinstance(old_list_etag_raw, str) else None)
        if inm:
            list_headers["If-None-Match"] = inm

        status, headers, body = http_request("GET", list_url, headers=list_headers, timeout=120)
        maybe_throttle_on_error_limit(headers)

        list_last_modified = headers.get("last-modified")
        out_lm_epoch = parse_http_date_to_epoch(list_last_modified)
        out_write_lm = "true" if out_lm_epoch is not None else "false"

        if status == 304:
            out_status = "completed"
            out_next = sde_next
            publish_outputs()
            return 0

        if status == 420:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            return 1

        if status >= 500:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        if status != 200:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        new_list_etag_raw = headers.get("etag")
        new_list_etag_norm = normalize_etag(new_list_etag_raw)

        try:
            structure_list: List[int] = json.loads(body.decode("utf-8"))
        except Exception:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

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

        # ------------------------------------------------------------
        # Fase enriquecimiento
        # ------------------------------------------------------------
        solarsystems = load_sde_solarsystems(sde_solarsystems_path)
        types = load_sde_types(sde_types_path)

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

        fatal_unauth = False

        for sid in sorted(temp_records.keys()):
            rec = temp_records.get(sid)
            if rec is None:
                continue

            url = detail_tpl.format(structure_id=sid)
            req_headers = {
                "User-Agent": ua,
                "Accept": "application/json",
                "Authorization": f"Bearer {current_token()}",
            }
            rec_inm = etag_to_if_none_match(rec.etag)
            if rec_inm:
                req_headers["If-None-Match"] = rec_inm

            while True:
                st, hdr, b = http_request("GET", url, headers=req_headers, timeout=60)
                maybe_throttle_on_error_limit(hdr)

                if st == 401:
                    if retry_budget <= 0:
                        fatal_unauth = True
                        break
                    retry_budget -= 1
                    rotate_token()
                    req_headers["Authorization"] = f"Bearer {current_token()}"
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                if st == 420:
                    if retry_budget <= 0:
                        fatal_unauth = True
                        break
                    retry_budget -= 1
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                if st == 304:
                    break

                if st == 200:
                    try:
                        obj = json.loads(b.decode("utf-8"))
                    except Exception:
                        break

                    name = obj.get("name")
                    solar_system_id = obj.get("solar_system_id")
                    type_id = obj.get("type_id")

                    rec.station = str(name) if name is not None else rec.station
                    rec.market = True
                    rec.dock = True

                    if solar_system_id is not None:
                        rec.solarSystem = solarsystems.get(int(solar_system_id))

                    if type_id is not None:
                        rec.stationType = types.get(int(type_id))

                    rec.etag = normalize_etag(hdr.get("etag") or rec.etag) or rec.etag
                    break

                if st == 403:
                    rec.station = None
                    rec.stationType = None
                    rec.solarSystem = None
                    rec.dock = None
                    rec.market = True
                    rec.etag = normalize_etag(hdr.get("etag") or rec.etag) or rec.etag
                    break

                if st == 404:
                    temp_records.pop(sid, None)
                    break

                if st >= 500:
                    break

                break

            if fatal_unauth:
                break

        if fatal_unauth:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            return 1

        # ------------------------------------------------------------
        # Fase volcados (data file + BQ) — en una sola operación cada uno
        # ------------------------------------------------------------
        old_data_hash = canonical_hash_records(old_records)
        new_data_hash = canonical_hash_records(temp_records)
        data_changed = old_data_hash != new_data_hash

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

        old_bq_rows = rows_for_bq(old_records)
        new_bq_rows = rows_for_bq(temp_records)

        def hash_rows(rows: List[Dict[str, Any]]) -> str:
            h = hashlib.sha256()
            for row in rows:
                line = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                h.update(line.encode("utf-8"))
                h.update(b"\n")
            return h.hexdigest()

        bq_changed = hash_rows(old_bq_rows) != hash_rows(new_bq_rows)
        bq_should_rewrite = bq_changed and len(new_bq_rows) > 0

        bq_ok_or_skipped = True
        if bq_should_rewrite:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", delete=False, prefix="eou_structures_", suffix=".jsonl"
            ) as tmpf:
                ndjson_path = tmpf.name
                for row in new_bq_rows:
                    tmpf.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    tmpf.write("\n")
            try:
                run_bq_rewrite(project_id, bq_dataset, bq_table, ndjson_path)
            except Exception:
                bq_ok_or_skipped = False
            finally:
                try:
                    os.remove(ndjson_path)
                except Exception:
                    pass

        if not bq_ok_or_skipped:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        data_ok_or_skipped = True
        if data_changed:
            try:
                write_structures_gz(data_path, temp_records)
                repo_dirty = True
            except Exception:
                data_ok_or_skipped = False

        if not data_ok_or_skipped:
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            return 1

        # ------------------------------------------------------------
        # Update states/structures.json (STRICT condition requested)
        # ------------------------------------------------------------
        etag_changed = bool(new_list_etag_norm) and (new_list_etag_norm != old_list_etag_norm)
        if etag_changed and data_ok_or_skipped and bq_ok_or_skipped:
            try:
                atomic_write_text(
                    state_path,
                    json.dumps({"etag": str(new_list_etag_norm)}, ensure_ascii=False, separators=(",", ":")) + "\n",
                )
                repo_dirty = True
            except Exception:
                out_status = "failed"
                out_next = now_epoch() + 1800
                publish_outputs()
                return 1

        # ------------------------------------------------------------
        # Fase final
        # ------------------------------------------------------------
        out_status = "completed"
        out_next = sde_next
        publish_outputs()
        return 0

    except Exception:
        # Critical for debugging "exit code 1" without changing business logic.
        traceback.print_exc()
        out_status = "failed"
        out_next = now_epoch() + 1800
        publish_outputs()
        return 1


if __name__ == "__main__":
    sys.exit(main())
