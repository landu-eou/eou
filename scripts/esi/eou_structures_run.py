#!/usr/bin/env python3
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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

UTC = timezone.utc


# ---------------------------
# Small utilities
# ---------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise KeyError(f"Missing required env var: {name}")
        return default
    return v


def now_epoch() -> int:
    return int(datetime.now(tz=UTC).timestamp())


def gha_set_output(key: str, value: str) -> None:
    path = os.getenv("GITHUB_OUTPUT")
    if not path:
        print(f"::notice::{key}={value}")
        return
    value = value.replace("\n", " ").replace("\r", " ")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def summary_append(md: str) -> None:
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(md)
        if not md.endswith("\n"):
            f.write("\n")


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


def sleep_ms(ms: int) -> None:
    if ms <= 0:
        return
    time.sleep(ms / 1000.0)


def sleep_with_jitter(seconds: int, jitter_max: int = 3) -> None:
    if seconds <= 0:
        return
    jitter = random.SystemRandom().randint(0, jitter_max)
    time.sleep(seconds + jitter)


# ---------------------------
# ETag handling
# ---------------------------

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


# ---------------------------
# Rate limiter (operational change)
# ---------------------------

@dataclasses.dataclass
class EsiRatePolicy:
    max_rpm: int
    min_sleep_ms: int
    burst: int
    soft_err_remain: int
    hard_err_remain: int


class TokenBucket:
    """Simple token bucket limiter:
    - capacity = burst
    - refill rate = max_rpm / 60 tokens per second
    """
    def __init__(self, max_rpm: int, burst: int) -> None:
        self.max_rpm = max(1, max_rpm)
        self.capacity = max(1, burst)
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        dt = now - self.updated
        self.updated = now
        refill_per_sec = self.max_rpm / 60.0
        self.tokens = min(self.capacity, self.tokens + dt * refill_per_sec)

    def acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # Sleep a bit and retry
            sleep_ms(250)


class EsiLimiter:
    def __init__(self, policy: EsiRatePolicy) -> None:
        self.policy = policy
        self.bucket = TokenBucket(policy.max_rpm, policy.burst)
        self.min_err_remain_seen: Optional[int] = None
        self.last_err_reset_seen: Optional[int] = None
        self.dynamic_slowdown_ms: int = 0

    def before_request(self) -> None:
        self.bucket.acquire()
        # Always apply a small base delay to avoid tight loops (smoother traffic)
        sleep_ms(self.policy.min_sleep_ms + self.dynamic_slowdown_ms)

    def observe_headers(self, headers_lc: Dict[str, str]) -> None:
        remain_s = headers_lc.get("x-esi-error-limit-remain")
        reset_s = headers_lc.get("x-esi-error-limit-reset")
        try:
            remain = int(remain_s) if remain_s is not None else None
            reset = int(reset_s) if reset_s is not None else None
        except Exception:
            remain = None
            reset = None

        if remain is not None:
            if self.min_err_remain_seen is None or remain < self.min_err_remain_seen:
                self.min_err_remain_seen = remain

            # Soft slowdown to avoid approaching 420:
            # if remain <= soft threshold, increase per-request delay.
            if remain <= self.policy.soft_err_remain:
                # ramp up delay as remain approaches hard threshold
                # remain=soft -> +200ms, remain=hard -> +1200ms (approx)
                span = max(1, self.policy.soft_err_remain - self.policy.hard_err_remain)
                x = max(0, self.policy.soft_err_remain - remain)
                extra = int(200 + (1000 * (x / span)))
                self.dynamic_slowdown_ms = min(2000, extra)

            # Hard protection: if remain <= hard threshold, sleep until reset
            if remain <= self.policy.hard_err_remain and reset and reset > 0:
                self.last_err_reset_seen = reset
                sleep_with_jitter(reset + 1, jitter_max=5)
                # After reset, clear slowdown a bit
                self.dynamic_slowdown_ms = min(self.dynamic_slowdown_ms, 300)

        if reset is not None:
            self.last_err_reset_seen = reset


# ---------------------------
# HTTP
# ---------------------------

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
            hdrs = {k.lower(): v for k, v in resp.headers.items()}  # case-insensitive safe
            body = resp.read() if method != "HEAD" else b""
            return status, hdrs, body
    except urllib.error.HTTPError as e:
        hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        body = e.read() if method != "HEAD" else b""
        return int(e.code), hdrs, body
    except Exception as e:
        return 503, {}, (str(e).encode("utf-8")[:200])


# ---------------------------
# Data model
# ---------------------------

@dataclasses.dataclass
class StructureRecord:
    stationID: int
    station: Optional[str] = None
    stationType: Optional[str] = None      # resolved name (SDE type)
    solarSystem: Optional[str] = None      # resolved name (SDE system)
    dock: Optional[bool] = None
    market: bool = True
    etag: Optional[str] = None

    # internal (not exported)
    _type_id: Optional[int] = None
    _solar_system_id: Optional[int] = None

    def to_json_obj(self) -> Dict[str, Any]:
        # Keep key order per spec (json.dumps preserves insertion order)
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


# ---------------------------
# Repo IO
# ---------------------------

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


def read_structures_gz(path: str) -> Dict[int, StructureRecord]:
    out: Dict[int, StructureRecord] = {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                rec = StructureRecord.from_json_obj(obj)
                out[rec.stationID] = rec
    except FileNotFoundError:
        return {}
    return out


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
                fileobj=raw,
                mode="wb",
                compresslevel=9,
                mtime=0,
                filename="structures.jsonl",
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


# ---------------------------
# SDE on-demand loaders
# ---------------------------

def load_sde_solarsystems_subset(path: str, needed: Set[int]) -> Dict[int, str]:
    if not needed:
        return {}
    out: Dict[int, str] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sid = int(obj["solarSystemID"])
            if sid in needed:
                out[sid] = str(obj["solarSystem"])
                if len(out) == len(needed):
                    break
    return out


def load_sde_types_subset(path: str, needed: Set[int]) -> Dict[int, str]:
    if not needed:
        return {}
    out: Dict[int, str] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            tid = int(obj["typeID"])
            if tid in needed:
                out[tid] = str(obj["type"])
                if len(out) == len(needed):
                    break
    return out


# ---------------------------
# Token selection
# ---------------------------

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


# ---------------------------
# SDE next run
# ---------------------------

def compute_sde_next_run_epoch(sde_url: str) -> Tuple[str, Optional[int]]:
    # Request without ETag as requested
    status, headers, _ = http_request("HEAD", sde_url, headers={"User-Agent": "landu-eou/eou (EOU structures)"})
    if status == 200:
        lm = headers.get("last-modified")
        lm_epoch = parse_http_date_to_epoch(lm)
        if lm_epoch is None:
            return "FAILED", None

        lm_dt = datetime.fromtimestamp(lm_epoch, tz=UTC)
        t = lm_dt.timetz().replace(tzinfo=UTC)
        now_dt = datetime.now(tz=UTC)

        candidate = now_dt.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
        if candidate <= now_dt:
            candidate = candidate + timedelta(days=1)

        jitter = random.SystemRandom().randint(300, 900)
        next_dt = candidate + timedelta(seconds=1800 + jitter)
        return "DONE_OK", int(next_dt.timestamp())

    if status == 420:
        return "FAILED_420", None
    if status >= 500:
        return "FAILED_5XX", None
    return "FAILED", None


# ---------------------------
# BigQuery rewrite (keep behaviour)
# ---------------------------

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
        ["bq", f"--project_id={project_id}", "mk", "-t", f"--schema={schema}", "--clustering_fields=solarSystem", table_ref],
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    subprocess.run(
        ["bq", f"--project_id={project_id}", "load", "--source_format=NEWLINE_DELIMITED_JSON", table_ref, ndjson_path],
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


# ---------------------------
# Main
# ---------------------------

def main() -> int:
    # Workflow outputs contract (must exist)
    out_status = "failed"
    out_next = now_epoch() + 1800
    out_write_lm = "false"
    out_lm_epoch: Optional[int] = None
    repo_dirty = False

    # Explicit stage states requested
    stage = {
        "SDE_NEXT_RUN": "SKIPPED_BY_DESIGN",
        "LIST": "SKIPPED_BY_DESIGN",
        "ENRICH": "SKIPPED_BY_DESIGN",
        "DATA_WRITE": "SKIPPED_BY_DESIGN",
        "BQ_WRITE": "SKIPPED_BY_DESIGN",
        "STATE_ETAG_WRITE": "SKIPPED_BY_DESIGN",
    }

    metrics = Counter()
    timings: Dict[str, float] = {}

    def t0(name: str) -> None:
        timings[name] = time.monotonic()

    def t1(name: str) -> float:
        if name not in timings:
            return 0.0
        return max(0.0, time.monotonic() - timings[name])

    def publish_outputs() -> None:
        gha_set_output("status", out_status)
        gha_set_output("next_run_epoch", str(out_next))
        gha_set_output("write_last_modified", out_write_lm)
        gha_set_output("last_modified_epoch", "" if out_lm_epoch is None else str(out_lm_epoch))
        gha_set_output("repo_dirty", "true" if repo_dirty else "false")

    def render_summary(extra_error: Optional[str] = None) -> None:
        summary_append("# ESI Structures — Run Summary")
        summary_append(f"- UTC: `{datetime.now(tz=UTC).isoformat(timespec='seconds')}`")
        summary_append("")
        summary_append("## Stage states")
        for k in ["SDE_NEXT_RUN", "LIST", "ENRICH", "DATA_WRITE", "BQ_WRITE", "STATE_ETAG_WRITE"]:
            summary_append(f"- **{k}**: `{stage[k]}`")

        summary_append("")
        summary_append("## Key outcomes")
        summary_append(f"- Final status: **{out_status}**")
        summary_append(f"- Next run (epoch): `{out_next}`")
        if out_lm_epoch is not None:
            summary_append(f"- Last-Modified (epoch): `{out_lm_epoch}`")

        summary_append("")
        summary_append("## Metrics")
        # Common counters
        keys = [
            "list_http_200", "list_http_304", "list_http_420", "list_http_5xx",
            "detail_http_200", "detail_http_304", "detail_http_401", "detail_http_403",
            "detail_http_404", "detail_http_420", "detail_http_5xx",
            "retry_used_401", "retry_used_420",
            "structures_in_list", "structures_seen",
            "data_changed", "bq_changed", "bq_rows_written",
        ]
        for k in keys:
            if k in metrics:
                summary_append(f"- `{k}`: {metrics[k]}")

        summary_append("")
        summary_append("## ESI sustainability signals")
        min_rem = limiter.min_err_remain_seen
        last_reset = limiter.last_err_reset_seen
        summary_append(f"- Min `X-ESI-Error-Limit-Remain` seen: `{min_rem}`")
        summary_append(f"- Last `X-ESI-Error-Limit-Reset` seen: `{last_reset}`")
        summary_append(f"- Dynamic slowdown ms: `{limiter.dynamic_slowdown_ms}`")
        summary_append(f"- Policy max_rpm: `{policy.max_rpm}`, burst: `{policy.burst}`, min_sleep_ms: `{policy.min_sleep_ms}`")

        summary_append("")
        summary_append("## Timings (seconds)")
        for k in ["SDE", "LIST", "ENRICH", "SDE_LOOKUP", "DATA_WRITE", "BQ_WRITE"]:
            summary_append(f"- `{k}`: {t1(k):.2f}")

        if extra_error:
            summary_append("")
            summary_append("## Error")
            summary_append(f"- `{extra_error}`")

    try:
        # Read env
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

        policy = EsiRatePolicy(
            max_rpm=int(_env("ESI_MAX_RPM", "60")),
            min_sleep_ms=int(_env("ESI_MIN_SLEEP_MS", "150")),
            burst=int(_env("ESI_BURST", "5")),
            soft_err_remain=int(_env("ESI_SOFT_ERR_REMAIN", "15")),
            hard_err_remain=int(_env("ESI_HARD_ERR_REMAIN", "5")),
        )
        global limiter
        limiter = EsiLimiter(policy)

        tokens = select_access_tokens(primary_char_id, _env("EOU_ACCESS_TOKENS_1"), _env("EOU_ACCESS_TOKENS_2"))
        if not tokens:
            out_status = "failed"
            out_next = now_epoch() + 1800
            stage["SDE_NEXT_RUN"] = "FAILED"
            publish_outputs()
            render_summary("No access tokens available.")
            return 1

        ua = "landu-eou/eou (EOU structures; GitHub Actions)"

        # ---------------------------
        # Phase: SDE_NEXT_RUN
        # ---------------------------
        t0("SDE")
        kind, sde_next = compute_sde_next_run_epoch(sde_url)
        if kind == "DONE_OK" and sde_next is not None:
            stage["SDE_NEXT_RUN"] = "DONE_OK"
        else:
            stage["SDE_NEXT_RUN"] = "FAILED"
            out_status = "failed"
            if kind == "FAILED_420":
                out_next = now_epoch() + 300
            elif kind == "FAILED_5XX":
                out_next = now_epoch() + 1800
            else:
                out_next = now_epoch() + 1800
            out_write_lm = "false"
            out_lm_epoch = None
            publish_outputs()
            render_summary(f"SDE head request failed: {kind}")
            return 1

        # ---------------------------
        # Phase: LIST structures (If-None-Match from states/structures.json)
        # ---------------------------
        t0("LIST")
        old_state = load_json_file(state_path) or {}
        old_etag_norm = normalize_etag(old_state.get("etag") if isinstance(old_state.get("etag"), str) else None)

        list_headers = {"User-Agent": ua, "Accept": "application/json"}
        inm = etag_to_if_none_match(old_state.get("etag") if isinstance(old_state.get("etag"), str) else None)
        if inm:
            list_headers["If-None-Match"] = inm

        limiter.before_request()
        st, hdr, body = http_request("GET", list_url, headers=list_headers, timeout=120)
        limiter.observe_headers(hdr)

        lm = hdr.get("last-modified")
        out_lm_epoch = parse_http_date_to_epoch(lm)
        out_write_lm = "true" if out_lm_epoch is not None else "false"

        if st == 304:
            stage["LIST"] = "DONE_OK"
            metrics["list_http_304"] += 1
            out_status = "completed"
            out_next = int(sde_next)
            publish_outputs()
            render_summary()
            return 0

        if st == 200:
            metrics["list_http_200"] += 1
        elif st == 420:
            metrics["list_http_420"] += 1
            stage["LIST"] = "FAILED"
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            render_summary("List returned 420.")
            return 1
        elif st >= 500:
            metrics["list_http_5xx"] += 1
            stage["LIST"] = "FAILED"
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary("List returned 5xx.")
            return 1
        else:
            stage["LIST"] = "FAILED"
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary(f"List returned unexpected status {st}.")
            return 1

        stage["LIST"] = "DONE_OK"
        new_list_etag_norm = normalize_etag(hdr.get("etag"))
        try:
            structure_list: List[int] = json.loads(body.decode("utf-8"))
        except Exception:
            stage["LIST"] = "FAILED"
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary("Failed to parse list JSON.")
            return 1

        metrics["structures_in_list"] = len(structure_list)

        # ---------------------------
        # Phase: reconcile in-memory
        # ---------------------------
        old_records = read_structures_gz(data_path)
        list_set = set(int(x) for x in structure_list)

        temp_records: Dict[int, StructureRecord] = {sid: rec for sid, rec in old_records.items() if sid in list_set}
        for sid in list_set:
            if sid not in temp_records:
                temp_records[sid] = StructureRecord(
                    stationID=sid, station=None, stationType=None, solarSystem=None, dock=None, market=True, etag=None
                )

        # ---------------------------
        # Phase: ENRICH details with global retry budget 401/420
        # ---------------------------
        t0("ENRICH")
        stage["ENRICH"] = "DONE_OK"

        token_idx = 0

        def current_token() -> str:
            nonlocal token_idx
            token_idx = min(token_idx, len(tokens) - 1)
            return tokens[token_idx][1]

        def rotate_token() -> None:
            nonlocal token_idx
            if token_idx + 1 < len(tokens):
                token_idx += 1

        needed_type_ids: Set[int] = set()
        needed_system_ids: Set[int] = set()
        fatal = False

        for sid in sorted(list(temp_records.keys())):
            metrics["structures_seen"] += 1
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
                limiter.before_request()
                st2, hdr2, b2 = http_request("GET", url, headers=req_headers, timeout=60)
                limiter.observe_headers(hdr2)

                if st2 == 401:
                    metrics["detail_http_401"] += 1
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    metrics["retry_used_401"] += 1
                    rotate_token()
                    req_headers["Authorization"] = f"Bearer {current_token()}"
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                if st2 == 420:
                    metrics["detail_http_420"] += 1
                    if retry_budget <= 0:
                        fatal = True
                        break
                    retry_budget -= 1
                    metrics["retry_used_420"] += 1
                    sleep_with_jitter(30, jitter_max=5)
                    continue

                if st2 == 304:
                    metrics["detail_http_304"] += 1
                    break

                if st2 == 200:
                    metrics["detail_http_200"] += 1
                    try:
                        obj = json.loads(b2.decode("utf-8"))
                    except Exception:
                        break

                    rec.station = str(obj.get("name")) if obj.get("name") is not None else rec.station
                    rec.market = True
                    rec.dock = True

                    ssid = obj.get("solar_system_id")
                    tid = obj.get("type_id")
                    if ssid is not None:
                        rec._solar_system_id = int(ssid)
                        needed_system_ids.add(int(ssid))
                    if tid is not None:
                        rec._type_id = int(tid)
                        needed_type_ids.add(int(tid))

                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 403:
                    metrics["detail_http_403"] += 1
                    rec.station = None
                    rec.stationType = None
                    rec.solarSystem = None
                    rec.dock = None
                    rec.market = True
                    rec._type_id = None
                    rec._solar_system_id = None
                    rec.etag = normalize_etag(hdr2.get("etag") or rec.etag) or rec.etag
                    break

                if st2 == 404:
                    metrics["detail_http_404"] += 1
                    temp_records.pop(sid, None)
                    break

                if st2 >= 500:
                    metrics["detail_http_5xx"] += 1
                    # keep record as-is for this run
                    break

                # other statuses: keep record
                break

            if fatal:
                stage["ENRICH"] = "FAILED"
                break

        if fatal:
            out_status = "failed"
            out_next = now_epoch() + 300
            publish_outputs()
            render_summary("Fatal: retry budget exhausted due to 401/420.")
            return 1

        # ---------------------------
        # Phase: SDE lookups on-demand (types + solarsystems)
        # ---------------------------
        t0("SDE_LOOKUP")
        sol_map = load_sde_solarsystems_subset(sde_solarsystems_path, needed_system_ids)
        type_map = load_sde_types_subset(sde_types_path, needed_type_ids)

        # Fill resolved names (only for records that have IDs)
        for rec in temp_records.values():
            if rec._solar_system_id is not None and rec.solarSystem is None:
                rec.solarSystem = sol_map.get(rec._solar_system_id)
            if rec._type_id is not None and rec.stationType is None:
                rec.stationType = type_map.get(rec._type_id)

        # ---------------------------
        # Phase: DATA + BQ decisions (same behaviour)
        # ---------------------------
        old_data_hash = canonical_hash_records(old_records)
        new_data_hash = canonical_hash_records(temp_records)
        data_changed = old_data_hash != new_data_hash
        metrics["data_changed"] = 1 if data_changed else 0

        def rows_for_bq(records: Dict[int, StructureRecord]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for sid in sorted(records.keys()):
                r = records[sid]
                # keep rule: only export if some fields not null and the "complete" rule holds
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
        metrics["bq_changed"] = 1 if bq_changed else 0

        bq_should_rewrite = bq_changed and len(new_bq_rows) > 0

        # BQ write
        t0("BQ_WRITE")
        if bq_should_rewrite:
            stage["BQ_WRITE"] = "DONE_OK"
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, prefix="eou_structures_", suffix=".jsonl") as tmpf:
                ndjson_path = tmpf.name
                for row in new_bq_rows:
                    tmpf.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    tmpf.write("\n")
            try:
                run_bq_rewrite(project_id, bq_dataset, bq_table, ndjson_path)
                metrics["bq_rows_written"] = len(new_bq_rows)
            except Exception:
                stage["BQ_WRITE"] = "FAILED"
            finally:
                try:
                    os.remove(ndjson_path)
                except Exception:
                    pass
        else:
            stage["BQ_WRITE"] = "SKIPPED_BY_DESIGN"

        if stage["BQ_WRITE"] == "FAILED":
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary("BigQuery rewrite failed.")
            return 1

        # Data write
        t0("DATA_WRITE")
        if data_changed:
            try:
                write_structures_gz(data_path, temp_records)
                repo_dirty = True
                stage["DATA_WRITE"] = "DONE_OK"
            except Exception:
                stage["DATA_WRITE"] = "FAILED"
        else:
            stage["DATA_WRITE"] = "SKIPPED_BY_DESIGN"

        if stage["DATA_WRITE"] == "FAILED":
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary("Data file write failed.")
            return 1

        # Update state etag STRICT:
        # - etag changed
        # - DATA_WRITE != FAILED (DONE_OK or SKIPPED_BY_DESIGN)
        # - BQ_WRITE != FAILED (DONE_OK or SKIPPED_BY_DESIGN)
        etag_changed = bool(new_list_etag_norm) and (new_list_etag_norm != old_etag_norm)
        if etag_changed and stage["DATA_WRITE"] != "FAILED" and stage["BQ_WRITE"] != "FAILED":
            try:
                atomic_write_text(state_path, json.dumps({"etag": str(new_list_etag_norm)}, separators=(",", ":")) + "\n")
                repo_dirty = True
                stage["STATE_ETAG_WRITE"] = "DONE_OK"
            except Exception:
                stage["STATE_ETAG_WRITE"] = "FAILED"
        else:
            stage["STATE_ETAG_WRITE"] = "SKIPPED_BY_DESIGN"

        if stage["STATE_ETAG_WRITE"] == "FAILED":
            out_status = "failed"
            out_next = now_epoch() + 1800
            publish_outputs()
            render_summary("Failed to update states/structures.json.")
            return 1

        # Final
        out_status = "completed"
        out_next = int(sde_next)
        publish_outputs()
        render_summary()
        return 0

    except Exception as e:
        traceback.print_exc()
        out_status = "failed"
        out_next = now_epoch() + 1800
        publish_outputs()
        try:
            render_summary(f"Unhandled exception: {type(e).__name__}")
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
