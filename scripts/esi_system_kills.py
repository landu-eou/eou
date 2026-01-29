#!/usr/bin/env python3
import json
import os
import sys
import time
import gzip
import subprocess
import urllib.request
from email.utils import parsedate_to_datetime

# -----------------------------
# Helpers
# -----------------------------
def now_epoch() -> int:
    return int(time.time())

def write_github_env(key: str, value: str) -> None:
    path = os.environ.get("GITHUB_ENV")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")

def run(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr

def sh(cmd: list[str], check: bool = True) -> str:
    rc, out, err = run(cmd)
    if check and rc != 0:
        raise RuntimeError(f"Command failed ({rc}): {' '.join(cmd)}\n{err}")
    return out

def parse_headers_file(path: str) -> dict[str, str]:
    h: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip("\r\n")
                if not line or line.lower().startswith("http/"):
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    h[k.strip().lower()] = v.strip()
    except FileNotFoundError:
        pass
    return h

def chunks(lst: list[int], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

# -----------------------------
# Env
# -----------------------------
WORKFLOW_ID = os.environ["WORKFLOW_ID"]
ESI_URL = os.environ["ESI_URL"]
STATE_PATH = os.environ["STATE_PATH"]
SDE_MAP_PATH = os.environ["SDE_MAP_PATH"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_TABLE"]
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
EXPIRES_FALLBACK = safe_int(os.environ.get("EXPIRES", "86400"), 86400)

# Default: backoff 10 minutes unless we compute Expires+60
next_run_epoch = now_epoch() + 600
write_github_env("NEXT_RUN_EPOCH", str(next_run_epoch))

# -----------------------------
# Read prior committed state
# -----------------------------
etag_prev = ""
last_mod_prev = ""
if os.path.exists(STATE_PATH):
    try:
        st = json.load(open(STATE_PATH, "r", encoding="utf-8"))
        lc = st.get("lastCommitted") or {}
        etag_prev = (lc.get("etag") or "").strip()
        last_mod_prev = (lc.get("lastModifiedRfc1123") or "").strip()
    except Exception:
        pass

# -----------------------------
# Fetch ESI (curl already ran in workflow)
# We expect files: headers.txt, body.json, and env HTTP_CODE
# -----------------------------
http_code = os.environ.get("HTTP_CODE", "").strip()
headers = parse_headers_file("headers.txt")

etag_new = headers.get("etag", "").strip()
last_mod_rfc = headers.get("last-modified", "").strip()
expires_rfc = headers.get("expires", "").strip()
retry_after = headers.get("retry-after", "").strip()
err_reset = headers.get("x-esi-error-limit-reset", "").strip()
err_limited = headers.get("x-esi-error-limited", "").strip()

parsed = {
    "http_code": http_code,
    "etag": etag_new,
    "last_modified_rfc1123": last_mod_rfc,
    "expires_rfc1123": expires_rfc,
    "ts_iso": "",
    "early_exit": True,
    "next_run_epoch": next_run_epoch,
    "reason": "",
}

def compute_next_run_from_expires() -> int:
    # next_run = Expires(header) + 60s; fallback = now + EXPIRES_FALLBACK + 60s
    nr = now_epoch() + EXPIRES_FALLBACK + 60
    if expires_rfc:
        try:
            nr = int(parsedate_to_datetime(expires_rfc).timestamp()) + 60
        except Exception:
            pass
    return nr

# Handle rate limit / 420
if http_code == "420" or (err_limited.lower() == "true"):
    wait = 600
    for v in (retry_after, err_reset):
        try:
            if v:
                wait = max(wait, int(float(v)))
        except Exception:
            pass
    parsed["reason"] = "error_limited"
    parsed["next_run_epoch"] = now_epoch() + wait
    write_github_env("NEXT_RUN_EPOCH", str(parsed["next_run_epoch"]))
    with open("parsed.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
    sys.exit(0)

# No changes
if http_code == "304":
    parsed["reason"] = "not_modified_304"
    with open("parsed.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
    sys.exit(0)

# Any non-200
if http_code != "200":
    parsed["reason"] = "http_not_200"
    with open("parsed.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
    sys.exit(0)

# 200 but missing Last-Modified
if not last_mod_rfc:
    parsed["reason"] = "missing_last_modified"
    with open("parsed.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
    sys.exit(0)

# Same as state (ETag + Last-Modified)
if etag_new and last_mod_rfc and etag_new == etag_prev and last_mod_rfc == last_mod_prev:
    parsed["reason"] = "same_as_state"
    with open("parsed.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
    sys.exit(0)

# New data -> proceed
ts_iso = parsedate_to_datetime(last_mod_rfc).astimezone().isoformat().replace("+00:00", "Z")
parsed["ts_iso"] = ts_iso
parsed["early_exit"] = False
parsed["reason"] = "ok_new_data"
parsed["next_run_epoch"] = compute_next_run_from_expires()
write_github_env("NEXT_RUN_EPOCH", str(parsed["next_run_epoch"]))

with open("parsed.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")

# -----------------------------
# Ensure BQ table exists
# -----------------------------
if not GCP_PROJECT_ID:
    raise RuntimeError("Missing GCP_PROJECT_ID env")

full_table = f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"
rc, _, _ = run(["bq", "show", "--format=prettyjson", full_table])
if rc != 0:
    # Create partitioned table (ts partition)
    sh([
        "bq", "mk", "--table",
        "--time_partitioning_type=DAY",
        "--time_partitioning_field=ts",
        full_table,
        "ts:TIMESTAMP,solarSystem:STRING,npc_kills:INTEGER,pod_kills:INTEGER,ship_kills:INTEGER"
    ], check=True)

# -----------------------------
# Dedupe guard: ts exists?
# -----------------------------
query = (
    f"SELECT COUNT(1) c "
    f"FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` "
    f"WHERE ts=@ts"
)
out = sh([
    "bq", "query",
    "--use_legacy_sql=false",
    "--format=csv",
    "--parameter=ts:TIMESTAMP:" + ts_iso,
    query
], check=True)

lines = [x.strip("\r") for x in out.strip().split("\n") if x.strip()]
count = 0
if len(lines) >= 2:
    count = safe_int(lines[-1], 0)

def write_state_new():
    doc = {
        "schemaVersion": 1,
        "workflowId": WORKFLOW_ID,
        "endpoint": ESI_URL,
        "bq": {"dataset": BQ_DATASET, "table": BQ_TABLE},
        "lastCommitted": {
            "etag": etag_new,
            "lastModifiedRfc1123": last_mod_rfc,
            "expiresRfc1123": expires_rfc,
            "committedAtUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open("state_new.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(doc, ensure_ascii=False, indent=2) + "\n")

if count > 0:
    # fulfilled: already loaded previously but state drifted
    write_state_new()
    sys.exit(0)

# -----------------------------
# Transform body.json -> out.ndjson with solarSystem mapping
# -----------------------------
data = json.load(open("body.json", "r", encoding="utf-8"))

mapping: dict[int, str] = {}
with gzip.open(SDE_MAP_PATH, "rt", encoding="utf-8") as f:
    for line in f:
        o = json.loads(line)
        mapping[int(o["solarSystemId"])] = o["solarSystem"]

missing_ids = [int(x["system_id"]) for x in data if int(x["system_id"]) not in mapping]

# Resolve missing via /universe/names best-effort
if missing_ids:
    for ch in chunks(missing_ids, 500):
        req = urllib.request.Request(
            "https://esi.evetech.net/latest/universe/names/?datasource=tranquility",
            data=json.dumps(ch).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "landu-eou/eou (GitHub Actions)",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                arr = json.loads(resp.read().decode("utf-8"))
                for it in arr:
                    if it.get("category") == "solar_system" and "id" in it and "name" in it:
                        mapping[int(it["id"])] = it["name"]
        except Exception:
            pass

with open("out.ndjson", "w", encoding="utf-8") as out_f:
    for row in data:
        sid = int(row["system_id"])
        out = {
            "ts": ts_iso,
            "solarSystem": mapping.get(sid, str(sid)),
            "npc_kills": safe_int(row.get("npc_kills", 0), 0),
            "pod_kills": safe_int(row.get("pod_kills", 0), 0),
            "ship_kills": safe_int(row.get("ship_kills", 0), 0),
        }
        out_f.write(json.dumps(out, ensure_ascii=False) + "\n")

# -----------------------------
# Load to BQ (append)
# -----------------------------
sh([
    "bq", "load",
    "--quiet",
    "--source_format=NEWLINE_DELIMITED_JSON",
    f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
    "out.ndjson"
], check=True)

# fulfilled: load OK -> write state_new.json
write_state_new()
sys.exit(0)
