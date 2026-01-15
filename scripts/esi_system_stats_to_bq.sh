echo "SCRIPT VERSION: $(git rev-parse --short HEAD 2>/dev/null || echo no-git)"

#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR at line $LINENO";' ERR


: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${BQ_DATASET:=eou}"
: "${STATE_FILE:=.orch/state.jsonl}"
: "${ESI_BASE_URL:=https://esi.evetech.net/latest}"
: "${ESI_DATASOURCE:=tranquility}"
: "${USER_AGENT:?Missing USER_AGENT}"

FORCE="${FORCE:-false}"
DRY_RUN="${DRY_RUN:-false}"

mkdir -p "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

workflow_ref="${GITHUB_WORKFLOW_REF:-unknown}"
workflow_yaml="$(echo "$workflow_ref" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"
now_iso="$(date -u +"%Y-%m-%d %H:%M:%S+00:00")"
now_epoch="$(date -u +%s)"

# ---- read state line 1 (ESI) and line 2 (refresh) ----
line1="$(sed -n '1p' "$STATE_FILE" || true)"
line2="$(sed -n '2p' "$STATE_FILE" || true)"

echo "STATE_FILE=$STATE_FILE"
echo "STATE line1 (raw): $(printf '%q' "$line1")"
echo "STATE line2 (raw): $(printf '%q' "$line2")"

echo "STATE line1 first bytes (hex):"
printf "%s" "$line1" | head -c 16 | xxd -p || true

# Fallback si faltan líneas
[[ -n "$line1" ]] || line1='{}'
[[ -n "$line2" ]] || line2='{"type":"BQ_REFRESH_STATE"}'

# Fallback si el JSON está corrupto (BOM, basura, etc.)
if ! echo "$line1" | jq -e . >/dev/null 2>&1; then
  echo "ERROR: state line1 is not valid JSON; falling back to {}"
  line1='{}'
fi
if ! echo "$line2" | jq -e . >/dev/null 2>&1; then
  echo "ERROR: state line2 is not valid JSON; falling back to default"
  line2='{"type":"BQ_REFRESH_STATE"}'
fi

old_etag_jumps="$(echo "$line1" | jq -r '.etag_jumps // ""' 2>/dev/null || echo "")"
old_etag_kills="$(echo "$line1" | jq -r '.etag_kills // ""' 2>/dev/null || echo "")"
next_eligible="$(echo "$line1" | jq -r '.next_eligible_run_at // ""' 2>/dev/null || echo "")"


if [[ "${FORCE}" != "true" && -n "$next_eligible" && "$next_eligible" != "null" ]]; then
  next_epoch="$(date -u -d "$next_eligible" +%s 2>/dev/null || echo 0)"
  if (( now_epoch < next_epoch )); then
    echo "Too early. next_eligible_run_at=$next_eligible (now=$now_iso). Exiting without ESI calls."
    exit 0
  fi
fi

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir"; else cleanup; fi; exit $rc' EXIT

hdr_get() {
  local file="$1" key="$2"
  # returns last match (some responses include multiple header blocks on redirects etc.)
  grep -i "^${key}:" "$file" | tail -n 1 | cut -d: -f2- | tr -d '\r' | xargs || true
}

fetch_one() {
  local name="$1" url="$2" etag="$3"
  local hdr="$tmpdir/${name}.hdr"
  local body="$tmpdir/${name}.json"
  local code_file="$tmpdir/${name}.code"

  local -a curl_args
  curl_args=(
    -sS --compressed
    -H "Accept: application/json"
    -H "User-Agent: ${USER_AGENT}"
    -D "$hdr"
    -o "$body"
    -w "%{http_code}"
  )
  if [[ -n "$etag" ]]; then
    curl_args+=(-H "If-None-Match: ${etag}")
  fi

  local code
  code="$(curl "${curl_args[@]}" "$url" || true)"
  echo "$code" > "$code_file"
}

# ---- ESI endpoints ----
url_jumps="${ESI_BASE_URL}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"
url_kills="${ESI_BASE_URL}/universe/system_kills/?datasource=${ESI_DATASOURCE}"

fetch_one "jumps" "$url_jumps" "$old_etag_jumps"
fetch_one "kills" "$url_kills" "$old_etag_kills"

code_jumps="$(cat "$tmpdir/jumps.code")"
code_kills="$(cat "$tmpdir/kills.code")"

# ---- DEBUG/VALIDATION: ensure responses are valid JSON arrays before jq transforms ----
echo "---- Debug headers (first 30 lines) ----"
echo "[jumps headers]"; head -n 30 "$tmpdir/jumps.hdr" || true
echo "[kills headers]"; head -n 30 "$tmpdir/kills.hdr" || true
echo "----------------------------------------"

# If 200, validate JSON and type
if [[ "$code_jumps" == "200" ]]; then
  if ! jq -e . "$tmpdir/jumps.json" >/dev/null 2>&1; then
    echo "ERROR: jumps body is not valid JSON. First 300 bytes:"
    head -c 300 "$tmpdir/jumps.json" | sed 's/[^[:print:]\t]/?/g'
    exit 1
  fi
  if ! jq -e 'type=="array"' "$tmpdir/jumps.json" >/dev/null 2>&1; then
    echo "ERROR: jumps JSON is not an array. Body (first 400 chars):"
    jq -c . "$tmpdir/jumps.json" | head -c 400
    echo
    exit 1
  fi
fi

if [[ "$code_kills" == "200" ]]; then
  if ! jq -e . "$tmpdir/kills.json" >/dev/null 2>&1; then
    echo "ERROR: kills body is not valid JSON. First 300 bytes:"
    head -c 300 "$tmpdir/kills.json" | sed 's/[^[:print:]\t]/?/g'
    exit 1
  fi
  if ! jq -e 'type=="array"' "$tmpdir/kills.json" >/dev/null 2>&1; then
    echo "ERROR: kills JSON is not an array. Body (first 400 chars):"
    jq -c . "$tmpdir/kills.json" | head -c 400
    echo
    exit 1
  fi
fi

if [[ ! "$code_jumps" =~ ^(200|304)$ ]]; then
  echo "ESI jumps returned HTTP $code_jumps — aborting to avoid burning error budget."
  exit 1
fi
if [[ ! "$code_kills" =~ ^(200|304)$ ]]; then
  echo "ESI kills returned HTTP $code_kills — aborting to avoid burning error budget."
  exit 1
fi

# Error limit headers (log only; if you want, you can hard-stop when remain is low)
err_remain_j="$(hdr_get "$tmpdir/jumps.hdr" "X-ESI-Error-Limit-Remain")"
err_reset_j="$(hdr_get "$tmpdir/jumps.hdr" "X-ESI-Error-Limit-Reset")"
err_remain_k="$(hdr_get "$tmpdir/kills.hdr" "X-ESI-Error-Limit-Remain")"
err_reset_k="$(hdr_get "$tmpdir/kills.hdr" "X-ESI-Error-Limit-Reset")"
echo "ESI error budget (jumps): remain=${err_remain_j:-?}, reset_s=${err_reset_j:-?}"
echo "ESI error budget (kills): remain=${err_remain_k:-?}, reset_s=${err_reset_k:-?}"

etag_jumps_new="$(hdr_get "$tmpdir/jumps.hdr" "ETag")"
etag_kills_new="$(hdr_get "$tmpdir/kills.hdr" "ETag")"
expires_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Expires")"
expires_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Expires")"
lm_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Last-Modified")"
lm_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Last-Modified")"

# Normalize / fallback
[[ -n "$etag_jumps_new" ]] || etag_jumps_new="$old_etag_jumps"
[[ -n "$etag_kills_new" ]] || etag_kills_new="$old_etag_kills"

to_iso_or_empty() {
  local rfc="$1"
  if [[ -z "$rfc" ]]; then echo ""; return; fi
  date -u -d "$rfc" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo ""
}

expires_jumps_iso="$(to_iso_or_empty "$expires_jumps_raw")"
expires_kills_iso="$(to_iso_or_empty "$expires_kills_raw")"
lm_jumps_iso="$(to_iso_or_empty "$lm_jumps_raw")"
lm_kills_iso="$(to_iso_or_empty "$lm_kills_raw")"

# Compute next_eligible_run_at = max(expires) + 60s (anti-polling)
expires_j_epoch=0
expires_k_epoch=0
if [[ -n "$expires_jumps_raw" ]]; then expires_j_epoch="$(date -u -d "$expires_jumps_raw" +%s 2>/dev/null || echo 0)"; fi
if [[ -n "$expires_kills_raw" ]]; then expires_k_epoch="$(date -u -d "$expires_kills_raw" +%s 2>/dev/null || echo 0)"; fi
max_exp_epoch=$(( expires_j_epoch > expires_k_epoch ? expires_j_epoch : expires_k_epoch ))
next_eligible_epoch=$(( max_exp_epoch + 60 ))
next_eligible_iso="$(date -u -d "@$next_eligible_epoch" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo "")"

changed_jumps=false
changed_kills=false
if [[ "$code_jumps" == "200" && -n "$etag_jumps_new" && "$etag_jumps_new" != "$old_etag_jumps" ]]; then changed_jumps=true; fi
if [[ "$code_kills" == "200" && -n "$etag_kills_new" && "$etag_kills_new" != "$old_etag_kills" ]]; then changed_kills=true; fi

should_ingest=false
if [[ "$changed_jumps" == "true" && "$changed_kills" == "true" ]]; then
  should_ingest=true
fi

echo "Status: jumps=$code_jumps changed=$changed_jumps | kills=$code_kills changed=$changed_kills | ingest=$should_ingest"

# ---- Ensure tables exist (DDL only; no DML, compatible con Sandbox) ----
detect_bq_location() {
  local out
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || true)"
  if echo "$out" | jq -e . >/dev/null 2>&1; then
    echo "$out" | jq -r '.location // empty'
  else
    echo ""
  fi
}

BQ_LOCATION="$(detect_bq_location || true)"
[[ -n "$BQ_LOCATION" ]] || BQ_LOCATION="US"

ensure_tables() {
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
    consolidated_at TIMESTAMP NOT NULL,
    observed_at     TIMESTAMP NOT NULL,
    expires_at      TIMESTAMP,
    etag            STRING,
    system_id       INT64 NOT NULL,
    ship_jumps      INT64
  )
  CLUSTER BY system_id, consolidated_at;

  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\` (
    consolidated_at TIMESTAMP NOT NULL,
    observed_at     TIMESTAMP NOT NULL,
    expires_at      TIMESTAMP,
    etag            STRING,
    system_id       INT64 NOT NULL,
    ship_kills      INT64,
    npc_kills       INT64,
    pod_kills       INT64
  )
  CLUSTER BY system_id, consolidated_at;
  " >/dev/null
}

load_ndjson_append() {
  local table="$1" file="$2"
  echo "Loading: ${GCP_PROJECT_ID}:${BQ_DATASET}.${table}"
  echo "NDJSON sample (first 3 lines):"
  head -n 3 "$file" || true
  echo "bq load output:"
  bq --location="$BQ_LOCATION" load \
    --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${table}" \
    "$file"
}

# --- DEFENSIVE CHECKS BEFORE jq PARSING ---

# If any endpoint returned 304, do not try to parse body
if [[ "$code_jumps" == "304" || "$code_kills" == "304" ]]; then
  echo "At least one endpoint returned 304; skipping body parsing and ingestion."
  should_ingest=false
fi

# Validate JSON bodies if we are going to ingest
if [[ "$should_ingest" == "true" ]]; then
  if ! jq -e . "$tmpdir/jumps.json" >/dev/null 2>&1; then
    echo "ERROR: jumps body is not valid JSON"
    head -c 400 "$tmpdir/jumps.json" | sed 's/[^[:print:]\t]/?/g'
    exit 1
  fi

  if ! jq -e . "$tmpdir/kills.json" >/dev/null 2>&1; then
    echo "ERROR: kills body is not valid JSON"
    head -c 400 "$tmpdir/kills.json" | sed 's/[^[:print:]\t]/?/g'
    exit 1
  fi

  if ! jq -e 'type=="array"' "$tmpdir/jumps.json" >/dev/null; then
    echo "ERROR: jumps JSON is not an array. Body:"
    jq -c . "$tmpdir/jumps.json" | head -c 400
    exit 1
  fi

  if ! jq -e 'type=="array"' "$tmpdir/kills.json" >/dev/null; then
    echo "ERROR: kills JSON is not an array. Body:"
    jq -c . "$tmpdir/kills.json" | head -c 400
    exit 1
  fi
fi

if [[ "$should_ingest" == "true" && "$DRY_RUN" != "true" ]]; then
  ensure_tables

  # Build NDJSON with consolidated_at from Last-Modified (per tu requisito)
  consolidated_j="${lm_jumps_iso:-$now_iso}"
  consolidated_k="${lm_kills_iso:-$now_iso}"

  jumps_ndjson="$tmpdir/jumps.ndjson"
  kills_ndjson="$tmpdir/kills.ndjson"

  jq -c \
    --arg consolidated_at "$consolidated_j" \
    --arg observed_at "$now_iso" \
    --arg expires_at "$expires_jumps_iso" \
    --arg etag "$etag_jumps_new" \
    '.[] | {
      consolidated_at: $consolidated_at,
      observed_at: $observed_at,
      expires_at: (if $expires_at=="" then null else $expires_at end),
      etag: (if $etag=="" then null else $etag end),
      system_id: (.system_id|tonumber),
      ship_jumps: (.ship_jumps|tonumber)
    }' "$tmpdir/jumps.json" > "$jumps_ndjson"

  jq -c \
    --arg consolidated_at "$consolidated_k" \
    --arg observed_at "$now_iso" \
    --arg expires_at "$expires_kills_iso" \
    --arg etag "$etag_kills_new" \
    '.[] | {
      consolidated_at: $consolidated_at,
      observed_at: $observed_at,
      expires_at: (if $expires_at=="" then null else $expires_at end),
      etag: (if $etag=="" then null else $etag end),
      system_id: (.system_id|tonumber),
      ship_kills: (.ship_kills|tonumber),
      npc_kills: (.npc_kills|tonumber),
      pod_kills: (.pod_kills|tonumber)
    }' "$tmpdir/kills.json" > "$kills_ndjson"

  load_ndjson_append "system_jumps" "$jumps_ndjson"
  load_ndjson_append "system_kills" "$kills_ndjson"
else
  echo "No ingestion performed (either no change or DRY_RUN=true)."
fi

# ---- Update state line 1 ----
# Regla:
# - Siempre actualizar expires_* y next_eligible_run_at (anti-abuso / anti-polling).
# - Solo actualizar ETags persistidos cuando se haya ingerido (ambos changed).
new_etag_jumps="$old_etag_jumps"
new_etag_kills="$old_etag_kills"
new_lm_jumps="$(echo "$line1" | jq -r '.last_modified_jumps // ""')"
new_lm_kills="$(echo "$line1" | jq -r '.last_modified_kills // ""')"

if [[ "$should_ingest" == "true" ]]; then
  new_etag_jumps="$etag_jumps_new"
  new_etag_kills="$etag_kills_new"
  [[ -n "$lm_jumps_iso" ]] && new_lm_jumps="$lm_jumps_iso"
  [[ -n "$lm_kills_iso" ]] && new_lm_kills="$lm_kills_iso"
fi

line1_new="$(jq -cn \
  --arg type "ESI_ETAG_STATE" \
  --arg updated_at "$now_iso" \
  --arg workflow "$workflow_yaml" \
  --arg etag_jumps "$new_etag_jumps" \
  --arg etag_kills "$new_etag_kills" \
  --arg last_modified_jumps "$new_lm_jumps" \
  --arg last_modified_kills "$new_lm_kills" \
  --arg expires_jumps "$expires_jumps_iso" \
  --arg expires_kills "$expires_kills_iso" \
  --arg next_eligible_run_at "$next_eligible_iso" \
  '{
    type: $type,
    updated_at: $updated_at,
    workflow: $workflow,
    etag_jumps: (if $etag_jumps=="" then null else $etag_jumps end),
    etag_kills: (if $etag_kills=="" then null else $etag_kills end),
    last_modified_jumps: (if $last_modified_jumps=="" then null else $last_modified_jumps end),
    last_modified_kills: (if $last_modified_kills=="" then null else $last_modified_kills end),
    expires_jumps: (if $expires_jumps=="" then null else $expires_jumps end),
    expires_kills: (if $expires_kills=="" then null else $expires_kills end),
    next_eligible_run_at: (if $next_eligible_run_at=="" then null else $next_eligible_run_at end)
  }'
)"

printf "%s\n%s\n" "$line1_new" "$line2" > "$STATE_FILE"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY_RUN=true → no commit."
  exit 0
fi

# Commit only if state file changed
if ! git diff --quiet -- "$STATE_FILE"; then
  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git add "$STATE_FILE"
  git commit -m "orch: update ESI system stats state ($now_iso)" >/dev/null
  git push >/dev/null
  echo "State committed."
else
  echo "No state changes to commit."
fi
