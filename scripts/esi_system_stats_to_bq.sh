#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Config (via env)
# -----------------------------
: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${BQ_DATASET:=eou}"

: "${ESI_BASE:=https://esi.evetech.net/latest}"
: "${ESI_DATASOURCE:=tranquility}"
: "${USER_AGENT:=landu-eou/eou (ESI system stats) contact: landueve78@gmail.com}"

: "${STATE_FILE:=.orch/state.jsonl}"

# Control knobs (useful for manual tests)
: "${FORCE:=false}"     # if true, ignores next_eligible_run_at gate
: "${DRY_RUN:=false}"   # if true, don't load to BQ and don't git commit

WORKFLOW_REF="${GITHUB_WORKFLOW_REF:-landu-eou/eou/.github/workflows/wof-esi-bq-system-stats.yml@refs/heads/main}"
WORKFLOW_YAML="$(echo "$WORKFLOW_REF" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"

URL_JUMPS="${ESI_BASE}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"
URL_KILLS="${ESI_BASE}/universe/system_kills/?datasource=${ESI_DATASOURCE}"

# -----------------------------
# Helpers
# -----------------------------
log() { echo "$@" >&2; }

utc_now_rfc3339() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# Convert HTTP-date (e.g. "Thu, 15 Jan 2026 00:51:24 GMT") -> RFC3339Z
httpdate_to_rfc3339z() {
  local httpdate="$1"
  [[ -z "$httpdate" ]] && { echo ""; return 0; }
  date -u -d "$httpdate" +"%Y-%m-%dT%H:%M:%SZ"
}

# RFC3339Z -> epoch seconds
rfc3339z_to_epoch() {
  local rfc="$1"
  [[ -z "$rfc" ]] && { echo ""; return 0; }
  date -u -d "$rfc" +%s
}

# epoch seconds -> RFC3339Z
epoch_to_rfc3339z() {
  local epoch="$1"
  [[ -z "$epoch" ]] && { echo ""; return 0; }
  date -u -d "@$epoch" +"%Y-%m-%dT%H:%M:%SZ"
}

# Get header value from curl -D header file (case-insensitive)
get_hdr() {
  local hdr_file="$1" header_name="$2"
  # prints raw value (trimmed), empty if not present
  awk -v IGNORECASE=1 -v h="$header_name" '
    BEGIN{FS=":"}
    $1 ~ "^"h"$" {
      sub(/^[^:]*:[[:space:]]*/, "", $0);
      sub(/\r$/, "", $0);
      print $0;
      exit
    }
  ' "$hdr_file"
}

strip_etag_quotes() {
  local et="$1"
  et="${et%$'\r'}"
  et="${et#\"}"
  et="${et%\"}"
  echo "$et"
}

detect_bq_location() {
  # bq show may print warnings before JSON; extract JSON from first "{" onwards
  local out json loc
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  if ! echo "$json" | jq -e . >/dev/null 2>&1; then
    log "ERROR: Could not parse dataset JSON from: bq show ${GCP_PROJECT_ID}:${BQ_DATASET}"
    log "$out"
    return 1
  fi
  loc="$(echo "$json" | jq -r '.location // empty')"
  [[ -z "$loc" ]] && { log "ERROR: dataset location missing"; return 1; }
  echo "$loc"
}

ensure_state_file() {
  mkdir -p "$(dirname "$STATE_FILE")"
  touch "$STATE_FILE"

  # Ensure it has 2 lines (ESI_ETAG_STATE + BQ_REFRESH_STATE)
  local l1 l2
  l1="$(sed -n '1p' "$STATE_FILE" || true)"
  l2="$(sed -n '2p' "$STATE_FILE" || true)"

  if [[ -z "$l1" ]]; then
    l1='{"type":"ESI_ETAG_STATE","updated_at":null,"workflow":null,"etag_jumps":null,"etag_kills":null,"last_modified_jumps":null,"last_modified_kills":null,"expires_jumps":null,"expires_kills":null,"next_eligible_run_at":null}'
  fi
  if [[ -z "$l2" ]]; then
    l2='{"type":"BQ_REFRESH_STATE","updated_at":null,"workflow":null,"last_refresh_at":null,"next_refresh_at":null}'
  fi

  printf "%s\n%s\n" "$l1" "$l2" > "$STATE_FILE"
}

read_state_field() {
  local line_json="$1" jq_expr="$2"
  echo "$line_json" | jq -r "$jq_expr // empty"
}

should_gate_by_next_eligible() {
  # returns 0 if allowed to run, 1 if should stop
  local next_eligible="$1"
  [[ "$FORCE" == "true" ]] && return 0
  [[ -z "$next_eligible" ]] && return 0

  local now_epoch next_epoch
  now_epoch="$(date -u +%s)"
  next_epoch="$(rfc3339z_to_epoch "$next_eligible")"
  [[ -z "$next_epoch" ]] && return 0

  if (( now_epoch < next_epoch )); then
    log "Gate: now < next_eligible_run_at (${next_eligible}) → skipping."
    return 1
  fi
  return 0
}

fetch_one() {
  # args: name url old_etag tmpdir
  local name="$1" url="$2" old_etag="$3" tmpdir="$4"

  local hdr="${tmpdir}/${name}.hdr"
  local body="${tmpdir}/${name}.json"
  local code_file="${tmpdir}/${name}.code"

  local -a curl_args
  curl_args=(-sS --compressed
    -H "Accept: application/json"
    -H "User-Agent: ${USER_AGENT}"
    -D "$hdr"
    -o "$body"
    -w "%{http_code}"
  )
  if [[ -n "$old_etag" ]]; then
    curl_args+=(-H "If-None-Match: \"$old_etag\"")
  fi

  local code
  code="$(curl "${curl_args[@]}" "$url")"
  echo "$code" > "$code_file"

  # For 304, body may be empty; that's OK.
  echo "$hdr|$body|$code"
}

ensure_tables() {
  local bq_loc="$1"
  bq --location="$bq_loc" query --use_legacy_sql=false "
  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
    ts        TIMESTAMP NOT NULL,
    system_id INT64     NOT NULL,
    ship_jumps INT64
  )
  PARTITION BY DATE(ts)
  CLUSTER BY system_id;

  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\` (
    ts        TIMESTAMP NOT NULL,
    system_id INT64     NOT NULL,
    ship_kills INT64,
    npc_kills  INT64,
    pod_kills  INT64
  )
  PARTITION BY DATE(ts)
  CLUSTER BY system_id;
  "
}

load_ndjson_append() {
  local bq_loc="$1" table="$2" file="$3"
  bq --location="$bq_loc" load --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${table}" \
    "$file"
}

commit_state_if_needed() {
  [[ "$DRY_RUN" == "true" ]] && { log "DRY_RUN=true → no git commit."; return 0; }

  if git diff --quiet -- "$STATE_FILE"; then
    log "State unchanged → no commit."
    return 0
  fi

  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git add "$STATE_FILE"
  git commit -m "orch: update ESI system stats state ($(utc_now_rfc3339))"
  git push
  log "State committed."
}

# -----------------------------
# Main
# -----------------------------
ensure_state_file

LINE1="$(sed -n '1p' "$STATE_FILE")"
OLD_ETAG_JUMPS="$(read_state_field "$LINE1" '.etag_jumps')"
OLD_ETAG_KILLS="$(read_state_field "$LINE1" '.etag_kills')"
NEXT_ELIGIBLE_OLD="$(read_state_field "$LINE1" '.next_eligible_run_at')"

if ! should_gate_by_next_eligible "$NEXT_ELIGIBLE_OLD"; then
  exit 0
fi

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

# Fetch both endpoints (use old ETags as validators)
J_RES="$(fetch_one "jumps" "$URL_JUMPS" "$OLD_ETAG_JUMPS" "$TMPDIR")"
K_RES="$(fetch_one "kills" "$URL_KILLS" "$OLD_ETAG_KILLS" "$TMPDIR")"

J_HDR="${J_RES%%|*}"; rest="${J_RES#*|}"; J_BODY="${rest%%|*}"; J_CODE="${rest#*|}"
K_HDR="${K_RES%%|*}"; rest="${K_RES#*|}"; K_BODY="${rest%%|*}"; K_CODE="${rest#*|}"

# Parse headers
J_ETAG="$(strip_etag_quotes "$(get_hdr "$J_HDR" "etag")")"
K_ETAG="$(strip_etag_quotes "$(get_hdr "$K_HDR" "etag")")"

J_LM_HTTP="$(get_hdr "$J_HDR" "last-modified")"
K_LM_HTTP="$(get_hdr "$K_HDR" "last-modified")"
J_EXP_HTTP="$(get_hdr "$J_HDR" "expires")"
K_EXP_HTTP="$(get_hdr "$K_HDR" "expires")"

J_LAST_MODIFIED="$(httpdate_to_rfc3339z "$J_LM_HTTP")"
K_LAST_MODIFIED="$(httpdate_to_rfc3339z "$K_LM_HTTP")"
J_EXPIRES="$(httpdate_to_rfc3339z "$J_EXP_HTTP")"
K_EXPIRES="$(httpdate_to_rfc3339z "$K_EXP_HTTP")"

# Compute next_eligible_run_at = max(expires_*) + 60s
J_EXP_EPOCH="$(rfc3339z_to_epoch "$J_EXPIRES")"
K_EXP_EPOCH="$(rfc3339z_to_epoch "$K_EXPIRES")"
MAX_EXP_EPOCH="$J_EXP_EPOCH"
if [[ -n "$K_EXP_EPOCH" ]] && [[ -n "$MAX_EXP_EPOCH" ]] && (( K_EXP_EPOCH > MAX_EXP_EPOCH )); then
  MAX_EXP_EPOCH="$K_EXP_EPOCH"
fi
if [[ -z "$MAX_EXP_EPOCH" ]]; then
  NEXT_ELIGIBLE_NEW=""
else
  NEXT_ELIGIBLE_NEW="$(epoch_to_rfc3339z $((MAX_EXP_EPOCH + 60)))"
fi

# Determine "changed" based on ETag differences
CHANGED_J="false"
CHANGED_K="false"
if [[ "$J_CODE" == "200" ]]; then
  [[ -z "$OLD_ETAG_JUMPS" || "$J_ETAG" != "$OLD_ETAG_JUMPS" ]] && CHANGED_J="true"
fi
if [[ "$K_CODE" == "200" ]]; then
  [[ -z "$OLD_ETAG_KILLS" || "$K_ETAG" != "$OLD_ETAG_KILLS" ]] && CHANGED_K="true"
fi
# If 304, it means "not modified" under the validator logic.
if [[ "$J_CODE" == "304" ]]; then CHANGED_J="false"; fi
if [[ "$K_CODE" == "304" ]]; then CHANGED_K="false"; fi

# Decide ingestion: only when BOTH changed and BOTH are 200 (we need both bodies)
INGEST="false"
if [[ "$CHANGED_J" == "true" && "$CHANGED_K" == "true" && "$J_CODE" == "200" && "$K_CODE" == "200" ]]; then
  INGEST="true"
fi

log "Status: jumps=${J_CODE} changed=${CHANGED_J} | kills=${K_CODE} changed=${CHANGED_K} | ingest=${INGEST}"

# If ingest, validate JSON bodies and load to BigQuery
if [[ "$INGEST" == "true" && "$DRY_RUN" != "true" ]]; then
  jq -e 'type=="array"' "$J_BODY" >/dev/null
  jq -e 'type=="array"' "$K_BODY" >/dev/null

  BQ_LOCATION="$(detect_bq_location)"
  log "BQ_LOCATION=${BQ_LOCATION}"

  ensure_tables "$BQ_LOCATION"

  # Snapshot ts (TIMESTAMP) comes from Last-Modified (normalized RFC3339Z).
  # This matches HTTP semantics: Last-Modified is when the server believes the resource last changed. :contentReference[oaicite:3]{index=3}
  TS_J="$J_LAST_MODIFIED"
  TS_K="$K_LAST_MODIFIED"
  [[ -z "$TS_J" || -z "$TS_K" ]] && { log "ERROR: Missing Last-Modified (cannot derive ts)"; exit 1; }

  J_NDJSON="${TMPDIR}/jumps.ndjson"
  K_NDJSON="${TMPDIR}/kills.ndjson"

  jq -c --arg ts "$TS_J" '
    .[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_jumps: (if (.ship_jumps // null) == null then null else (.ship_jumps|tonumber) end)
    }' "$J_BODY" > "$J_NDJSON"

  jq -c --arg ts "$TS_K" '
    .[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_kills: (if (.ship_kills // null) == null then null else (.ship_kills|tonumber) end),
      npc_kills:  (if (.npc_kills  // null) == null then null else (.npc_kills|tonumber) end),
      pod_kills:  (if (.pod_kills  // null) == null then null else (.pod_kills|tonumber) end)
    }' "$K_BODY" > "$K_NDJSON"

  load_ndjson_append "$BQ_LOCATION" "system_jumps" "$J_NDJSON"
  load_ndjson_append "$BQ_LOCATION" "system_kills" "$K_NDJSON"
else
  log "No ingestion performed (either no change or DRY_RUN=true)."
fi

# Update state.jsonl EVERY run (as requested): store ETag/Last-Modified/Expires + computed next_eligible_run_at
NOW="$(utc_now_rfc3339)"

LINE1_NEW="$(jq -cn \
  --arg type "ESI_ETAG_STATE" \
  --arg updated_at "$NOW" \
  --arg workflow "$WORKFLOW_YAML" \
  --arg etag_jumps "$J_ETAG" \
  --arg etag_kills "$K_ETAG" \
  --arg last_modified_jumps "$J_LAST_MODIFIED" \
  --arg last_modified_kills "$K_LAST_MODIFIED" \
  --arg expires_jumps "$J_EXPIRES" \
  --arg expires_kills "$K_EXPIRES" \
  --arg next_eligible_run_at "$NEXT_ELIGIBLE_NEW" \
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

LINE2="$(sed -n '2p' "$STATE_FILE")"
printf "%s\n%s\n" "$LINE1_NEW" "$LINE2" > "$STATE_FILE"

commit_state_if_needed
