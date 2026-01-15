#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Config (via env in workflow)
# ----------------------------
: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${BQ_DATASET:=eou}"
: "${STATE_FILE:=.orch/state.jsonl}"

: "${ESI_BASE_URL:=https://esi.evetech.net/latest}"
: "${ESI_DATASOURCE:=tranquility}"
: "${USER_AGENT:?Missing USER_AGENT}"

: "${FORCE:=false}"
: "${DRY_RUN:=false}"

JUMPS_URL="${ESI_BASE_URL}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"
KILLS_URL="${ESI_BASE_URL}/universe/system_kills/?datasource=${ESI_DATASOURCE}"

# -------------
# Small helpers
# -------------
log() { echo "$@" >&2; }

now_rfc3339z() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# Parse RFC1123 (e.g. "Wed, 14 Jan 2026 23:51:24 GMT") -> RFC3339 Z
rfc1123_to_rfc3339z() {
  local s="${1:-}"
  [[ -z "$s" ]] && { echo ""; return 0; }
  date -u -d "$s" +"%Y-%m-%dT%H:%M:%SZ"
}

# Lowercase header keys, strip CR, return first match value (no surrounding quotes)
hdr_get() {
  local hdr_file="$1" key_lc="$2"
  awk -v k="$key_lc" '
    BEGIN{IGNORECASE=1}
    {gsub("\r","")}
    tolower($0) ~ ("^" k ":[[:space:]]*") {
      sub("^[^:]*:[[:space:]]*","",$0)
      print $0
      exit
    }' "$hdr_file"
}

# Detect dataset location robustly even if bq prints warnings before JSON
detect_bq_location() {
  local out json loc
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  [[ -z "$json" ]] && return 1
  loc="$(printf '%s' "$json" | jq -r '.location // empty')"
  [[ -z "$loc" ]] && return 1
  echo "$loc"
}

# Fetch one endpoint with optional If-None-Match
fetch_one() {
  local name="$1" url="$2" etag="$3"
  local hdr="$4" body="$5" code_file="$6"

  local -a args
  args=(-sS --compressed
        -H "Accept: application/json"
        -H "User-Agent: ${USER_AGENT}"
        -D "$hdr" -o "$body" -w "%{http_code}")

  [[ -n "$etag" ]] && args+=(-H "If-None-Match: \"$etag\"")

  local code
  code="$(curl "${args[@]}" "$url")"
  echo "$code" > "$code_file"
  log "HTTP $name=$code"
}

# Ensure state.jsonl exists with 2 lines
init_state_file() {
  mkdir -p "$(dirname "$STATE_FILE")"
  touch "$STATE_FILE"

  local l1 l2
  l1="$(sed -n '1p' "$STATE_FILE" || true)"
  l2="$(sed -n '2p' "$STATE_FILE" || true)"

  if [[ -z "${l1:-}" ]]; then
    l1='{"type":"ESI_ETAG_STATE","updated_at":null,"workflow":null,"etag_jumps":null,"etag_kills":null,"last_modified_jumps":null,"last_modified_kills":null,"expires_jumps":null,"expires_kills":null,"next_eligible_run_at":null}'
  fi
  if [[ -z "${l2:-}" ]]; then
    l2='{"type":"BQ_REFRESH_STATE","updated_at":null,"workflow":null,"last_refresh_at":null,"next_refresh_at":null}'
  fi

  # Validate JSON (hard fail if corrupted)
  echo "$l1" | jq -e . >/dev/null
  echo "$l2" | jq -e . >/dev/null

  printf '%s\n%s\n' "$l1" "$l2" > "$STATE_FILE"
}

# Compute next_eligible_run_at = max(expires_jumps, expires_kills) + 60s (RFC3339Z)
compute_next_eligible() {
  local exp_j="$1" exp_k="$2"
  [[ -z "$exp_j" && -z "$exp_k" ]] && { echo ""; return 0; }

  # Convert to epoch safely; if one missing, treat as 0
  local ej ek
  ej=0; ek=0
  [[ -n "$exp_j" ]] && ej="$(date -u -d "$exp_j" +%s)"
  [[ -n "$exp_k" ]] && ek="$(date -u -d "$exp_k" +%s)"
  local m=$(( ej > ek ? ej : ek ))
  local n=$(( m + 60 ))
  date -u -d "@$n" +"%Y-%m-%dT%H:%M:%SZ"
}

# Create tables if not exist (partition + cluster)
ensure_tables() {
  local loc="$1"
  bq --location="$loc" query --use_legacy_sql=false "
  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
    ts TIMESTAMP NOT NULL,
    system_id INT64 NOT NULL,
    ship_jumps INT64
  )
  PARTITION BY DATE(ts)
  CLUSTER BY system_id;

  CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\` (
    ts TIMESTAMP NOT NULL,
    system_id INT64 NOT NULL,
    ship_kills INT64,
    npc_kills INT64,
    pod_kills INT64
  )
  PARTITION BY DATE(ts)
  CLUSTER BY system_id;
  " >/dev/null
}

# bq load append NDJSON
bq_load_append() {
  local loc="$1" table="$2" file="$3"
  bq --location="$loc" load \
    --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${table}" \
    "$file" >/dev/null
}

# -----------------
# Main script logic
# -----------------
init_state_file

LINE1="$(sed -n '1p' "$STATE_FILE")"
LINE2="$(sed -n '2p' "$STATE_FILE")"

OLD_ETAG_J="$(echo "$LINE1" | jq -r '.etag_jumps // ""')"
OLD_ETAG_K="$(echo "$LINE1" | jq -r '.etag_kills // ""')"
NEXT_ELIGIBLE="$(echo "$LINE1" | jq -r '.next_eligible_run_at // ""')"

# Respect scheduler hint (avoid unnecessary calls) unless FORCE=true
if [[ "$FORCE" != "true" && -n "$NEXT_ELIGIBLE" ]]; then
  now_s="$(date -u +%s)"
  next_s="$(date -u -d "$NEXT_ELIGIBLE" +%s)"
  if (( now_s < next_s )); then
    log "Not eligible yet (now=$(now_rfc3339z), next_eligible_run_at=$NEXT_ELIGIBLE). Exiting."
    exit 0
  fi
fi

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

J_HDR="$TMPDIR/jumps.hdr"; J_BODY="$TMPDIR/jumps.json"; J_CODE="$TMPDIR/jumps.code"
K_HDR="$TMPDIR/kills.hdr"; K_BODY="$TMPDIR/kills.json"; K_CODE="$TMPDIR/kills.code"

fetch_one "jumps" "$JUMPS_URL" "$OLD_ETAG_J" "$J_HDR" "$J_BODY" "$J_CODE"
fetch_one "kills" "$KILLS_URL" "$OLD_ETAG_K" "$K_HDR" "$K_BODY" "$K_CODE"

CODE_J="$(cat "$J_CODE")"
CODE_K="$(cat "$K_CODE")"

# Pull headers (may be absent in 304/edge cases)
RAW_ETAG_J="$(hdr_get "$J_HDR" "etag" | sed -E 's/^"//; s/"$//')"
RAW_ETAG_K="$(hdr_get "$K_HDR" "etag" | sed -E 's/^"//; s/"$//')"

RAW_LM_J="$(hdr_get "$J_HDR" "last-modified")"
RAW_LM_K="$(hdr_get "$K_HDR" "last-modified")"
RAW_EXP_J="$(hdr_get "$J_HDR" "expires")"
RAW_EXP_K="$(hdr_get "$K_HDR" "expires")"

LM_J="$(rfc1123_to_rfc3339z "$RAW_LM_J")"
LM_K="$(rfc1123_to_rfc3339z "$RAW_LM_K")"
EXP_J="$(rfc1123_to_rfc3339z "$RAW_EXP_J")"
EXP_K="$(rfc1123_to_rfc3339z "$RAW_EXP_K")"

# If server didn't send ETag (rare), keep old
NEW_ETAG_J="${RAW_ETAG_J:-$OLD_ETAG_J}"
NEW_ETAG_K="${RAW_ETAG_K:-$OLD_ETAG_K}"

CHANGED_J="false"
CHANGED_K="false"
[[ -n "$NEW_ETAG_J" && "$NEW_ETAG_J" != "$OLD_ETAG_J" ]] && CHANGED_J="true"
[[ -n "$NEW_ETAG_K" && "$NEW_ETAG_K" != "$OLD_ETAG_K" ]] && CHANGED_K="true"

# If body is present, validate JSON (200 should be array)
if [[ "$CODE_J" == "200" ]]; then jq -e 'type=="array"' "$J_BODY" >/dev/null; fi
if [[ "$CODE_K" == "200" ]]; then jq -e 'type=="array"' "$K_BODY" >/dev/null; fi

# Ingest policy: only when BOTH changed and both are 200
INGEST="false"
if [[ "$CODE_J" == "200" && "$CODE_K" == "200" && "$CHANGED_J" == "true" && "$CHANGED_K" == "true" ]]; then
  INGEST="true"
fi

log "Status: jumps=$CODE_J changed=$CHANGED_J | kills=$CODE_K changed=$CHANGED_K | ingest=$INGEST"

# BigQuery ingestion (append)
if [[ "$INGEST" == "true" && "$DRY_RUN" != "true" ]]; then
  BQ_LOCATION="$(detect_bq_location)" || { log "ERROR: Could not detect dataset location"; exit 1; }
  ensure_tables "$BQ_LOCATION"

  # ts comes from Last-Modified (snapshot timestamp) in RFC3339Z
  [[ -z "$LM_J" || -z "$LM_K" ]] && { log "ERROR: Missing Last-Modified for ingest"; exit 1; }

  J_NDJSON="$TMPDIR/jumps.ndjson"
  K_NDJSON="$TMPDIR/kills.ndjson"

  jq -c --arg ts "$LM_J" '
    .[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_jumps: ((.ship_jumps // 0)|tonumber)
    }' "$J_BODY" > "$J_NDJSON"

  jq -c --arg ts "$LM_K" '
    .[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_kills: ((.ship_kills // 0)|tonumber),
      npc_kills:  ((.npc_kills  // 0)|tonumber),
      pod_kills:  ((.pod_kills  // 0)|tonumber)
    }' "$K_BODY" > "$K_NDJSON"

  bq_load_append "$BQ_LOCATION" "system_jumps" "$J_NDJSON"
  bq_load_append "$BQ_LOCATION" "system_kills" "$K_NDJSON"
else
  log "No ingestion performed (either no change, not both-200, or DRY_RUN=true)."
fi

# ----
# Update state.jsonl (always reflect latest headers we saw)
# ----
UPDATED_AT="$(now_rfc3339z)"

# If we didn't receive LM/Expires (e.g. unusual 304), keep previous values
PREV_LM_J="$(echo "$LINE1" | jq -r '.last_modified_jumps // ""')"
PREV_LM_K="$(echo "$LINE1" | jq -r '.last_modified_kills // ""')"
PREV_EXP_J="$(echo "$LINE1" | jq -r '.expires_jumps // ""')"
PREV_EXP_K="$(echo "$LINE1" | jq -r '.expires_kills // ""')"

[[ -z "$LM_J" ]] && LM_J="$PREV_LM_J"
[[ -z "$LM_K" ]] && LM_K="$PREV_LM_K"
[[ -z "$EXP_J" ]] && EXP_J="$PREV_EXP_J"
[[ -z "$EXP_K" ]] && EXP_K="$PREV_EXP_K"

NEXT_ELIGIBLE_NEW="$(compute_next_eligible "$EXP_J" "$EXP_K")"

# Workflow name from GHA env if present; otherwise keep previous
WORKFLOW_YAML="${GITHUB_WORKFLOW_REF:-}"
if [[ -n "$WORKFLOW_YAML" ]]; then
  # e.g. owner/repo/.github/workflows/file.yml@ref -> file.yml
  WORKFLOW_YAML="$(echo "$WORKFLOW_YAML" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"
fi
[[ -z "$WORKFLOW_YAML" ]] && WORKFLOW_YAML="$(echo "$LINE1" | jq -r '.workflow // ""')"

LINE1_NEW="$(jq -cn \
  --arg updated_at "$UPDATED_AT" \
  --arg workflow "$WORKFLOW_YAML" \
  --arg etag_jumps "$NEW_ETAG_J" \
  --arg etag_kills "$NEW_ETAG_K" \
  --arg lm_j "$LM_J" \
  --arg lm_k "$LM_K" \
  --arg exp_j "$EXP_J" \
  --arg exp_k "$EXP_K" \
  --arg next_run "$NEXT_ELIGIBLE_NEW" '
{
  type: "ESI_ETAG_STATE",
  updated_at: $updated_at,
  workflow: (if $workflow=="" then null else $workflow end),
  etag_jumps: (if $etag_jumps=="" then null else $etag_jumps end),
  etag_kills: (if $etag_kills=="" then null else $etag_kills end),
  last_modified_jumps: (if $lm_j=="" then null else $lm_j end),
  last_modified_kills: (if $lm_k=="" then null else $lm_k end),
  expires_jumps: (if $exp_j=="" then null else $exp_j end),
  expires_kills: (if $exp_k=="" then null else $exp_k end),
  next_eligible_run_at: (if $next_run=="" then null else $next_run end)
}
')"

printf '%s\n%s\n' "$LINE1_NEW" "$LINE2" > "$STATE_FILE"

# Commit state if changed and not dry-run (workflow has contents:write)
if [[ "$DRY_RUN" != "true" ]]; then
  if ! git diff --quiet -- "$STATE_FILE"; then
    git config user.name "github-actions[bot]"
    git config user.email "github-actions[bot]@users.noreply.github.com"
    git add "$STATE_FILE"
    git commit -m "orch: update ESI system stats state ($UPDATED_AT)"
    git push
    log "State committed."
  else
    log "State unchanged; no commit."
  fi
else
  log "DRY_RUN=true → no commit."
fi
