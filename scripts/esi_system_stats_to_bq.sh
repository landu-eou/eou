#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Required env
# -----------------------------
: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${USER_AGENT:?Missing USER_AGENT}"

# Optional env (defaults)
: "${BQ_DATASET:=eou}"
: "${STATE_FILE:=.orch/state.jsonl}"
: "${ESI_BASE_URL:=https://esi.evetech.net/latest}"
: "${ESI_DATASOURCE:=tranquility}"

FORCE="${FORCE:-false}"   # if true, ignore next_eligible_run_at gate
DRY_RUN="${DRY_RUN:-false}"

mkdir -p "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

workflow_ref="${GITHUB_WORKFLOW_REF:-unknown}"
workflow_yaml="$(echo "$workflow_ref" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"
now_iso="$(date -u +"%Y-%m-%d %H:%M:%S+00:00")"
now_epoch="$(date -u +%s)"

# -----------------------------
# State file helpers (2-line JSONL)
# -----------------------------
line1="$(sed -n '1p' "$STATE_FILE" 2>/dev/null || true)"
line2="$(sed -n '2p' "$STATE_FILE" 2>/dev/null || true)"
[[ -n "$line1" ]] || line1='{"type":"ESI_ETAG_STATE"}'
[[ -n "$line2" ]] || line2='{"type":"BQ_REFRESH_STATE"}'

# Validate JSON, fallback if corrupted
if ! echo "$line1" | jq -e . >/dev/null 2>&1; then line1='{"type":"ESI_ETAG_STATE"}'; fi
if ! echo "$line2" | jq -e . >/dev/null 2>&1; then line2='{"type":"BQ_REFRESH_STATE"}'; fi

old_etag_jumps="$(echo "$line1" | jq -r '.etag_jumps // ""')"
old_etag_kills="$(echo "$line1" | jq -r '.etag_kills // ""')"
next_eligible="$(echo "$line1" | jq -r '.next_eligible_run_at // ""')"

# Gate by next_eligible_run_at unless FORCE=true
if [[ "$FORCE" != "true" && -n "$next_eligible" && "$next_eligible" != "null" ]]; then
  next_epoch="$(date -u -d "$next_eligible" +%s 2>/dev/null || echo 0)"
  if (( now_epoch < next_epoch )); then
    echo "Too early. next_eligible_run_at=$next_eligible (now=$now_iso). Exiting."
    exit 0
  fi
fi

# -----------------------------
# Temp workspace
# -----------------------------
tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir"; else cleanup; fi; exit $rc' EXIT

# -----------------------------
# HTTP helpers
# -----------------------------
hdr_get() {
  local file="$1" key="$2"
  grep -i "^${key}:" "$file" | tail -n 1 | cut -d: -f2- | tr -d '\r' | xargs || true
}

to_iso_or_empty() {
  local rfc="$1"
  [[ -n "$rfc" ]] || { echo ""; return; }
  date -u -d "$rfc" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo ""
}

to_date_or_empty() {
  local rfc="$1"
  [[ -n "$rfc" ]] || { echo ""; return; }
  date -u -d "$rfc" +"%Y-%m-%d" 2>/dev/null || echo ""
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
  [[ -n "$etag" ]] && curl_args+=(-H "If-None-Match: ${etag}")

  local code
  code="$(curl "${curl_args[@]}" "$url" || true)"
  echo "$code" > "$code_file"
}

# -----------------------------
# ESI calls
# -----------------------------
url_jumps="${ESI_BASE_URL}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"
url_kills="${ESI_BASE_URL}/universe/system_kills/?datasource=${ESI_DATASOURCE}"

fetch_one "jumps" "$url_jumps" "$old_etag_jumps"
fetch_one "kills" "$url_kills" "$old_etag_kills"

code_jumps="$(cat "$tmpdir/jumps.code")"
code_kills="$(cat "$tmpdir/kills.code")"

# Abort on unexpected HTTP codes to protect error budget
[[ "$code_jumps" =~ ^(200|304)$ ]] || { echo "ESI jumps HTTP $code_jumps"; exit 1; }
[[ "$code_kills" =~ ^(200|304)$ ]] || { echo "ESI kills HTTP $code_kills"; exit 1; }

# Read caching headers
etag_jumps_new="$(hdr_get "$tmpdir/jumps.hdr" "ETag")"
etag_kills_new="$(hdr_get "$tmpdir/kills.hdr" "ETag")"
expires_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Expires")"
expires_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Expires")"
lm_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Last-Modified")"
lm_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Last-Modified")"

# Normalize
[[ -n "$etag_jumps_new" ]] || etag_jumps_new="$old_etag_jumps"
[[ -n "$etag_kills_new" ]] || etag_kills_new="$old_etag_kills"

expires_jumps_iso="$(to_iso_or_empty "$expires_jumps_raw")"
expires_kills_iso="$(to_iso_or_empty "$expires_kills_raw")"
lm_jumps_iso="$(to_iso_or_empty "$lm_jumps_raw")"
lm_kills_iso="$(to_iso_or_empty "$lm_kills_raw")"

# next_eligible_run_at = max(expires) + 60s
expires_j_epoch=0; expires_k_epoch=0
[[ -n "$expires_jumps_raw" ]] && expires_j_epoch="$(date -u -d "$expires_jumps_raw" +%s 2>/dev/null || echo 0)"
[[ -n "$expires_kills_raw" ]] && expires_k_epoch="$(date -u -d "$expires_kills_raw" +%s 2>/dev/null || echo 0)"
max_exp_epoch=$(( expires_j_epoch > expires_k_epoch ? expires_j_epoch : expires_k_epoch ))
next_eligible_epoch=$(( max_exp_epoch + 60 ))
next_eligible_iso="$(date -u -d "@$next_eligible_epoch" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo "")"

changed_jumps=false
changed_kills=false
[[ "$code_jumps" == "200" && -n "$etag_jumps_new" && "$etag_jumps_new" != "$old_etag_jumps" ]] && changed_jumps=true
[[ "$code_kills" == "200" && -n "$etag_kills_new" && "$etag_kills_new" != "$old_etag_kills" ]] && changed_kills=true

# Your rule: only ingest when BOTH changed
should_ingest=false
[[ "$changed_jumps" == "true" && "$changed_kills" == "true" ]] && should_ingest=true

# If any returned 304 -> cannot ingest
[[ "$code_jumps" == "304" || "$code_kills" == "304" ]] && should_ingest=false

echo "Status: jumps=$code_jumps changed=$changed_jumps | kills=$code_kills changed=$changed_kills | ingest=$should_ingest"

# -----------------------------
# BigQuery location detection (robust against gcloud warnings)
# -----------------------------
detect_bq_location() {
  local out json
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  [[ -n "$json" ]] || return 0
  echo "$json" | jq -r '.location // empty' 2>/dev/null || true
}

BQ_LOCATION="$(detect_bq_location || true)"
[[ -n "$BQ_LOCATION" ]] || { echo "ERROR: Could not detect dataset location for ${GCP_PROJECT_ID}:${BQ_DATASET}"; exit 1; }

# -----------------------------
# Table schema (new)
# -----------------------------
# system_jumps: date, system_id, ship_jumps
# system_kills: date, system_id, ship_kills, npc_kills, pod_kills
#
# We'll PARTITION BY date and CLUSTER BY system_id for efficiency.
ensure_tables_and_migrate_if_needed() {
  # Detect whether tables exist and whether they already have 'date' column
  local q
  q="
  DECLARE has_date_jumps BOOL DEFAULT (
    SELECT COUNTIF(column_name='date')>0
    FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS\`
    WHERE table_name='system_jumps'
  );
  DECLARE has_date_kills BOOL DEFAULT (
    SELECT COUNTIF(column_name='date')>0
    FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS\`
    WHERE table_name='system_kills'
  );

  -- Create if missing (new schema)
  IF NOT EXISTS (
    SELECT 1 FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.INFORMATION_SCHEMA.TABLES\`
    WHERE table_name='system_jumps'
  ) THEN
    EXECUTE IMMEDIATE '''
      CREATE TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
        date DATE NOT NULL,
        system_id INT64 NOT NULL,
        ship_jumps INT64
      )
      PARTITION BY date
      CLUSTER BY system_id
    ''';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.INFORMATION_SCHEMA.TABLES\`
    WHERE table_name='system_kills'
  ) THEN
    EXECUTE IMMEDIATE '''
      CREATE TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\` (
        date DATE NOT NULL,
        system_id INT64 NOT NULL,
        ship_kills INT64,
        npc_kills INT64,
        pod_kills INT64
      )
      PARTITION BY date
      CLUSTER BY system_id
    ''';
  END IF;

  -- If old schema detected (no 'date' column), migrate by rebuild+swap
  IF has_date_jumps = FALSE THEN
    EXECUTE IMMEDIATE '''
      CREATE TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps_new\` (
        date DATE NOT NULL,
        system_id INT64 NOT NULL,
        ship_jumps INT64
      )
      PARTITION BY date
      CLUSTER BY system_id
    ''';

    -- Best-effort transform from old schema if columns exist
    EXECUTE IMMEDIATE '''
      INSERT INTO \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps_new\` (date, system_id, ship_jumps)
      SELECT DATE(consolidated_at) AS date, system_id, ship_jumps
      FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\`
    ''';

    EXECUTE IMMEDIATE 'DROP TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\`';
    EXECUTE IMMEDIATE 'ALTER TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps_new\` RENAME TO system_jumps';
  END IF;

  IF has_date_kills = FALSE THEN
    EXECUTE IMMEDIATE '''
      CREATE TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills_new\` (
        date DATE NOT NULL,
        system_id INT64 NOT NULL,
        ship_kills INT64,
        npc_kills INT64,
        pod_kills INT64
      )
      PARTITION BY date
      CLUSTER BY system_id
    ''';

    EXECUTE IMMEDIATE '''
      INSERT INTO \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills_new\` (date, system_id, ship_kills, npc_kills, pod_kills)
      SELECT DATE(consolidated_at) AS date, system_id, ship_kills, npc_kills, pod_kills
      FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\`
    ''';

    EXECUTE IMMEDIATE 'DROP TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\`';
    EXECUTE IMMEDIATE 'ALTER TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills_new\` RENAME TO system_kills';
  END IF;
  "
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "$q" >/dev/null
}

load_ndjson_append() {
  local table="$1" file="$2"
  bq --location="$BQ_LOCATION" load \
    --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${table}" \
    "$file" >/dev/null
}

# -----------------------------
# Ingestion
# -----------------------------
if [[ "$should_ingest" == "true" && "$DRY_RUN" != "true" ]]; then
  # Validate bodies
  jq -e 'type=="array"' "$tmpdir/jumps.json" >/dev/null
  jq -e 'type=="array"' "$tmpdir/kills.json" >/dev/null

  ensure_tables_and_migrate_if_needed

  # date comes from Last-Modified (your requirement)
  date_jumps="$(to_date_or_empty "$lm_jumps_raw")"
  date_kills="$(to_date_or_empty "$lm_kills_raw")"
  [[ -n "$date_jumps" ]] || date_jumps="$(date -u +%Y-%m-%d)"
  [[ -n "$date_kills" ]] || date_kills="$(date -u +%Y-%m-%d)"

  jumps_ndjson="$tmpdir/jumps.ndjson"
  kills_ndjson="$tmpdir/kills.ndjson"

  jq -c \
    --arg date "$date_jumps" \
    '.[] | {
      date: $date,
      system_id: (.system_id|tonumber),
      ship_jumps: ((.ship_jumps // 0)|tonumber)
    }' "$tmpdir/jumps.json" > "$jumps_ndjson"

  jq -c \
    --arg date "$date_kills" \
    '.[] | {
      date: $date,
      system_id: (.system_id|tonumber),
      ship_kills: ((.ship_kills // 0)|tonumber),
      npc_kills: ((.npc_kills // 0)|tonumber),
      pod_kills: ((.pod_kills // 0)|tonumber)
    }' "$tmpdir/kills.json" > "$kills_ndjson"

  load_ndjson_append "system_jumps" "$jumps_ndjson"
  load_ndjson_append "system_kills" "$kills_ndjson"
else
  echo "No ingestion performed (either no change or DRY_RUN=true)."
fi

# -----------------------------
# Update state (always refresh cache headers + next_eligible)
# Only update stored ETags + Last-Modified when ingestion happened (your rule)
# -----------------------------
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

# Commit only if changed
if ! git diff --quiet -- "$STATE_FILE"; then
  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git add "$STATE_FILE"
  git commit -m "orch: update ESI system stats state ($now_iso)"
  git push
  echo "State committed."
else
  echo "No state changes to commit."
fi
