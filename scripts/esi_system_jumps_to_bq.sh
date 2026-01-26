#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR at line $LINENO" >&2' ERR

: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${USER_AGENT:?Missing USER_AGENT}"

BQ_DATASET="${BQ_DATASET:-eou}"
ESI_BASE_URL="${ESI_BASE_URL:-https://esi.evetech.net/latest}"
ESI_DATASOURCE="${ESI_D:contentReference[oaicite:10]{index=10}y}"

ENDPOINT="system_jumps"
STATE_PATH="${STATE_PATH:-eou/state/system_jumps.json}"

FORCE="${FORCE:-false}"
DRY_RUN="${DRY_RUN:-false}"

now_rfc3339z() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

to_rfc3339z_or_empty() {
  local http_date="$1"
  [[ -z "$http_date" ]] && { echo ""; return; }
  date -u -d "$http_date" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo ""
}

truncate_to_hour_rfc3339z() {
  local ts="$1"
  [[ -z "$ts" ]] && { echo ""; return; }
  date -u -d "$ts" +"%Y-%m-%dT%H:00:00Z" 2>/dev/null || echo ""
}

hdr_get() {
  local file="$1" key="$2"
  grep -i "^${key}:" "$file" | tail -n 1 | cut -d: -f2- | tr -d '\r' | xargs || true
}

detect_bq_location() {
  local out json
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  [[ -z "$json" ]] && echo "" && return
  echo "$json" | jq -r '.location // empty' 2>/dev/null || echo ""
}

ensure_tables() {
  # Final (idempotente por system_id+hour_ts)
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
    CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
      hour_ts              TIMESTAMP NOT NULL,
      system_id            INT64     NOT NULL,
      ship_jumps           INT64,
      source_last_modified TIMESTAMP,
      ingested_at          TIMESTAMP NOT NULL
    )
    PARTITION BY DATE(hour_ts)
    CLUSTER BY system_id;
  "

  # Staging (se sobreescribe en cada run)
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
    CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps_stg\` (
      hour_ts              TIMESTAMP NOT NULL,
      system_id            INT64     NOT NULL,
      ship_jumps           INT64,
      source_last_modified TIMESTAMP
    );
  "
}

load_staging_replace() {
  local file="$1"
  bq --location="$BQ_LOCATION" load \
    --replace=true \
    --source_format=NEWLINE_DELIMITED_JSON \
    --schema="hour_ts:TIMESTAMP,system_id:INT64,ship_jumps:INT64,source_last_modified:TIMESTAMP" \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.system_jumps_stg" \
    "$file"
}

merge_staging_into_final() {
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
    MERGE \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` T
    USING \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps_stg\` S
    ON T.system_id = S.system_id AND T.hour_ts = S.hour_ts
    WHEN MATCHED THEN
      UPDATE SET
        ship_jumps = S.ship_jumps,
        source_last_modified = S.source_last_modified,
        ingested_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (hour_ts, system_id, ship_jumps, source_last_modified, ingested_at)
      VALUES (S.hour_ts, S.system_id, S.ship_jumps, S.source_last_modified, CURRENT_TIMESTAMP());
  "
}

git_commit_state_main() {
  local msg="$1"
  local max_retries=5
  local i=1

  git config user.name  "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"

  while (( i <= max_retries )); do
    git add "$STATE_PATH" >/dev/null 2>&1 || true
    git commit -m "$msg" >/dev/null 2>&1 || true

    # rebase para absorber cambios ajenos en main (si los hay)
    git pull --rebase origin main >/dev/null 2>&1 || true

    if git push origin HEAD:main >/dev/null 2>&1; then
      return 0
    fi

    echo "WARN: push failed (attempt $i/$max_retries). Retrying..." >&2
    sleep 2
    ((i++))
  done

  echo "ERROR: could not push state to main after $max_retries attempts" >&2
  return 1
}

# ------------------- Leer state (main) -------------------
old_etag=""
old_lm=""

if [[ -f "$STATE_PATH" ]]; then
  if jq -e . "$STATE_PATH" >/dev/null 2>&1; then
    old_etag="$(jq -r '.etag // ""' "$STATE_PATH")"
    old_lm="$(jq -r '.last_modified // ""' "$STATE_PATH")"
    [[ "$old_etag" == "null" ]] && old_etag=""
    [[ "$old_lm" == "null" ]] && old_lm=""
  fi
fi

# ------------------- Fetch ESI -------------------
tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir" >&2; else cleanup; fi; exit $rc' EXIT

hdr="$tmpdir/resp.hdr"
body="$tmpdir/resp.json"

url="${ESI_BASE_URL}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"

curl_args=(
  -sS --compressed
  -H "Accept: application/json"
  -H "User-Agent: ${USER_AGENT}"
  -D "$hdr"
  -o "$body"
  -w "%{http_code}"
)

# FORCE=true -> no condicional (reingesta “a la fuerza”)
if [[ "$FORCE" != "true" && -n "$old_etag" ]]; then
  curl_args+=(-H "If-None-Match: ${old_etag}")
fi

code="$(curl "${curl_args[@]}" "$url" || true)"
echo "Status: jumps=$code"

[[ "$code" =~ ^(200|304)$ ]] || { echo "ESI HTTP $code" >&2; exit 1; }

etag_new="$(hdr_get "$hdr" "ETag")"
lm_raw="$(hdr_get "$hdr" "Last-Modified")"
lm_iso="$(to_rfc3339z_or_empty "$lm_raw")"
updated_at="$(now_rfc3339z)"

# ------------------- Decide “changed” -------------------
changed=false

if [[ "$FORCE" == "true" && "$code" == "200" ]]; then
  changed=true
elif [[ "$code" == "200" ]]; then
  if [[ -n "$etag_new" && "$etag_new" != "$old_etag" ]]; then
    changed=true
  elif [[ -z "$etag_new" && -n "$lm_iso" && "$lm_iso" != "$old_lm" ]]; then
    changed=true
  elif [[ -z "$old_etag" && -n "$etag_new" ]]; then
    changed=true
  fi
fi

# ------------------- BigQuery (staging + MERGE anti-duplicados) -------------------
BQ_LOCATION="$(detect_bq_location || true)"
[[ -n "$BQ_LOCATION" ]] || { echo "ERROR: Could not detect dataset location" >&2; exit 1; }

if [[ "$changed" == "true" && "$DRY_RUN" != "true" ]]; then
  jq -e 'type=="array"' "$body" >/dev/null

  source_ts="$lm_iso"
  [[ -n "$source_ts" ]] || source_ts="$updated_at"

  hour_ts="$(truncate_to_hour_rfc3339z "$source_ts")"
  [[ -n "$hour_ts" ]] || { echo "ERROR: could not compute hour_ts from $source_ts" >&2; exit 1; }

  ensure_tables

  ndjson="$tmpdir/data.ndjson"
  jq -c --arg hour "$hour_ts" --arg lm "$source_ts" '
    .[] | {
      hour_ts: $hour,
      system_id: (.system_id|tonumber),
      ship_jumps: (.ship_jumps|tonumber),
      source_last_modified: $lm
    }' "$body" > "$ndjson"

  load_staging_replace "$ndjson"
  merge_staging_into_final

  echo "Upserted system_jumps hour_ts=$hour_ts (anti-duplicates via MERGE)"
else
  echo "No ingestion performed (unchanged or DRY_RUN=true)."
fi

# ------------------- Construir state mínimo -------------------
new_etag="$old_etag"
new_lm="$old_lm"

if [[ "$changed" == "true" ]]; then
  [[ -n "$etag_new" ]] && new_etag="$etag_new"
  [[ -n "$lm_iso" ]] && new_lm="$lm_iso"
  [[ -z "$lm_iso" ]] && new_lm="$updated_at"
fi

mkdir -p "$(dirname "$STATE_PATH")"
jq -cn --arg etag "$new_etag" --arg lm "$new_lm" '
  {
    etag: (if $etag=="" then null else $etag end),
    last_modified: (if $lm=="" then null else $lm end)
  }' > "$STATE_PATH"

echo "State updated (working tree): $STATE_PATH"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY_RUN=true → no commit to main."
  exit 0
fi

git_commit_state_main "orch: update state ${ENDPOINT} ($(now_rfc3339z))"
echo "State committed to main: $STATE_PATH"
