#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR at line $LINENO";' ERR

echo "SCRIPT VERSION: $(git rev-parse --short HEAD 2>/dev/null || echo no-git)"

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

# Fallback si el JSON está corrupto
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
prev_next_eligible="$(echo "$line1" | jq -r '.next_eligible_run_at // ""' 2>/dev/null || echo "")"

# Anti-polling: si no FORCE y todavía no llegamos a next_eligible_run_at, salir sin tocar ESI
if [[ "${FORCE}" != "true" && -n "$prev_next_eligible" && "$prev_next_eligible" != "null" ]]; then
  prev_next_epoch="$(date -u -d "$prev_next_eligible" +%s 2>/dev/null || echo 0)"
  if (( now_epoch < prev_next_epoch )); then
    echo "Too early. next_eligible_run_at=$prev_next_eligible (now=$now_iso). Exiting without ESI calls."
    exit 0
  fi
fi

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir"; else cleanup; fi; exit $rc' EXIT

# Header extractor robusto (case-insensitive, CRLF safe)
hdr_get() {
  local file="$1" key="$2"
  # last match por si hay múltiples bloques de headers
  grep -i "^${key}:" "$file" | tail -n 1 | cut -d: -f2- | tr -d '\r' | xargs || true
}

to_iso_or_empty() {
  local rfc="$1"
  if [[ -z "$rfc" ]]; then echo ""; return; fi
  date -u -d "$rfc" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo ""
}

epoch_from_rfc() {
  local rfc="$1"
  [[ -n "$rfc" ]] || { echo 0; return; }
  date -u -d "$rfc" +%s 2>/dev/null || echo 0
}

detect_bq_location() {
  local out json
  # Captura stdout+stderr; a veces el CLI mete WARNING en stdout
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  if [[ -z "$json" ]]; then return 0; fi
  if echo "$json" | jq -e . >/dev/null 2>&1; then
    echo "$json" | jq -r '.location // empty'
  else
    return 0
  fi
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

echo "---- Debug headers (first 30 lines) ----"
echo "[jumps headers]"; head -n 30 "$tmpdir/jumps.hdr" || true
echo "[kills headers]"; head -n 30 "$tmpdir/kills.hdr" || true
echo "----------------------------------------"

# Aceptamos solo 200 o 304 (cualquier otra cosa: abortar para no quemar budget)
if [[ ! "$code_jumps" =~ ^(200|304)$ ]]; then
  echo "ESI jumps returned HTTP $code_jumps — aborting to avoid burning error budget."
  exit 1
fi
if [[ ! "$code_kills" =~ ^(200|304)$ ]]; then
  echo "ESI kills returned HTTP $code_kills — aborting to avoid burning error budget."
  exit 1
fi

# Log de budget (solo informativo)
err_remain_j="$(hdr_get "$tmpdir/jumps.hdr" "X-ESI-Error-Limit-Remain")"
err_reset_j="$(hdr_get "$tmpdir/jumps.hdr" "X-ESI-Error-Limit-Reset")"
err_remain_k="$(hdr_get "$tmpdir/kills.hdr" "X-ESI-Error-Limit-Remain")"
err_reset_k="$(hdr_get "$tmpdir/kills.hdr" "X-ESI-Error-Limit-Reset")"
echo "ESI error budget (jumps): remain=${err_remain_j:-?}, reset_s=${err_reset_j:-?}"
echo "ESI error budget (kills): remain=${err_remain_k:-?}, reset_s=${err_reset_k:-?}"

# ---- Parse headers (SIEMPRE, haya ingest o no) ----
etag_jumps_hdr="$(hdr_get "$tmpdir/jumps.hdr" "ETag")"
etag_kills_hdr="$(hdr_get "$tmpdir/kills.hdr" "ETag")"
expires_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Expires")"
expires_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Expires")"
lm_jumps_raw="$(hdr_get "$tmpdir/jumps.hdr" "Last-Modified")"
lm_kills_raw="$(hdr_get "$tmpdir/kills.hdr" "Last-Modified")"

# Normaliza ISO (lo guardamos siempre en state)
expires_jumps_iso="$(to_iso_or_empty "$expires_jumps_raw")"
expires_kills_iso="$(to_iso_or_empty "$expires_kills_raw")"
lm_jumps_iso="$(to_iso_or_empty "$lm_jumps_raw")"
lm_kills_iso="$(to_iso_or_empty "$lm_kills_raw")"

# next_eligible_run_at = max(Expires) + 60s
expires_j_epoch="$(epoch_from_rfc "$expires_jumps_raw")"
expires_k_epoch="$(epoch_from_rfc "$expires_kills_raw")"
max_exp_epoch=$(( expires_j_epoch > expires_k_epoch ? expires_j_epoch : expires_k_epoch ))
next_eligible_epoch=$(( max_exp_epoch + 60 ))
next_eligible_iso="$(date -u -d "@$next_eligible_epoch" +"%Y-%m-%d %H:%M:%S+00:00" 2>/dev/null || echo "")"

# Fallback de ETag si viene vacío (no debería)
etag_jumps_new="$etag_jumps_hdr"
etag_kills_new="$etag_kills_hdr"
[[ -n "$etag_jumps_new" ]] || etag_jumps_new="$old_etag_jumps"
[[ -n "$etag_kills_new" ]] || etag_kills_new="$old_etag_kills"

# Detecta cambios (tu regla: solo ingerir si los dos cambiaron)
changed_jumps=false
changed_kills=false
if [[ "$code_jumps" == "200" && -n "$etag_jumps_new" && "$etag_jumps_new" != "$old_etag_jumps" ]]; then changed_jumps=true; fi
if [[ "$code_kills" == "200" && -n "$etag_kills_new" && "$etag_kills_new" != "$old_etag_kills" ]]; then changed_kills=true; fi

should_ingest=false
if [[ "$changed_jumps" == "true" && "$changed_kills" == "true" ]]; then
  should_ingest=true
fi

# Si alguno es 304, NO ingerir (y no intentar parsear body)
if [[ "$code_jumps" == "304" || "$code_kills" == "304" ]]; then
  echo "At least one endpoint returned 304; skipping body parsing and ingestion."
  should_ingest=false
fi

echo "Status: jumps=$code_jumps changed=$changed_jumps | kills=$code_kills changed=$changed_kills | ingest=$should_ingest"

# ---- Validación JSON solo si vamos a ingerir ----
if [[ "$should_ingest" == "true" ]]; then
  jq -e . "$tmpdir/jumps.json" >/dev/null 2>&1 || { echo "ERROR: jumps body not valid JSON"; head -c 400 "$tmpdir/jumps.json"; exit 1; }
  jq -e . "$tmpdir/kills.json" >/dev/null 2>&1 || { echo "ERROR: kills body not valid JSON"; head -c 400 "$tmpdir/kills.json"; exit 1; }
  jq -e 'type=="array"' "$tmpdir/jumps.json" >/dev/null 2>&1 || { echo "ERROR: jumps JSON not array"; exit 1; }
  jq -e 'type=="array"' "$tmpdir/kills.json" >/dev/null 2>&1 || { echo "ERROR: kills JSON not array"; exit 1; }
fi

# ---- BigQuery ----
BQ_LOCATION="$(detect_bq_location || true)"
if [[ -z "$BQ_LOCATION" ]]; then
  echo "ERROR: Could not detect dataset location for ${GCP_PROJECT_ID}:${BQ_DATASET}"
  echo "Refusing to default to US."
  exit 1
fi
echo "BQ_LOCATION=$BQ_LOCATION"

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
  "
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

# ---- Ingest ----
if [[ "$should_ingest" == "true" && "$DRY_RUN" != "true" ]]; then
  ensure_tables

  # Tu requisito: consolidated_at = Last-Modified (por endpoint)
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
# Reglas:
# - SIEMPRE guardar last_modified_* / expires_* / next_eligible_run_at desde headers del run
# - Solo actualizar etags persistidos si hubo ingesta (ambos cambiaron)
new_etag_jumps="$old_etag_jumps"
new_etag_kills="$old_etag_kills"
if [[ "$should_ingest" == "true" ]]; then
  new_etag_jumps="$etag_jumps_new"
  new_etag_kills="$etag_kills_new"
fi

line1_new="$(jq -cn \
  --arg type "ESI_ETAG_STATE" \
  --arg updated_at "$now_iso" \
  --arg workflow "$workflow_yaml" \
  --arg etag_jumps "$new_etag_jumps" \
  --arg etag_kills "$new_etag_kills" \
  --arg last_modified_jumps "$lm_jumps_iso" \
  --arg last_modified_kills "$lm_kills_iso" \
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
  git commit -m "orch: update ESI system stats state ($now_iso)"
  git push
  echo "State committed."
else
  echo "No state changes to commit."
fi
