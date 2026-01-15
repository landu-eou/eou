#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR at line $LINENO" >&2' ERR

# -----------------------------
# Required env
# -----------------------------
: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${USER_AGENT:?Missing USER_AGENT}"

# -----------------------------
# Optional env (defaults)
# -----------------------------
BQ_DATASET="${BQ_DATASET:-eou}"
STATE_FILE="${STATE_FILE:-.orch/state.jsonl}"
ESI_BASE_URL="${ESI_BASE_URL:-https://esi.evetech.net/latest}"
ESI_DATASOURCE="${ESI_DATASOURCE:-tranquility}"
FORCE="${FORCE:-false}"
DRY_RUN="${DRY_RUN:-false}"

WORKFLOW_REF="${GITHUB_WORKFLOW_REF:-unknown}"
WORKFLOW_YAML="$(echo "$WORKFLOW_REF" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"
NOW_ISO_Z="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
NOW_EPOCH="$(date -u +%s)"

mkdir -p "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

# -----------------------------
# Helpers
# -----------------------------
hdr_get() {
  local file="$1" key="$2"
  grep -i "^${key}:" "$file" | tail -n 1 | cut -d: -f2- | tr -d '\r' | xargs || true
}

rfc1123_to_rfc3339z_or_empty() {
  local rfc="$1"
  [[ -z "$rfc" ]] && { echo ""; return; }
  date -u -d "$rfc" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo ""
}

epoch_from_rfc1123_or_zero() {
  local rfc="$1"
  [[ -z "$rfc" ]] && { echo 0; return; }
  date -u -d "$rfc" +%s 2>/dev/null || echo 0
}

detect_bq_location() {
  # bq show puede mezclar warnings; extraemos el JSON desde la primera "{"
  local out json
  out="$(bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>&1 || true)"
  json="$(printf '%s\n' "$out" | awk 'BEGIN{p=0} /^[[:space:]]*{/{p=1} p{print}')"
  [[ -z "$json" ]] && echo "" && return
  echo "$json" | jq -r '.location // empty' 2>/dev/null || echo ""
}

ensure_state_file_shape() {
  # Forzamos 3 líneas fijas. Si faltan, las inicializamos.
  local l1 l2 l3
  l1="$(sed -n '1p' "$STATE_FILE" 2>/dev/null || true)"
  l2="$(sed -n '2p' "$STATE_FILE" 2>/dev/null || true)"
  l3="$(sed -n '3p' "$STATE_FILE" 2>/dev/null || true)"

  echo "${l1:-{"type":"ESI_ENDPOINT_STATE","endpoint":"system_jumps","updated_at":null,"workflow":"wof-esi-bq-system-jumps.yml","etag":null,"last_modified":null,"expires":null,"next_eligible_run_at":null}}" \
    | jq -e . >/dev/null 2>&1 || l1='{"type":"ESI_ENDPOINT_STATE","endpoint":"system_jumps","updated_at":null,"workflow":"wof-esi-bq-system-jumps.yml","etag":null,"last_modified":null,"expires":null,"next_eligible_run_at":null}'

  echo "${l2:-{"type":"ESI_ENDPOINT_STATE","endpoint":"system_kills","updated_at":null,"workflow":"wof-esi-bq-system-kills.yml","etag":null,"last_modified":null,"expires":null,"next_eligible_run_at":null}}" \
    | jq -e . >/dev/null 2>&1 || l2='{"type":"ESI_ENDPOINT_STATE","endpoint":"system_kills","updated_at":null,"workflow":"wof-esi-bq-system-kills.yml","etag":null,"last_modified":null,"expires":null,"next_eligible_run_at":null}'

  echo "${l3:-{"type":"BQ_REFRESH_STATE","updated_at":null,"workflow":null,"last_refresh_at":null,"next_refresh_at":null}}" \
    | jq -e . >/dev/null 2>&1 || l3='{"type":"BQ_REFRESH_STATE","updated_at":null,"workflow":null,"last_refresh_at":null,"next_refresh_at":null}'

  printf '%s\n%s\n%s\n' "$l1" "$l2" "$l3" > "$STATE_FILE"
}

ensure_table() {
  # Tabla: eou.system_jumps
  # Columna: ts TIMESTAMP NOT NULL (derivada de Last-Modified)
  # Partición: DATE(ts)
  # Cluster: system_id
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
    CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_jumps\` (
      ts         TIMESTAMP NOT NULL,
      system_id   INT64     NOT NULL,
      ship_jumps  INT64
    )
    PARTITION BY DATE(ts)
    CLUSTER BY system_id;
  "
}

load_ndjson_append() {
  local file="$1"
  bq --location="$BQ_LOCATION" load \
    --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.system_jumps" \
    "$file"
}

commit_state_if_changed() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY_RUN=true → no commit."
    return 0
  fi

  if git diff --quiet -- "$STATE_FILE"; then
    echo "State unchanged → no commit."
    return 0
  fi

  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git add "$STATE_FILE"
  git commit -m "orch: update system_jumps state (${NOW_ISO_Z})"
  git push
  echo "State committed."
}

# -----------------------------
# State read (line 1 = jumps)
# -----------------------------
ensure_state_file_shape

LINE_NUM=1
line="$(sed -n "${LINE_NUM}p" "$STATE_FILE")"

old_etag="$(echo "$line" | jq -r '.etag // ""')"
next_eligible="$(echo "$line" | jq -r '.next_eligible_run_at // ""')"

# Gate: si todavía no toca, salimos antes de llamar a ESI
if [[ "$FORCE" != "true" && -n "$next_eligible" && "$next_eligible" != "null" ]]; then
  next_epoch="$(date -u -d "$next_eligible" +%s 2>/dev/null || echo 0)"
  if (( NOW_EPOCH < next_epoch )); then
    echo "Too early. next_eligible_run_at=$next_eligible (now=$NOW_ISO_Z). Exit."
    exit 0
  fi
fi

# -----------------------------
# Fetch ESI (conditional)
# -----------------------------
tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir" >&2; else cleanup; fi; exit $rc' EXIT

hdr="$tmpdir/jumps.hdr"
body="$tmpdir/jumps.json"
code_file="$tmpdir/jumps.code"

url="${ESI_BASE_URL}/universe/system_jumps/?datasource=${ESI_DATASOURCE}"

curl_args=(
  -sS --compressed
  -H "Accept: application/json"
  -H "User-Agent: ${USER_AGENT}"
  -D "$hdr"
  -o "$body"
  -w "%{http_code}"
)

[[ -n "$old_etag" ]] && curl_args+=(-H "If-None-Match: ${old_etag}")

code="$(curl "${curl_args[@]}" "$url" || true)"
echo "$code" > "$code_file"

[[ "$code" =~ ^(200|304)$ ]] || { echo "ESI system_jumps HTTP $code" >&2; exit 1; }

etag_new="$(hdr_get "$hdr" "ETag")"
expires_raw="$(hdr_get "$hdr" "Expires")"
lm_raw="$(hdr_get "$hdr" "Last-Modified")"

# Normalización RFC3339 Z
expires_z="$(rfc1123_to_rfc3339z_or_empty "$expires_raw")"
lm_z="$(rfc1123_to_rfc3339z_or_empty "$lm_raw")"

# next_eligible_run_at = expires + 60s
exp_epoch="$(epoch_from_rfc1123_or_zero "$expires_raw")"
next_epoch=$(( exp_epoch + 60 ))
next_z="$(date -u -d "@$next_epoch" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")"

# Si falta ETag por alguna razón, mantenemos la vieja
[[ -n "$etag_new" ]] || etag_new="$old_etag"

changed=false
if [[ "$code" == "200" && -n "$etag_new" && "$etag_new" != "$old_etag" ]]; then
  changed=true
fi

should_ingest=false
if [[ "$code" == "200" && "$changed" == "true" ]]; then
  should_ingest=true
fi

echo "Status: jumps=$code changed=$changed | ingest=$should_ingest"

# -----------------------------
# BigQuery ingest (solo si hay cambio real)
# -----------------------------
if [[ "$should_ingest" == "true" ]]; then
  # Validación JSON (ESI devuelve array)
  jq -e . "$body" >/dev/null
  jq -e 'type=="array"' "$body" >/dev/null

  # Dataset location
  BQ_LOCATION="$(detect_bq_location)"
  [[ -n "$BQ_LOCATION" ]] || { echo "ERROR: Could not detect dataset location for ${GCP_PROJECT_ID}:${BQ_DATASET}" >&2; exit 1; }

  # Tabla y carga
  ensure_table

  ndjson="$tmpdir/jumps.ndjson"
  jq -c --arg ts "$lm_z" '.[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_jumps: (.ship_jumps|tonumber)
    }' "$body" > "$ndjson"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY_RUN=true → skipping BigQuery load."
  else
    load_ndjson_append "$ndjson"
  fi
fi

# -----------------------------
# Update state line 1
# -----------------------------
line_new="$(jq -cn \
  --arg type "ESI_ENDPOINT_STATE" \
  --arg endpoint "system_jumps" \
  --arg updated_at "$NOW_ISO_Z" \
  --arg workflow "$WORKFLOW_YAML" \
  --arg etag "$etag_new" \
  --arg last_modified "$lm_z" \
  --arg expires "$expires_z" \
  --arg next_eligible_run_at "$next_z" \
  '{
    type: $type,
    endpoint: $endpoint,
    updated_at: $updated_at,
    workflow: $workflow,
    etag: (if $etag=="" then null else $etag end),
    last_modified: (if $last_modified=="" then null else $last_modified end),
    expires: (if $expires=="" then null else $expires end),
    next_eligible_run_at: (if $next_eligible_run_at=="" then null else $next_eligible_run_at end)
  }'
)"

l1="$line_new"
l2="$(sed -n '2p' "$STATE_FILE")"
l3="$(sed -n '3p' "$STATE_FILE")"
printf '%s\n%s\n%s\n' "$l1" "$l2" "$l3" > "$STATE_FILE"

commit_state_if_changed
