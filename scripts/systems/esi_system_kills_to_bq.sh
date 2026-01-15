#!/usr/bin/env bash
set -euo pipefail

trap 'echo "ERROR at line $LINENO" >&2' ERR

: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${USER_AGENT:?Missing USER_AGENT}"

BQ_DATASET="${BQ_DATASET:-eou}"
ESI_BASE_URL="${ESI_BASE_URL:-https://esi.evetech.net/latest}"
ESI_DATASOURCE="${ESI_DATASOURCE:-tranquility}"

ENDPOINT="${ENDPOINT:-system_kills}"
STATE_FILE="${STATE_FILE:-.orch/state/system_kills.json}"
STATE_BRANCH="${STATE_BRANCH:-orch-state-system-kills}"

FORCE="${FORCE:-false}"
DRY_RUN="${DRY_RUN:-false}"

workflow_ref="${GITHUB_WORKFLOW_REF:-unknown}"
workflow_yaml="$(echo "$workflow_ref" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"

now_rfc3339() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

to_rfc3339z_or_empty() {
  local http_date="$1"
  [[ -z "$http_date" ]] && { echo ""; return; }
  date -u -d "$http_date" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo ""
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

ensure_table() {
  bq --location="$BQ_LOCATION" query --use_legacy_sql=false "
    CREATE TABLE IF NOT EXISTS \`${GCP_PROJECT_ID}.${BQ_DATASET}.system_kills\` (
      ts         TIMESTAMP NOT NULL,
      system_id  INT64     NOT NULL,
      ship_kills INT64,
      npc_kills  INT64,
      pod_kills  INT64
    )
    PARTITION BY DATE(ts)
    CLUSTER BY system_id;
  "
}

load_ndjson_append() {
  local table="$1" file="$2"
  bq --location="$BQ_LOCATION" load \
    --noreplace \
    --source_format=NEWLINE_DELIMITED_JSON \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${table}" \
    "$file"
}

mkdir -p "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

state_raw="$(cat "$STATE_FILE" 2>/dev/null || true)"
if ! echo "$state_raw" | jq -e . >/dev/null 2>&1; then
  state_raw='{"type":"ESI_ENDPOINT_STATE","endpoint":"system_kills","updated_at":null,"workflow":"wof-esi-bq-system-kills.yml","etag":null,"last_modified":null,"expires":null,"next_eligible_run_at":null}'
fi

old_etag="$(echo "$state_raw" | jq -r '.etag // ""')"
next_eligible="$(echo "$state_raw" | jq -r '.next_eligible_run_at // ""')"

if [[ "$FORCE" != "true" && -n "$next_eligible" && "$next_eligible" != "null" ]]; then
  now_epoch="$(date -u +%s)"
  next_epoch="$(date -u -d "$next_eligible" +%s 2>/dev/null || echo 0)"
  if (( now_epoch < next_epoch )); then
    echo "Too early. next_eligible_run_at=$next_eligible (now=$(now_rfc3339)). Exit."
    exit 0
  fi
fi

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "Keeping tmpdir for debugging: $tmpdir" >&2; else cleanup; fi; exit $rc' EXIT

hdr="$tmpdir/resp.hdr"
body="$tmpdir/resp.json"

url="${ESI_BASE_URL}/universe/system_kills/?datasource=${ESI_DATASOURCE}"

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
echo "Status: kills=$code"

[[ "$code" =~ ^(200|304)$ ]] || { echo "ESI HTTP $code" >&2; exit 1; }

etag_new="$(hdr_get "$hdr" "ETag")"
expires_raw="$(hdr_get "$hdr" "Expires")"
lm_raw="$(hdr_get "$hdr" "Last-Modified")"

expires_iso="$(to_rfc3339z_or_empty "$expires_raw")"
lm_iso="$(to_rfc3339z_or_empty "$lm_raw")"
updated_at="$(now_rfc3339)"

exp_epoch=0
[[ -n "$expires_raw" ]] && exp_epoch="$(date -u -d "$expires_raw" +%s 2>/dev/null || echo 0)"
next_epoch=$(( exp_epoch + 60 ))
next_iso="$(date -u -d "@$next_epoch" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")"

changed=false
if [[ "$code" == "200" && -n "$etag_new" && "$etag_new" != "$old_etag" ]]; then
  changed=true
fi

BQ_LOCATION="$(detect_bq_location || true)"
[[ -n "$BQ_LOCATION" ]] || { echo "ERROR: Could not detect dataset location" >&2; exit 1; }

if [[ "$changed" == "true" && "$DRY_RUN" != "true" ]]; then
  jq -e 'type=="array"' "$body" >/dev/null

  ensure_table

  ts_val="$lm_iso"
  [[ -n "$ts_val" ]] || ts_val="$updated_at"

  ndjson="$tmpdir/data.ndjson"
  jq -c --arg ts "$ts_val" '
    .[] | {
      ts: $ts,
      system_id: (.system_id|tonumber),
      ship_kills: (.ship_kills|tonumber),
      npc_kills:  (.npc_kills|tonumber),
      pod_kills:  (.pod_kills|tonumber)
    }' "$body" > "$ndjson"

  load_ndjson_append "system_kills" "$ndjson"
  echo "Ingested system_kills snapshot ts=$ts_val"
else
  echo "No ingestion performed (unchanged or DRY_RUN=true)."
fi

new_etag="$old_etag"
new_lm="$(echo "$state_raw" | jq -r '.last_modified // ""')"
[[ "$new_lm" == "null" ]] && new_lm=""

if [[ "$changed" == "true" ]]; then
  [[ -n "$etag_new" ]] && new_etag="$etag_new"
  [[ -n "$lm_iso" ]] && new_lm="$lm_iso"
fi

state_new="$(jq -cn \
  --arg type "ESI_ENDPOINT_STATE" \
  --arg endpoint "$ENDPOINT" \
  --arg updated_at "$updated_at" \
  --arg workflow "$workflow_yaml" \
  --arg etag "$new_etag" \
  --arg last_modified "$new_lm" \
  --arg expires "$expires_iso" \
  --arg next_eligible_run_at "$next_iso" \
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

printf "%s\n" "$state_new" > "$STATE_FILE"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY_RUN=true → no commit."
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

git fetch origin "$STATE_BRANCH" 2>/dev/null || true

if git show-ref --verify --quiet "refs/heads/$STATE_BRANCH"; then
  git checkout "$STATE_BRANCH"
elif git show-ref --verify --quiet "refs/remotes/origin/$STATE_BRANCH"; then
  git checkout -b "$STATE_BRANCH" "origin/$STATE_BRANCH"
else
  git checkout -b "$STATE_BRANCH"
fi

git add "$STATE_FILE"

if git diff --cached --quiet; then
  echo "No state changes to commit."
  exit 0
fi

git commit -m "orch: update state ${ENDPOINT} (${updated_at})"
git push -u origin "HEAD:$STATE_BRANCH"
echo "State committed to branch: $STATE_BRANCH"
