#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Missing GCP_PROJECT_ID}"
: "${BQ_DATASET:=eou}"
: "${STATE_FILE:=.orch/state.jsonl}"

FORCE="${FORCE:-false}"
DRY_RUN="${DRY_RUN:-false}"
REFRESH_INTERVAL_DAYS="${REFRESH_INTERVAL_DAYS:-15}"

mkdir -p "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

workflow_ref="${GITHUB_WORKFLOW_REF:-unknown}"
workflow_yaml="$(echo "$workflow_ref" | sed -E 's@^.*\.github/workflows/@@; s/@.*$//')"
now_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
now_epoch="$(date -u +%s)"

line1="$(sed -n '1p' "$STATE_FILE" || true)"
line2="$(sed -n '2p' "$STATE_FILE" || true)"
[[ -n "$line1" ]] || line1='{"type":"ESI_ETAG_STATE"}'
[[ -n "$line2" ]] || line2='{"type":"BQ_REFRESH_STATE"}'

next_refresh="$(echo "$line2" | jq -r '.next_refresh_at // ""')"

if [[ "${FORCE}" != "true" && -n "$next_refresh" && "$next_refresh" != "null" ]]; then
  next_epoch="$(date -u -d "$next_refresh" +%s 2>/dev/null || echo 0)"
  if (( now_epoch < next_epoch )); then
    echo "Too early. next_refresh_at=$next_refresh (now=$now_iso). Exiting."
    exit 0
  fi
fi

detect_bq_location() {
  bq show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}" | jq -r '.location // empty'
}
BQ_LOCATION="$(detect_bq_location || true)"
[[ -n "$BQ_LOCATION" ]] || BQ_LOCATION="US"

table_exists() {
  local t="$1"
  bq --location="$BQ_LOCATION" show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}.${t}" >/dev/null 2>&1
}

table_meta() {
  local t="$1"
  bq --location="$BQ_LOCATION" show --format=prettyjson "${GCP_PROJECT_ID}:${BQ_DATASET}.${t}"
}

copy_table() {
  local src="$1" dst="$2"
  bq --location="$BQ_LOCATION" cp -f \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${src}" \
    "${GCP_PROJECT_ID}:${BQ_DATASET}.${dst}" >/dev/null
}

rm_table() {
  local t="$1"
  bq --location="$BQ_LOCATION" rm -f -t "${GCP_PROJECT_ID}:${BQ_DATASET}.${t}" >/dev/null
}

swap_refresh_one() {
  local t="$1"
  if ! table_exists "$t"; then
    echo "Table ${t} does not exist. Skipping."
    return 0
  fi

  local ts
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  local tmp="${t}__tmp_${ts}"

  echo "Refreshing ${t} via temp ${tmp} ..."

  local src_meta tmp_meta
  src_meta="$(table_meta "$t")"

  # 1) Copy to temp (new table → new creation time)
  copy_table "$t" "$tmp"

  # 2) Validate (metadata, no scan)
  tmp_meta="$(table_meta "$tmp")"

  src_rows="$(echo "$src_meta" | jq -r '.numRows // "0"')"
  tmp_rows="$(echo "$tmp_meta" | jq -r '.numRows // "0"')"
  src_bytes="$(echo "$src_meta" | jq -r '.numBytes // "0"')"
  tmp_bytes="$(echo "$tmp_meta" | jq -r '.numBytes // "0"')"

  if [[ "$src_rows" != "$tmp_rows" || "$src_bytes" != "$tmp_bytes" ]]; then
    echo "Validation failed for ${t}: src(rows=${src_rows},bytes=${src_bytes}) tmp(rows=${tmp_rows},bytes=${tmp_bytes})"
    echo "Keeping original; removing temp."
    rm_table "$tmp" || true
    return 1
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY_RUN=true → not swapping. Cleaning temp."
    rm_table "$tmp" || true
    return 0
  fi

  # 3) Remove original
  rm_table "$t"

  # 4) Copy temp back to original name (creates fresh table)
  copy_table "$tmp" "$t"

  # 5) Remove temp
  rm_table "$tmp"

  echo "Refreshed ${t} successfully."
}

swap_refresh_one "system_jumps"
swap_refresh_one "system_kills"

# Update refresh state line 2
next_epoch=$(( now_epoch + (REFRESH_INTERVAL_DAYS * 86400) ))
next_iso="$(date -u -d "@$next_epoch" +"%Y-%m-%dT%H:%M:%SZ")"

line2_new="$(jq -cn \
  --arg type "BQ_REFRESH_STATE" \
  --arg updated_at "$now_iso" \
  --arg workflow "$workflow_yaml" \
  --arg last_refresh_at "$now_iso" \
  --arg next_refresh_at "$next_iso" \
  '{
    type: $type,
    updated_at: $updated_at,
    workflow: $workflow,
    last_refresh_at: $last_refresh_at,
    next_refresh_at: $next_refresh_at
  }'
)"

printf "%s\n%s\n" "$line1" "$line2_new" > "$STATE_FILE"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY_RUN=true → no commit."
  exit 0
fi

if ! git diff --quiet -- "$STATE_FILE"; then
  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git add "$STATE_FILE"
  git commit -m "orch: refresh BQ tables state ($now_iso)" >/dev/null
  git push >/dev/null
  echo "Refresh state committed."
else
  echo "No refresh state changes to commit."
fi
