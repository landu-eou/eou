#!/usr/bin/env bash
set -euo pipefail

# Requiere: GITHUB_TOKEN
# Usa GitHub REST via gh api (gh viene preinstalado en runners hosted).
# Si prefieres puro curl, lo cambio.

wf_file="${WORKFLOW_FILE:-eou_market_orders_esi-gh-bq.yml}"

# repo info
owner_repo="${GITHUB_REPOSITORY}"
run_id="${GITHUB_RUN_ID}"

# Lista runs en progreso del workflow file, excluyendo el propio run.
# Si hay alguno, salimos 0 (sin trabajo).
in_progress_count="$(gh api \
  -H "Accept: application/vnd.github+json" \
  "/repos/${owner_repo}/actions/workflows/${wf_file}/runs?status=in_progress&per_page=50" \
  --jq ".workflow_runs | map(select(.id != ${run_id})) | length")"

if [[ "${in_progress_count}" -gt 0 ]]; then
  echo "guard: another run is in progress (${in_progress_count}). exiting without work."
  exit 0
fi

echo "guard: ok (no other in-progress runs)"
