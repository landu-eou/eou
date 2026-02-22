#!/usr/bin/env bash
set -euo pipefail

# Anti-queue:
# - If another run of the SAME workflow is already "in_progress", exit 0 without doing work.
# Uses ONLY official GitHub API via GITHUB_TOKEN (no third-party actions).

owner_repo="${GITHUB_REPOSITORY:?}"
repo_api="https://api.github.com/repos/${owner_repo}"

workflow_file="${WORKFLOW_FILE:?}"
run_id="${GITHUB_RUN_ID:?}"
token="${GITHUB_TOKEN:?}"

# Find workflow ID by filename
wf_json="$(curl -fsSL \
  -H "Authorization: Bearer ${token}" \
  -H "Accept: application/vnd.github+json" \
  "${repo_api}/actions/workflows")"

workflow_id="$(python -c "import json,sys; d=json.load(sys.stdin); 
w=[x for x in d.get('workflows',[]) if x.get('path','').endswith('/${workflow_file}') or x.get('name')=='${workflow_file}'];
print(w[0]['id'] if w else '')" <<< "${wf_json}")"

if [[ -z "${workflow_id}" ]]; then
  echo "guard: workflow_id not found for ${workflow_file} (continuing)"
  exit 0
fi

runs_json="$(curl -fsSL \
  -H "Authorization: Bearer ${token}" \
  -H "Accept: application/vnd.github+json" \
  "${repo_api}/actions/workflows/${workflow_id}/runs?status=in_progress&per_page=20")"

# If there is any other in_progress run besides current, exit cleanly
other_in_progress="$(python -c "import json,sys; d=json.load(sys.stdin);
rid='${run_id}';
runs=[r for r in d.get('workflow_runs',[]) if str(r.get('id'))!=rid and r.get('status')=='in_progress'];
print('1' if runs else '0')" <<< "${runs_json}")"

if [[ "${other_in_progress}" == "1" ]]; then
  echo "guard: another run is in progress -> exiting without work"
  exit 0
fi

echo "guard: ok (no other in_progress runs)"
