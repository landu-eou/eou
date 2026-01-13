#!/usr/bin/env bash
set -euo pipefail

PROJECT="${GCP_PROJECT_ID}"
DATASET="${BQ_DATASET}"
SOFT_CAP_GB="${BQ_SOFT_CAP_GB}"

gb_to_bytes() {
  python - <<PY
print(int(float("$1") * 1024 * 1024 * 1024))
PY
}

SOFT_CAP_BYTES="$(gb_to_bytes "${SOFT_CAP_GB}")"

echo "Checking BigQuery active storage soft-cap: ${SOFT_CAP_GB} GB..."
TOTAL_BYTES="$(bq --project_id="${PROJECT}" query --use_legacy_sql=false --format=csv --quiet \
  "SELECT IFNULL(SUM(size_bytes),0) FROM \`${PROJECT}.${DATASET}.__TABLES__\`" | tail -n 1)"
echo "Current dataset bytes: ${TOTAL_BYTES}"

if [ "${TOTAL_BYTES}" -ge "${SOFT_CAP_BYTES}" ]; then
  echo "Soft-cap reached. Setting ZKB_STOP=1 to avoid exceeding Sandbox limits."
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "ZKB_STOP=1" >> "${GITHUB_ENV}"
  fi
  exit 0
fi

mk_table_if_missing() {
  local table="$1"
  local schema="$2"
  if bq --project_id="${PROJECT}" show --format=none "${DATASET}.${table}" >/dev/null 2>&1; then
    echo "Table ${DATASET}.${table} exists."
  else
    echo "Creating ${DATASET}.${table}..."
    bq --project_id="${PROJECT}" mk --table "${DATASET}.${table}" "${schema}"
  fi
}

mk_table_if_missing "zkb_redisq_raw" "schemas/zkb/rq_raw.json"
mk_table_if_missing "zkb_enrichment_attempts" "schemas/zkb/ea.json"
mk_table_if_missing "zkb_esi_killmail_facts" "schemas/zkb/ekf.json"
mk_table_if_missing "zkb_kills_first_seen" "schemas/zkb/kfs.json"

# Final table: partitioned by request_date
if bq --project_id="${PROJECT}" show --format=none "${DATASET}.zkb_kills_enriched" >/dev/null 2>&1; then
  echo "Table ${DATASET}.zkb_kills_enriched exists."
else
  echo "Creating ${DATASET}.zkb_kills_enriched (partitioned)..."
  bq --project_id="${PROJECT}" mk --table \
    --time_partitioning_type=DAY \
    --time_partitioning_field=request_date \
    "${DATASET}.zkb_kills_enriched" schemas/zkb/ke.json
fi

echo "Bootstrap complete."
