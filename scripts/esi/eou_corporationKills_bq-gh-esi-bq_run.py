from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent


def load_local_module(filename: str, alias: str):
    module_path = BASE_DIR / filename
    spec = importlib.util.spec_from_file_location(alias, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


log_mod = load_local_module("eou_corporationKills_bq-gh-esi-bq_log.py", "corp_log")
state_mod = load_local_module("eou_corporationKills_bq-gh-esi-bq_state.py", "corp_state")
hash_mod = load_local_module("eou_corporationKills_bq-gh-esi-bq_hash.py", "corp_hash")
bq_mod = load_local_module("eou_corporationKills_bq-gh-esi-bq_bq.py", "corp_bq")
esi_mod = load_local_module("eou_corporationKills_bq-gh-esi-bq_esi.py", "corp_esi")


logger = log_mod.configure_logging()


def set_output(name: str, value: Any) -> None:
    github_output = os.getenv("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def main() -> int:
    workspace = Path(os.getenv("GITHUB_WORKSPACE", Path.cwd()))
    state_path = workspace / os.environ["STATE_FILE"]
    sql_path = workspace / os.environ["SQL_IDS_FILE"]

    project_id = os.environ["GCP_PROJECT_ID"]
    location = os.getenv("BQ_LOCATION", "EU")
    source_table = os.environ["BQ_SOURCE_TABLE"]
    target_table = os.environ["BQ_TARGET_TABLE"]
    staging_table = os.environ["BQ_STAGING_TABLE"]
    force_refresh = os.getenv("FORCE_REFRESH", "false").lower() == "true"
    esi_user_agent = os.environ["ESI_USER_AGENT"]

    now_utc = state_mod.utcnow()
    state = state_mod.read_state(state_path)

    logger.info("pipeline start")
    logger.info("state_file=%s", state_path)
    logger.info("sql_file=%s", sql_path)
    logger.info("source_table=%s", source_table)
    logger.info("target_table=%s", target_table)
    logger.info("force_refresh=%s", force_refresh)

    client = bq_mod.make_client(project_id=project_id, location=location)
    sql_text = bq_mod.load_sql(sql_path, source_table=source_table)
    corporation_ids = bq_mod.fetch_corporation_ids(client, sql_text)
    logger.info("bigquery corporation_ids=%s", len(corporation_ids))

    resolver = esi_mod.EsiResolver(
        logger=logger,
        user_agent=esi_user_agent,
    )
    resolved_mapping, unresolved_ids = resolver.resolve(corporation_ids)

    logger.info(
        "esi resolved=%s unresolved=%s batches=%s transient_retries=%s split_retries=%s min_error_limit_remain=%s",
        len(resolved_mapping),
        len(unresolved_ids),
        resolver.stats["batches_attempted"],
        resolver.stats["transient_retries"],
        resolver.stats["split_retries"],
        resolver.stats["min_error_limit_remain"],
    )

    existing_mapping: dict[int, str] = {}
    if unresolved_ids:
        existing_mapping = bq_mod.fetch_existing_mapping(client, target_table)
        logger.info("bigquery existing_rows=%s", len(existing_mapping))

    fallback_hits = 0
    missing_ids: list[int] = []
    final_rows: list[dict[str, object]] = []

    for corporation_id in corporation_ids:
        if corporation_id in resolved_mapping:
            final_rows.append(
                {
                    "corporation_id": corporation_id,
                    "corporation": resolved_mapping[corporation_id],
                }
            )
            continue

        if corporation_id in existing_mapping:
            fallback_hits += 1
            final_rows.append(
                {
                    "corporation_id": corporation_id,
                    "corporation": existing_mapping[corporation_id],
                }
            )
            continue

        missing_ids.append(corporation_id)

    logger.info("fallback_hits=%s missing_ids=%s", fallback_hits, len(missing_ids))
    if missing_ids:
        logger.warning("missing corporation_ids without fallback=%s", missing_ids[:20])

    new_hash, canonical_rows = hash_mod.compute_hash(final_rows)
    previous_hash = state["hash"] if state else None
    age_days = None
    if state:
        age_days = (now_utc - state["last-modified"]).total_seconds() / 86400.0

    should_write = False
    reason = "skip_same_hash"

    if force_refresh:
        should_write = True
        reason = "force_refresh"
    elif missing_ids:
        should_write = False
        reason = "skip_partial_esi_errors"
    elif state is None:
        should_write = True
        reason = "initial_load"
    elif new_hash != previous_hash:
        should_write = True
        reason = "hash_changed"
    elif age_days is not None and age_days > 55:
        should_write = True
        reason = "ttl_refresh"

    logger.info(
        "decision=%s should_write=%s previous_hash=%s new_hash=%s age_days=%s preview=%s",
        reason,
        should_write,
        previous_hash,
        new_hash,
        None if age_days is None else round(age_days, 3),
        bq_mod.render_preview(canonical_rows),
    )

    table_written = False
    state_changed = False

    if should_write:
        row_count = bq_mod.replace_target_table(
            client=client,
            project_id=project_id,
            location=location,
            staging_table=staging_table,
            target_table=target_table,
            rows=canonical_rows,
        )
        logger.info("bigquery write complete rows=%s", row_count)

        state_mod.write_state(
            state_path,
            hash_value=new_hash,
            last_modified=now_utc,
        )
        table_written = True
        state_changed = True

    set_output("table_written", str(table_written).lower())
    set_output("state_changed", str(state_changed).lower())
    set_output("resolved_count", len(resolved_mapping))
    set_output("unresolved_count", len(unresolved_ids))
    set_output("decision_reason", reason)

    logger.info("pipeline end")
    return 0


if __name__ == "__main__":
    sys.exit(main())
