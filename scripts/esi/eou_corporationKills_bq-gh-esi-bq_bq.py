from __future__ import annotations

"""
Funciones de acceso a BigQuery para el pipeline corporation_kills.

Responsabilidades:
- crear el cliente,
- cargar la SQL base,
- obtener la lista de corporation_id únicos,
- leer la tabla destino actual solo cuando se necesita fallback,
- recrear la tabla final en BigQuery.
"""

import json
import uuid
from itertools import islice
from pathlib import Path
from typing import Iterable

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def make_client(project_id: str, location: str) -> bigquery.Client:
    """Crea un cliente de BigQuery asociado al proyecto y ubicación."""
    return bigquery.Client(project=project_id, location=location)


def load_sql(path: Path, source_table: str) -> str:
    """
    Carga la consulta SQL desde fichero y sustituye la tabla fuente.

    El marcador {{SOURCE_TABLE}} se reemplaza por la tabla real definida
    en variables de entorno del workflow.
    """
    sql_text = path.read_text(encoding="utf-8")
    return sql_text.replace("{{SOURCE_TABLE}}", source_table)


def fetch_corporation_ids(
    client: bigquery.Client,
    sql_text: str,
) -> list[int]:
    """
    Ejecuta la consulta de extracción y devuelve corporation_id únicos.
    """
    job = client.query(sql_text)
    rows = job.result()
    return [int(row["corporation_id"]) for row in rows]


def fetch_existing_mapping(client: bigquery.Client, target_table: str) -> dict[int, str]:
    """
    Lee la tabla destino actual y devuelve el mapeo corporation_id -> corporation.

    Solo se usa como fallback cuando ESI no resuelve todos los IDs.
    """
    try:
        client.get_table(target_table)
    except NotFound:
        return {}

    sql = f"""
    SELECT corporation_id, corporation
    FROM `{target_table}`
    """
    rows = client.query(sql).result()
    return {int(row["corporation_id"]): str(row["corporation"]) for row in rows}


def _replace_with_empty_table(
    client: bigquery.Client,
    location: str,
    target_table: str,
) -> None:
    """
    Recrea la tabla destino vacía manteniendo el esquema esperado.

    Esto permite que la tabla derivada refleje correctamente una fuente vacía.
    """
    sql = f"""
    CREATE OR REPLACE TABLE `{target_table}` (
      corporation_id INT64,
      corporation STRING
    )
    """
    client.query(sql, location=location).result()


def replace_target_table(
    client: bigquery.Client,
    project_id: str,
    location: str,
    staging_table: str,
    target_table: str,
    rows: Iterable[dict[str, object]],
) -> int:
    """
    Recrea la tabla destino a partir de las filas finales canónicas.

    Flujo:
    1) carga JSON a una tabla temporal,
    2) hace CREATE OR REPLACE TABLE sobre la tabla final,
    3) elimina la tabla temporal.
    """
    rows = list(rows)
    if not rows:
        _replace_with_empty_table(client=client, location=location, target_table=target_table)
        return 0

    dataset_id = target_table.split(".", 2)[1]
    temp_table_id = f"{project_id}.{dataset_id}.{staging_table.split('.')[-1]}_{uuid.uuid4().hex[:8]}"

    schema = [
        bigquery.SchemaField("corporation_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("corporation", "STRING", mode="REQUIRED"),
    ]

    load_job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_json(
            rows,
            temp_table_id,
            job_config=load_job_config,
        )
        load_job.result()

        create_sql = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT corporation_id, corporation
        FROM `{temp_table_id}`
        ORDER BY corporation_id ASC
        """
        query_job = client.query(create_sql, location=location)
        query_job.result()
    finally:
        client.delete_table(temp_table_id, not_found_ok=True)

    return len(rows)


def render_preview(rows: Iterable[dict[str, object]], limit: int = 5) -> str:
    """
    Devuelve una vista reducida en JSON.

    Se conserva para no alterar la estructura del pipeline, aunque actualmente
    no se emite en logs visibles.
    """
    sample = list(islice(rows, limit))
    return json.dumps(sample, ensure_ascii=False)
