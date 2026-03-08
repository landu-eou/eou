#!/usr/bin/env python3
"""
Cliente HTTP y lógica de fetch paginado para market orders de ESI.

Este archivo implementa:
- Session reutilizable con pool de conexiones
- parseo y ordenación del pool de access tokens desde GitHub Secrets
- política de paginación / retries / backoff / rotación de tokens
- normalización mínima de órdenes para SQLite

No imprime logs informativos del cliente. Solo lanza excepciones reales.
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter


def _safe_int(value, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def parse_token_secret(secret_value: Optional[str]) -> Dict[str, str]:
    """
    Parseo tolerante de un secret JSON object:
      {"character_id": "access_token", ...}

    Si está vacío o no es JSON válido, devuelve {}.
    Nunca imprime tokens.
    """
    if not secret_value:
        return {}

    secret_value = secret_value.strip()
    if not secret_value:
        return {}

    try:
        raw = json.loads(secret_value)
    except Exception:
        return {}

    if not isinstance(raw, dict):
        return {}

    out: Dict[str, str] = {}
    for key, value in raw.items():
        char_id = str(key).strip()
        token = str(value).strip() if value is not None else ""
        if char_id and token:
            out[char_id] = token
    return out


def build_ordered_token_pairs_from_env(env: Optional[dict] = None) -> List[Tuple[str, str]]:
    """
    Construye el pool final de tokens desde:
      - EOU_ACCESS_TOKENS_1
      - EOU_ACCESS_TOKENS_2
      - PRIMARY_CHAR_ID

    Orden final:
      1) PRIMARY_CHAR_ID si existe
      2) resto ordenado por character_id desc
    """
    env = env or os.environ

    t1 = parse_token_secret(env.get("EOU_ACCESS_TOKENS_1"))
    t2 = parse_token_secret(env.get("EOU_ACCESS_TOKENS_2"))

    merged: Dict[str, str] = {}
    merged.update(t1)
    merged.update(t2)

    primary_char_id = str(env.get("PRIMARY_CHAR_ID", "")).strip()

    ordered_ids = sorted(merged.keys(), key=lambda x: int(x), reverse=True)
    ordered_pairs: List[Tuple[str, str]] = []

    if primary_char_id and primary_char_id in merged:
        ordered_pairs.append((primary_char_id, merged[primary_char_id]))

    for char_id in ordered_ids:
        if char_id == primary_char_id:
            continue
        ordered_pairs.append((char_id, merged[char_id]))

    return ordered_pairs


def build_session(user_agent: str, pool_size: int = 32) -> requests.Session:
    """
    Session HTTP con reuse de conexiones y cabeceras base.
    """
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": user_agent,
    })
    return session


def normalize_orders(payload: Sequence[dict]) -> List[tuple]:
    """
    Reduce cada order ESI al subconjunto operativo requerido por SQLite.
    """
    out: List[tuple] = []
    for row in payload:
        try:
            order_id = int(row["order_id"])
            issued = str(row["issued"])
            location_id = int(row["location_id"])
            type_id = int(row["type_id"])
            is_buy = 1 if bool(row["is_buy_order"]) else 0
            price = float(row["price"])
            volume_remain = int(row["volume_remain"])
        except Exception:
            continue

        out.append((
            order_id,
            issued,
            location_id,
            type_id,
            is_buy,
            price,
            volume_remain,
        ))
    return out


def _compute_backoff_seconds(headers: Dict[str, str]) -> float:
    """
    Backoff conservador para 420/429/5xx.

    Prioridad:
      - Retry-After si existe
      - si X-Esi-Error-Limit-Remain está bajo, usar Reset
      - si no, 30s
    """
    retry_after = _safe_int(headers.get("Retry-After"))
    remain = _safe_int(headers.get("X-Esi-Error-Limit-Remain"))
    reset = _safe_int(headers.get("X-Esi-Error-Limit-Reset"))

    wait = 30
    if retry_after is not None and retry_after > 0:
        wait = max(wait, retry_after)

    if remain is not None and remain <= 2 and reset is not None and reset > 0:
        wait = max(wait, reset)

    return float(wait)


def fetch_entity(
    *,
    entity,
    session: requests.Session,
    esi_base: str,
    datasource: str,
    polite_delay_s: float,
    stats,
    retry_budget,
    token_pool,
    batch_callback: Callable[[List[tuple]], None],
) -> dict:
    """
    Descarga todas las páginas de una entidad completa.

    Reglas:
      - regiones: endpoint público /markets/{region_id}/orders/?order_type=all
      - estructuras: endpoint autenticado /markets/structures/{structure_id}/
      - 404 región = fin
      - 404 estructura page1 = retry único 5s, luego ignorar si persiste
      - 401 estructura = rotar token, retry misma page, consume budget
      - 420/429/5xx = backoff + retry misma page, consume budget
      - otros 4xx: región -> fail, estructura -> ignorar
    """
    page = 1
    last_success_page = 0
    ignored = False
    retried_structure_404_page1 = False

    while True:
        if entity.kind == "region":
            url = f"{esi_base}/markets/{entity.entity_id}/orders/"
            params = {
                "datasource": datasource,
                "order_type": "all",
                "page": page,
            }
            headers = None
        else:
            url = f"{esi_base}/markets/structures/{entity.entity_id}/"
            params = {
                "datasource": datasource,
                "page": page,
            }
            token = token_pool.current_token() if token_pool is not None else None
            if not token:
                raise RuntimeError("No structure market access token available for authenticated ESI request")
            headers = {"Authorization": f"Bearer {token}"}

        stats.add_request()
        try:
            resp = session.get(url, params=params, headers=headers, timeout=(10, 30))
        except requests.RequestException:
            retry_budget.consume("network error")
            wait = 30.0
            stats.add_backoff(wait)
            time.sleep(wait)
            continue

        stats.observe_status(resp.status_code)
        stats.observe_last_modified(resp.headers.get("Last-Modified"))

        if resp.status_code == 200:
            payload = resp.json()
            batch = normalize_orders(payload)
            if batch:
                batch_callback(batch)
            last_success_page = page

            x_pages = _safe_int(resp.headers.get("X-Pages"))
            if x_pages is not None:
                if page >= x_pages:
                    break
            else:
                # Sin X-Pages, seguimos hasta encontrar 404.
                pass

            page += 1
            time.sleep(polite_delay_s)
            continue

        if resp.status_code == 404:
            if entity.kind == "region":
                break

            # Estructuras:
            if page == 1 and not retried_structure_404_page1:
                stats.inc("structure404_page1_retry", 1)
                retry_budget.consume("structure page1 404 retry")
                retried_structure_404_page1 = True
                time.sleep(5)
                continue

            if page == 1:
                stats.inc("ignored_structures", 1)
                ignored = True
            break

        if resp.status_code == 401 and entity.kind == "structure":
            retry_budget.consume("structure 401 rotate token")
            rotated = token_pool.rotate() if token_pool is not None else False
            if not rotated:
                raise RuntimeError("All structure market tokens exhausted after 401 rotation")
            time.sleep(5)
            continue

        if resp.status_code in (420, 429) or (500 <= resp.status_code <= 599):
            retry_budget.consume(f"HTTP {resp.status_code}")
            wait = _compute_backoff_seconds(resp.headers)
            stats.add_backoff(wait)
            time.sleep(wait)
            continue

        if 400 <= resp.status_code <= 499:
            if entity.kind == "structure":
                stats.inc("ignored_structures", 1)
                ignored = True
                break
            raise RuntimeError(
                f"Fatal 4xx while ingesting region {entity.entity_id} page {page}: HTTP {resp.status_code}"
            )

        raise RuntimeError(
            f"Unexpected HTTP status while ingesting {entity.kind} {entity.entity_id} page {page}: {resp.status_code}"
        )

    return {
        "kind": entity.kind,
        "entity_id": entity.entity_id,
        "name": entity.name,
        "pages": last_success_page,
        "ignored": ignored,
    }
