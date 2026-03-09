from __future__ import annotations

import math
import random
import time
from typing import Dict, Iterable, List, Tuple

import requests

ESI_NAMES_URL = "https://esi.evetech.net/latest/universe/names/"
TRANSIENT_STATUS_CODES = {408, 409, 420, 429, 500, 502, 503, 504}


class EsiResolver:
    def __init__(
        self,
        logger,
        user_agent: str,
        batch_size: int = 1000,
        timeout_seconds: int = 30,
        max_transient_retries: int = 2,
    ) -> None:
        self.logger = logger
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.max_transient_retries = max_transient_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
        )
        self.stats = {
            "batches_attempted": 0,
            "transient_retries": 0,
            "split_retries": 0,
            "min_error_limit_remain": None,
        }

    def resolve(self, corporation_ids: Iterable[int]) -> Tuple[Dict[int, str], List[int]]:
        unique_ids = sorted({int(value) for value in corporation_ids})
        resolved: Dict[int, str] = {}
        unresolved: List[int] = []

        for start in range(0, len(unique_ids), self.batch_size):
            batch = unique_ids[start : start + self.batch_size]
            batch_resolved, batch_unresolved = self._resolve_batch(batch)
            resolved.update(batch_resolved)
            unresolved.extend(batch_unresolved)

        unresolved = sorted(set(unresolved) - set(resolved.keys()))
        return resolved, unresolved

    def _resolve_batch(self, batch: List[int]) -> Tuple[Dict[int, str], List[int]]:
        if not batch:
            return {}, []

        self.stats["batches_attempted"] += 1
        response = self._post_with_retries(batch)
        if response is None:
            self.logger.warning("esi batch failed after retries; batch_size=%s", len(batch))
            return {}, batch

        if response.status_code == 200:
            return self._parse_success(batch, response)

        if response.status_code == 404:
            if len(batch) == 1:
                self.logger.warning("esi 404 unresolved corporation_id=%s", batch[0])
                return {}, batch

            midpoint = math.ceil(len(batch) / 2)
            self.stats["split_retries"] += 1

            left_resolved, left_unresolved = self._resolve_batch(batch[:midpoint])
            right_resolved, right_unresolved = self._resolve_batch(batch[midpoint:])
            merged = {**left_resolved, **right_resolved}
            return merged, left_unresolved + right_unresolved

        self.logger.warning(
            "esi non-fatal error status=%s batch_size=%s",
            response.status_code,
            len(batch),
        )
        return {}, batch

    def _post_with_retries(self, batch: List[int]) -> requests.Response | None:
        response: requests.Response | None = None

        for attempt in range(self.max_transient_retries + 1):
            try:
                response = self.session.post(
                    ESI_NAMES_URL,
                    params={"datasource": "tranquility"},
                    json=batch,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_transient_retries:
                    self.logger.warning(
                        "esi request exception after retries; batch_size=%s error=%s",
                        len(batch),
                        exc,
                    )
                    return None

                self.stats["transient_retries"] += 1
                self._sleep_backoff(attempt, None)
                continue

            self._inspect_error_limit(response)

            if response.status_code in {200, 404}:
                return response

            if response.status_code in TRANSIENT_STATUS_CODES:
                if attempt >= self.max_transient_retries:
                    return response

                self.stats["transient_retries"] += 1
                self._sleep_backoff(attempt, response)
                continue

            return response

        return response

    def _parse_success(
        self,
        batch: List[int],
        response: requests.Response,
    ) -> Tuple[Dict[int, str], List[int]]:
        resolved: Dict[int, str] = {}
        unresolved = set(batch)

        try:
            payload = response.json()
        except ValueError:
            self.logger.warning("esi returned invalid json; batch_size=%s", len(batch))
            return {}, batch

        if not isinstance(payload, list):
            self.logger.warning(
                "esi returned unexpected payload type=%s batch_size=%s",
                type(payload).__name__,
                len(batch),
            )
            return {}, batch

        for item in payload:
            if not isinstance(item, dict):
                continue

            entity_id = item.get("id")
            name = item.get("name")
            category = item.get("category")

            if entity_id is None or not name:
                continue
            if category and category != "corporation":
                self.logger.warning(
                    "esi returned unexpected category; id=%s category=%s",
                    entity_id,
                    category,
                )
                continue

            entity_id = int(entity_id)
            resolved[entity_id] = str(name)
            unresolved.discard(entity_id)

        if unresolved:
            self.logger.warning(
                "esi batch resolved partially; requested=%s resolved=%s unresolved=%s",
                len(batch),
                len(resolved),
                len(unresolved),
            )

        return resolved, sorted(unresolved)

    def _inspect_error_limit(self, response: requests.Response) -> None:
        remain = response.headers.get("X-ESI-Error-Limit-Remain")
        reset = response.headers.get("X-ESI-Error-Limit-Reset")

        try:
            remain_int = int(remain) if remain is not None else None
        except ValueError:
            remain_int = None

        if remain_int is None:
            return

        current = self.stats["min_error_limit_remain"]
        if current is None or remain_int < current:
            self.stats["min_error_limit_remain"] = remain_int

        if remain_int <= 5:
            self.logger.warning(
                "esi error budget low; remain=%s reset=%s status=%s",
                remain_int,
                reset,
                response.status_code,
            )
            try:
                sleep_seconds = max(int(reset or 1), 1)
            except ValueError:
                sleep_seconds = 1
            time.sleep(min(sleep_seconds, 30))

    def _sleep_backoff(self, attempt: int, response: requests.Response | None) -> None:
        reset_seconds = None
        if response is not None:
            reset_header = response.headers.get("X-ESI-Error-Limit-Reset")
            try:
                reset_seconds = int(reset_header) if reset_header is not None else None
            except ValueError:
                reset_seconds = None

        base = 2 ** attempt
        delay = max(base, reset_seconds or 0) + random.uniform(0.0, 0.5)
        time.sleep(min(delay, 30.0))
