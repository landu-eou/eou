from __future__ import annotations

import hashlib
import json
from typing import Iterable, Mapping


def canonical_rows(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    normalized = [
        {
            "corporation_id": int(row["corporation_id"]),
            "corporation": str(row["corporation"]),
        }
        for row in rows
    ]
    normalized.sort(key=lambda item: item["corporation_id"])
    return normalized


def compute_hash(rows: Iterable[Mapping[str, object]]) -> tuple[str, list[dict[str, object]]]:
    normalized = canonical_rows(rows)
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest, normalized
