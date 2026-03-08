from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_state(path: Path) -> Optional[dict[str, object]]:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "hash": str(payload["hash"]),
        "last-modified": parse_utc(str(payload["last-modified"])),
    }


def write_state(path: Path, hash_value: str, last_modified: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "hash": hash_value,
        "last-modified": format_utc(last_modified),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
