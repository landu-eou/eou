from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CorporationRow:
    corporation_id: int
    corporation: str


@dataclass(frozen=True)
class StateFile:
    hash: str
    last_modified: datetime


@dataclass(frozen=True)
class Decision:
    should_write: bool
    reason: str


@dataclass
class ResolutionStats:
    total_ids: int = 0
    resolved_ids: int = 0
    unresolved_ids: int = 0
    batches_attempted: int = 0
    transient_retries: int = 0
    split_retries: int = 0
    max_error_limit_seen: Optional[int] = None
