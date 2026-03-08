#!/usr/bin/env python3
"""
Tipos y utilidades compartidas del workflow de ingesta de market orders.

Este archivo centraliza estructuras temporales de runtime:
- Entity: una región o estructura a ingerir.
- RetryBudget: presupuesto global compartido de reintentos.
- StatsCollector: contadores internos del run y seguimiento de Last-Modified.
- TokenPool: pool ordenado y rotatorio de access tokens en memoria.

El archivo no toca red, disco ni Google Sheets. Solo representa estado efímero
compartido por el pipeline durante el run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import threading
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class Entity:
    """
    Representa una entidad de ingesta.

    kind:
      - "region"
      - "structure"

    entity_id:
      - region_id o structure_id

    name:
      - nombre legible para planner / pages cache
    """
    kind: str
    entity_id: int
    name: str


class RetryBudget:
    """
    Presupuesto global de reintentos compartido entre workers.

    Cada retry consume 1 unidad. Cuando llega a 0, el run debe fallar.
    """

    def __init__(self, initial: int) -> None:
        self._initial = max(0, int(initial))
        self._remaining = max(0, int(initial))
        self._lock = threading.Lock()

    @property
    def initial(self) -> int:
        return self._initial

    @property
    def remaining(self) -> int:
        with self._lock:
            return self._remaining

    def used(self) -> int:
        with self._lock:
            return self._initial - self._remaining

    def consume(self, reason: str = "") -> int:
        """
        Consume una unidad del presupuesto.

        Devuelve el restante tras consumir.
        Lanza RuntimeError si ya no queda presupuesto.
        """
        with self._lock:
            if self._remaining <= 0:
                raise RuntimeError(
                    f"Retry budget exhausted before retry{': ' + reason if reason else ''}"
                )
            self._remaining -= 1
            return self._remaining


class StatsCollector:
    """
    Contadores internos del run.

    Conserva además el mayor Last-Modified observado, en UTC ISO 8601 con sufijo Z.
    """

    _COUNTER_KEYS = (
        "requests",
        "http200",
        "http401",
        "http404",
        "http420",
        "http429",
        "http5xx",
        "backoff_seconds",
        "ignored_structures",
        "structure404_page1_retry",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, float] = {k: 0 for k in self._COUNTER_KEYS}
        self._max_last_modified_dt: Optional[datetime] = None

    def inc(self, key: str, amount: float = 1) -> None:
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def observe_status(self, status_code: int) -> None:
        """
        Actualiza contadores HTTP por clase o código relevante.
        """
        with self._lock:
            if status_code == 200:
                self._counters["http200"] += 1
            elif status_code == 401:
                self._counters["http401"] += 1
            elif status_code == 404:
                self._counters["http404"] += 1
            elif status_code == 420:
                self._counters["http420"] += 1
            elif status_code == 429:
                self._counters["http429"] += 1
            elif 500 <= status_code <= 599:
                self._counters["http5xx"] += 1

    def add_request(self) -> None:
        self.inc("requests", 1)

    def add_backoff(self, seconds: float) -> None:
        self.inc("backoff_seconds", float(seconds))

    def observe_last_modified(self, value: Optional[str]) -> None:
        """
        Guarda el mayor Last-Modified observado.
        """
        if not value:
            return
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        except Exception:
            return

        with self._lock:
            if self._max_last_modified_dt is None or dt > self._max_last_modified_dt:
                self._max_last_modified_dt = dt

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            max_last_modified = None
            if self._max_last_modified_dt is not None:
                max_last_modified = (
                    self._max_last_modified_dt
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            return {
                **self._counters,
                "max_last_modified": max_last_modified,
            }


class TokenPool:
    """
    Pool ordenado y rotatorio de tokens en memoria.

    El orden final debe ser:
      1) PRIMARY_CHAR_ID si existe
      2) resto de character_id de mayor a menor

    Los tokens nunca se imprimen ni se persisten.
    """

    def __init__(self, ordered_pairs: Sequence[Tuple[str, str]]) -> None:
        self._pairs: List[Tuple[str, str]] = list(ordered_pairs)
        self._idx = 0
        self._lock = threading.Lock()

    def size(self) -> int:
        with self._lock:
            return len(self._pairs)

    def current(self) -> Optional[Tuple[str, str]]:
        with self._lock:
            if not self._pairs:
                return None
            return self._pairs[self._idx]

    def rotate(self) -> bool:
        """
        Rota al siguiente token disponible.
        Devuelve False si no hay un siguiente token distinto.
        """
        with self._lock:
            if len(self._pairs) <= 1:
                return False
            self._idx = (self._idx + 1) % len(self._pairs)
            return True

    def current_token(self) -> Optional[str]:
        pair = self.current()
        return pair[1] if pair else None

    def current_character_id(self) -> Optional[str]:
        pair = self.current()
        return pair[0] if pair else None
