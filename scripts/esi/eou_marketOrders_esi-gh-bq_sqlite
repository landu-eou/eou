#!/usr/bin/env python3
"""
SQLite temporal del run.

Este archivo encapsula la base local usada durante la ingesta:
- varios workers descargan páginas
- un único hilo escritor recibe lotes por cola
- SQLite deduplica por order_id, quedándose con issued más reciente

La base vive en .tmp/ y es efímera. No se commitea.
"""

from __future__ import annotations

import queue
import sqlite3
import threading
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

OrderRow = Tuple[int, str, int, int, int, float, int]


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    issued TEXT NOT NULL,
    location_id INTEGER NOT NULL,
    type_id INTEGER NOT NULL,
    is_buy INTEGER NOT NULL,
    price REAL NOT NULL,
    volume_remain INTEGER NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO orders (
    order_id,
    issued,
    location_id,
    type_id,
    is_buy,
    price,
    volume_remain
)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(order_id) DO UPDATE SET
    issued = excluded.issued,
    location_id = excluded.location_id,
    type_id = excluded.type_id,
    is_buy = excluded.is_buy,
    price = excluded.price,
    volume_remain = excluded.volume_remain
WHERE excluded.issued > orders.issued;
"""


class SQLiteWriter:
    """
    Hilo escritor único para SQLite.

    Este patrón evita contención de escritura entre workers, simplifica la
    deduplicación y hace más predecible el rendimiento del run.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self._queue: "queue.Queue[Optional[List[OrderRow]]]" = queue.Queue(maxsize=512)
        self._thread = threading.Thread(target=self._run, name="sqlite-writer", daemon=True)
        self._error: Optional[BaseException] = None
        self._started = False

    def start(self) -> None:
        if not self._started:
            self._started = True
            self._thread.start()

    def enqueue(self, batch: Sequence[OrderRow]) -> None:
        if not batch:
            return
        self._queue.put(list(batch))

    def close(self) -> None:
        """
        Señala fin de escritura y espera al hilo escritor.
        """
        if self._started:
            self._queue.put(None)
            self._thread.join()

        if self._error is not None:
            raise RuntimeError(f"SQLite writer failed: {self._error}") from self._error

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute(CREATE_SQL)
        conn.commit()
        return conn

    def _run(self) -> None:
        conn = None
        try:
            conn = self._connect()
            while True:
                batch = self._queue.get()
                if batch is None:
                    break
                conn.executemany(UPSERT_SQL, batch)
                conn.commit()
        except BaseException as exc:
            self._error = exc
        finally:
            if conn is not None:
                conn.close()
