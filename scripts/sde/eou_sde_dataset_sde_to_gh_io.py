"""EOU · SDE Dataset (SDE → GH) — I/O helpers

Only uses the official CCP SDE JSONL ZIP.

This module is intentionally dependency-free (stdlib only).
"""

from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator

import zipfile


def find_zip_member(zf: zipfile.ZipFile, basename: str) -> str:
    """Find a member in the ZIP by basename.

    The official CCP ZIP sometimes includes files at the root or under a folder.
    We match on suffix to be robust.
    """
    candidates = [n for n in zf.namelist() if n.endswith("/" + basename) or n == basename]
    if not candidates:
        raise KeyError(f"{basename} not found in ZIP")
    # Prefer the shortest path (usually root), else first.
    return sorted(candidates, key=len)[0]


def iter_jsonl_from_zip(zf: zipfile.ZipFile, basename: str) -> Iterator[Dict]:
    """Yield JSON objects from a JSONL file inside a ZIP."""
    member = find_zip_member(zf, basename)
    with zf.open(member, "r") as raw:
        # CCP JSONL is UTF-8.
        text = io.TextIOWrapper(raw, encoding="utf-8")
        for line in text:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_jsonl_gz(path: str | Path, rows: Iterable[Dict]) -> None:
    """Write newline-delimited JSON to a .jsonl.gz file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8", compresslevel=9) as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}
