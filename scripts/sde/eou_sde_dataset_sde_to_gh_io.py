"""
EOU · SDE Dataset (SDE → GH) — I/O helpers

Only uses the official CCP SDE JSONL ZIP (plus small local caches like packaged.jsonl.gz).

This module is intentionally dependency-free (stdlib only).
"""

from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import zipfile


def find_zip_member(zf: zipfile.ZipFile, basename: str) -> str:
    """Find a member in the ZIP by basename.

    The official CCP ZIP sometimes includes files at the root or under a folder.
    We match on suffix to be robust.
    """
    candidates = [n for n in zf.namelist() if n.endswith("/" + basename) or n == basename]
    if not candidates:
        raise KeyError(f"{basename} not found in ZIP")
    return sorted(candidates, key=len)[0]


def iter_jsonl_from_zip(zf: zipfile.ZipFile, basename: str) -> Iterator[Dict]:
    """Yield JSON objects from a JSONL file inside a ZIP."""
    member = find_zip_member(zf, basename)
    with zf.open(member, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8")
        for line in text:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_jsonl_gz(path: str | Path, *, max_bad_lines: int = 20) -> List[Dict]:
    """Read newline-delimited JSON from a .jsonl.gz file.

    Robust mode: if a line is invalid JSON, we skip it and keep going.
    This prevents a single corrupted line from killing the whole workflow.
    """
    path = Path(path)
    out: List[Dict] = []
    if not path.exists():
        return out

    bad = 0
    with gzip.open(path, mode="rt", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except json.JSONDecodeError:
                bad += 1
                if bad <= max_bad_lines:
                    # Keep it short; runner logs are precious.
                    print(f"[WARN] Invalid JSON in {path} at line {i} (skipping).")
                continue

    if bad > max_bad_lines:
        print(f"[WARN] Skipped {bad} invalid JSON lines in {path} (showed first {max_bad_lines}).")

    return out


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
