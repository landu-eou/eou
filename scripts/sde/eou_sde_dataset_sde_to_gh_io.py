"""
EOU · SDE Dataset (SDE → GH) — I/O helpers (stdlib only)

- Read JSONL files inside CCP SDE ZIP
- Read/write .jsonl.gz (NDJSON gzip)
- Small utilities (sha256, env parsing, text write)
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

import zipfile


def find_zip_member(zf: zipfile.ZipFile, basename: str) -> str:
    """Find a member in the ZIP by basename (robust to root/subfolder)."""
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
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def read_jsonl_gz(path: str | Path, *, max_bad_lines: int = 20) -> List[Dict]:
    """Read newline-delimited JSON from a .jsonl.gz file, tolerating bad lines."""
    path = Path(path)
    if not path.exists():
        return []

    out: List[Dict] = []
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


def write_text(path: str | Path, text: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def sha256_file(path: str | Path) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default
