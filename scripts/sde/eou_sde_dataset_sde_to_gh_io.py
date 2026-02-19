from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import zipfile


def find_zip_member(zf: zipfile.ZipFile, basename: str) -> str:
    candidates = [n for n in zf.namelist() if n.endswith("/" + basename) or n == basename]
    if not candidates:
        raise KeyError(f"{basename} not found in ZIP")
    return sorted(candidates, key=len)[0]


def iter_jsonl_from_zip(zf: zipfile.ZipFile, basename: str) -> Iterator[Dict]:
    member = find_zip_member(zf, basename)
    with zf.open(member, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8")
        for line in text:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_jsonl_gz(path: str | Path) -> List[Dict]:
    path = Path(path)
    if not path.exists():
        return []
    out: List[Dict] = []
    with gzip.open(path, mode="rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def write_jsonl_gz(path: str | Path, rows: Iterable[Dict]) -> None:
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
    path = Path(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except Exception:
        return default
