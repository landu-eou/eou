"""
EOU · SDE Dataset (SDE → GH) — build outputs

Key change (2026-02):
- packaged ESI calls are done ONLY for types3 = (types2 - types1),
  where:
    types1 = baseline types.jsonl.gz already committed in repo
    types2 = freshly built published types from SDE
- data/esi/packaged.jsonl.gz remains "exceptions only" (packaged != volume)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import zipfile

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from eou_sde_dataset_sde_to_gh_io import iter_jsonl_from_zip, read_jsonl_gz, write_jsonl_gz  # noqa: E402
from eou_sde_dataset_sde_to_gh_names import safe_en_name  # noqa: E402


# -----------------------------
# Small helpers
# -----------------------------

def _get_int(obj: Dict, *keys: str) -> Optional[int]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return int(obj[k])
            except Exception:
                return None
    return None


def _get_float(obj: Dict, *keys: str) -> Optional[float]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return float(obj[k])
            except Exception:
                return None
    return None


def _get_bool(obj: Dict, *keys: str, default: bool = False) -> bool:
    for k in keys:
        if k in obj:
            return bool(obj[k])
    return default


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


# -----------------------------
# Baseline (types1) loader
# -----------------------------

def load_baseline_typeids(baseline_types_path: Optional[str]) -> Set[int]:
    """Load typeIDs from the committed baseline types file (types1)."""
    if not baseline_types_path:
        return set()
    p = Path(baseline_types_path)
    if not p.exists():
        return set()

    rows = read_jsonl_gz(p)
    out: Set[int] = set()
    for r in rows:
        tid = _get_int(r, "typeID")
        if tid is not None:
            out.add(tid)
    return out


# -----------------------------
# Existing packaged exceptions loader
# -----------------------------

def load_existing_packaged(existing_path: Optional[str]) -> Dict[int, Dict]:
    """Load packaged exceptions: typeID -> row.

    Row format:
      {"typeID":..., "type":..., "packaged":...}

    This file intentionally stores ONLY exceptions (packaged != volume).
    """
    if not existing_path:
        return {}
    p = Path(existing_path)
    if not p.exists():
        return {}

    rows = read_jsonl_gz(p)
    out: Dict[int, Dict] = {}
    for r in rows:
        tid = _get_int(r, "typeID")
        packaged = _get_float(r, "packaged")
        if tid is None or packaged is None:
            continue
        out[tid] = {"typeID": tid, "type": str(r.get("type") or ""), "packaged": float(packaged)}
    return out


def write_packaged_exceptions(path: str | Path, packaged_map: Dict[int, Dict]) -> None:
    """Write packaged exceptions ordered by typeID desc."""
    rows = list(packaged_map.values())
    rows.sort(key=lambda r: int(r["typeID"]), reverse=True)
    write_jsonl_gz(path, rows)


# -----------------------------
# ESI fetch (only for types3)
# -----------------------------

def esi_get_type(type_id: int, *, user_agent: str = "EOU-SDE-Packaged/1.0") -> Tuple[int, Optional[Dict]]:
    """Fetch /universe/types/{type_id}.

    Returns: (http_status, json_obj_or_none)
    """
    url = f"https://esi.evetech.net/universe/types/{type_id}"
    req = Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read().decode("utf-8")
            return status, json.loads(data)
    except HTTPError as e:
        try:
            body = e.read()
            _ = body  # ignore
        except Exception:
            pass
        return int(e.code), None
    except URLError:
        return 0, None


def refresh_packaged_for_new_types(
    *,
    new_type_ids: List[int],
    type_name_by_id: Dict[int, str],
    volume_by_id: Dict[int, float],
    packaged_map: Dict[int, Dict],
) -> None:
    """For each new typeID (types3), call ESI and update packaged exceptions map (in-place)."""

    min_delay_ms = env_int("ESI_MIN_DELAY_MS", 300)
    max_retries = env_int("ESI_MAX_RETRIES", 8)

    def sleep_ms(ms: int) -> None:
        time.sleep(ms / 1000.0)

    total = len(new_type_ids)
    if total == 0:
        print("[PACKAGED] No new types to evaluate (types3 empty).")
        return

    print(f"[PACKAGED] Evaluating {total} new typeIDs via ESI (types3).")
    # Log every N to show progress in long runs
    log_every = 50 if total >= 200 else 10

    for idx, tid in enumerate(new_type_ids, start=1):
        vol = volume_by_id.get(tid)
        if vol is None:
            # Shouldn't happen; skip safely.
            continue

        # simple retry loop
        attempt = 0
        while True:
            attempt += 1
            status, payload = esi_get_type(tid)

            # Good response
            if status == 200 and isinstance(payload, dict):
                esi_vol = payload.get("volume")
                pkg_vol = payload.get("packaged_volume")
                # If packaged_volume is absent, treat as "no exception"
                if pkg_vol is not None and esi_vol is not None:
                    try:
                        pkg_vol_f = float(pkg_vol)
                        vol_f = float(vol)
                        if pkg_vol_f != vol_f:
                            packaged_map[tid] = {
                                "typeID": tid,
                                "type": type_name_by_id.get(tid, str(tid)),
                                "packaged": pkg_vol_f,
                            }
                        else:
                            # ensure not present (exceptions-only)
                            packaged_map.pop(tid, None)
                    except Exception:
                        pass
                else:
                    packaged_map.pop(tid, None)

                break

            # Not found: remove if exists (shouldn't for new, but safe)
            if status == 404:
                packaged_map.pop(tid, None)
                break

            # Throttle/retry statuses:
            # - 420/429 (rate limits)
            # - 5xx transient
            retryable = status in {0, 420, 429, 500, 502, 503, 504}
            if retryable and attempt < max_retries:
                # Backoff: linear + base delay
                sleep_ms(min_delay_ms * attempt)
                continue

            # Non-retryable or max retries reached
            print(f"[WARN] ESI type {tid} failed (status={status}) after {attempt} attempts; skipping.")
            break

        # Base politeness delay (even after success)
        sleep_ms(min_delay_ms)

        if idx % log_every == 0 or idx == total:
            print(f"[PACKAGED] Progress {idx}/{total} (last typeID={tid}).")


# -----------------------------
# SDE reads (subset needed for types delta + output)
# -----------------------------

def read_published_types_from_sde(zf: zipfile.ZipFile) -> List[Dict]:
    """Return the published types objects from SDE types.jsonl as raw dicts."""
    out: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not _get_bool(obj, "published", default=False):
            continue
        out.append(obj)
    return out


def build_types_out_with_packaged_and_market_tree(
    zf: zipfile.ZipFile,
    *,
    packaged_map: Dict[int, Dict],
) -> List[Dict]:
    """
    Minimal version: produces types.jsonl.gz with:
      - typeID, type
      - volume (SDE)
      - packaged (from packaged_map exceptions else volume)
      - group, category, marketGroup
      - marketTree (placeholder unless you already have it implemented)
      - is_contraband, is_gategank

    NOTE: If you already have full marketTree logic in your current outputs.py,
    keep it and only replace the packaged delta parts; the delta strategy is orthogonal.
    """
    # ---- read groups/categories/marketGroups/contraband as in your existing code ----
    # Kept minimal here to focus on the delta strategy.
    groups_meta: Dict[int, Tuple[str, int]] = {}
    for obj in iter_jsonl_from_zip(zf, "groups.jsonl"):
        gid = int(obj.get("_key"))
        gname = safe_en_name(obj, fallback=str(gid))
        cat_id = int(obj.get("categoryID"))
        groups_meta[gid] = (gname, cat_id)

    categories: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "categories.jsonl"):
        cid = int(obj.get("_key"))
        categories[cid] = safe_en_name(obj, fallback=str(cid))

    marketgroup_names: Dict[int, str] = {}
    for obj in iter_jsonl_from_zip(zf, "marketGroups.jsonl"):
        mgid = int(obj.get("_key"))
        marketgroup_names[mgid] = safe_en_name(obj, fallback=str(mgid))

    contraband: Set[int] = set()
    for obj in iter_jsonl_from_zip(zf, "contrabandTypes.jsonl"):
        contraband.add(int(obj.get("_key")))

    rows: List[Dict] = []
    for obj in iter_jsonl_from_zip(zf, "types.jsonl"):
        if not _get_bool(obj, "published", default=False):
            continue

        tid = int(obj.get("_key"))
        tname = safe_en_name(obj, fallback=str(tid))
        volume = _get_float(obj, "volume") or 0.0

        # group/category
        gid = _get_int(obj, "group_id", "groupID")
        gname = ""
        cname = ""
        if gid is not None and gid in groups_meta:
            gname, cat_id = groups_meta[gid]
            cname = categories.get(cat_id, str(cat_id))

        # market group name
        mgid = _get_int(obj, "marketGroupID", "market_group_id", "marketGroupId")
        mgname = marketgroup_names.get(mgid, str(mgid)) if mgid is not None else ""

        # packaged: from exceptions if present, else volume
        pkg = packaged_map.get(tid, {}).get("packaged")
        packaged = float(pkg) if pkg is not None else float(volume)

        # marketTree: (keep your full logic; here placeholder)
        market_tree = None if mgid is None else f"Market → … → {mgname or str(mgid)}"

        rows.append(
            {
                "typeID": tid,
                "type": tname,
                "volume": float(volume),
                "packaged": float(packaged),
                "group": gname,
                "category": cname,
                "marketGroup": mgname,
                "marketTree": market_tree,
                "is_contraband": tid in contraband,
                "is_gategank": gname == "Smart Bomb",
            }
        )

    rows.sort(key=lambda r: r["typeID"])
    return rows


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="Path to CCP SDE JSONL ZIP")

    ap.add_argument("--out-sde", required=True, help="Output dir for SDE datasets")
    ap.add_argument("--out-esi", required=True, help="Output dir for ESI-derived datasets")

    ap.add_argument("--baseline-types", required=False, default="", help="Committed baseline types file (types1) to compute types3")
    ap.add_argument("--existing-packaged", required=False, default="", help="Existing packaged exceptions file (data/esi/packaged.jsonl.gz)")

    # (you have more args in your full script: exclude-config/state, etc.)
    ap.add_argument("--exclude-config", required=False, default="")
    ap.add_argument("--exclude-state", required=False, default="")

    args = ap.parse_args()

    out_sde = Path(args.out_sde)
    out_esi = Path(args.out_esi)
    out_sde.mkdir(parents=True, exist_ok=True)
    out_esi.mkdir(parents=True, exist_ok=True)

    # types1 (baseline) -> used to compute types3
    baseline_ids = load_baseline_typeids(args.baseline_types)
    print(f"[DELTA] baseline types1 size: {len(baseline_ids)}")

    # packaged exceptions currently in repo
    packaged_map = load_existing_packaged(args.existing_packaged)
    print(f"[PACKAGED] existing exceptions in repo: {len(packaged_map)}")

    with zipfile.ZipFile(args.zip) as zf:
        # Build types2 set from SDE (published)
        published_objs = read_published_types_from_sde(zf)
        types2_ids: Set[int] = set()
        volume_by_id: Dict[int, float] = {}
        type_name_by_id: Dict[int, str] = {}

        for obj in published_objs:
            tid = int(obj.get("_key"))
            types2_ids.add(tid)
            type_name_by_id[tid] = safe_en_name(obj, fallback=str(tid))
            volume_by_id[tid] = float(_get_float(obj, "volume") or 0.0)

        print(f"[DELTA] new SDE types2 size: {len(types2_ids)}")

        # types3 = types2 - types1 (NEW TYPES ONLY)
        types3_ids = sorted([tid for tid in types2_ids if tid not in baseline_ids])
        print(f"[DELTA] types3 (types2 - types1) size: {len(types3_ids)}")

        # prune packaged exceptions for types that disappeared from SDE
        before = len(packaged_map)
        packaged_map = {tid: row for tid, row in packaged_map.items() if tid in types2_ids}
        pruned = before - len(packaged_map)
        if pruned:
            print(f"[PACKAGED] pruned {pruned} exceptions (types no longer in SDE).")

        # call ESI only for types3
        refresh_packaged_for_new_types(
            new_type_ids=types3_ids,
            type_name_by_id=type_name_by_id,
            volume_by_id=volume_by_id,
            packaged_map=packaged_map,
        )

        # write packaged exceptions output (ESI-derived)
        write_packaged_exceptions(out_esi / "packaged.jsonl.gz", packaged_map)
        print(f"[PACKAGED] wrote exceptions: {len(packaged_map)} -> {out_esi/'packaged.jsonl.gz'}")

        # build types output (SDE) using packaged_map for packaged field
        types_rows = build_types_out_with_packaged_and_market_tree(zf, packaged_map=packaged_map)
        write_jsonl_gz(out_sde / "types.jsonl.gz", types_rows)
        print(f"[TYPES] wrote {len(types_rows)} published types -> {out_sde/'types.jsonl.gz'}")

        # TODO: (keep your existing code)
        # - regions/constellations/solarsystems/stations/stargates/corporations
        # - marketTree.jsonl.gz + marketTree.txt
        # - excludedMarketTypes.jsonl.gz + excludeMarketTypes.state.json

    # Sanity checks for the files we touched here
    for p in [out_esi / "packaged.jsonl.gz", out_sde / "types.jsonl.gz"]:
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Missing/empty output: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
