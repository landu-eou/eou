# --- scripts/sde/eou_sde_dataset_sde_to_gh_cynodock.py ---

"""
EOU · SDE Dataset (SDE → GH) — Cyno dock/jump security rules (HARDCODED, user-editable)

This module is intentionally simple and editable by the user:
- A mapping from stationType (English) -> cynoDockSecurityLevel + cynoDockSecurity label.
- A minimal API used by the builder.

Contract (API mínima):
  - station_cyno(station_type: str | None, docking: bool) -> tuple[int | None, str | None]
      Returns (level, label) for the station.
      If docking is False -> (None, None)
      If station_type is unknown -> (DEFAULT_LEVEL, DEFAULT_LABEL)

  - system_cyno(station_cyno_labels: Iterable[str | None], stations_count: int) -> str
      Returns cynoJumpSecurity for a solar system based on station labels, following the priority:
        1) any "safe"  -> "safe"
        2) any "risky" -> "risky"
        3) any "unsafe" OR "unknown"/"unk" -> "unsafe"
        4) if no stations (stations_count == 0) -> "no jump"
      If stations exist but all labels are None (e.g. docking=False everywhere), returns "no jump".

Notes:
- This file is meant to be edited by users to tweak stationType mappings and/or the aggregation logic.
"""

from __future__ import annotations

from typing import Iterable, Optional, Tuple

# --------------------------------------------------------------------
# Editable mapping: stationType (English) -> (cynoDockSecurityLevel, cynoDockSecurity)
# --------------------------------------------------------------------

DEFAULT_LEVEL: int = 15
DEFAULT_LABEL: str = "unknown"

STATIONTYPE_TO_CYNO: dict[str, tuple[int, str]] = {
    # unsafe
    "Amarr Industrial Station": (4, "unsafe"),
    "Minmatar Research Station": (5, "unsafe"),
    "Minmatar Station": (5, "unsafe"),
    "Gallente Industrial Station": (7, "unsafe"),
    "Sisters of EVE Industrial Station": (7, "unsafe"),
    "Minmatar Hub": (7, "unsafe"),
    "Minmatar Trade Post": (7, "unsafe"),
    "Caldari Logistics Station": (15, "unsafe"),
    # risky
    "Caldari Station Hub": (15, "risky"),
    "Amarr Standard Station": (17, "risky"),
    "Gallente Administrative Station": (17, "risky"),
    "Amarr Trade Post": (20, "risky"),
    "Gallente Trading Hub": (20, "risky"),
    "Minmatar Military Station": (20, "risky"),
    # safe
    "Paragon Fulfillment Center": (25, "safe"),
    "Amarr Mining Station": (27, "safe"),
    "Caldari Military Station": (27, "safe"),
    "Caldari Mining Station": (27, "safe"),
    "Caldari Research Station": (27, "safe"),
    "Minmatar Industrial Station": (27, "safe"),
    "Caldari Food Processing Plant Station": (34, "safe"),
    "Gallente Logistics Station": (34, "safe"),
    "Sisters of EVE Logistics Station": (34, "safe"),
    "Gallente Military Station": (34, "safe"),
    "Gallente Mining Station": (34, "safe"),
    "Amarr Research Station": (39, "safe"),
    "Gallente Station Hub": (39, "safe"),
    "Jita Trade Hub": (43, "safe"),
    "Amarr Station Hub": (50, "safe"),
    "Amarr Station Military": (50, "safe"),
    "Caldari Administrative Station": (50, "safe"),
    "Caldari Trading Station": (50, "safe"),
    "Gallente Research Station": (50, "safe"),
    "Minmatar Mining Station": (60, "safe"),
}


def station_cyno(station_type: Optional[str], docking: bool) -> Tuple[Optional[int], Optional[str]]:
    """
    Determine cyno dock security for a station.

    Args:
      station_type: English station type name (types.jsonl.name.en for station typeID).
      docking: Whether the station supports docking service.

    Returns:
      (cynoDockSecurityLevel, cynoDockSecurityLabel) or (None, None) if docking is False.
    """
    if not docking:
        return (None, None)

    if station_type and station_type in STATIONTYPE_TO_CYNO:
        return STATIONTYPE_TO_CYNO[station_type]

    return (DEFAULT_LEVEL, DEFAULT_LABEL)


def system_cyno(station_cyno_labels: Iterable[Optional[str]], stations_count: int) -> str:
    """
    Determine cyno jump security for a solar system from station labels.

    Priority (first match wins):
      1) safe
      2) risky
      3) unsafe OR unknown/unk
      4) no stations -> "no jump"

    If stations exist but all labels are None (e.g. docking=False everywhere), returns "no jump".

    Args:
      station_cyno_labels: labels from station_cyno(...)[1] for each station (may include None)
      stations_count: number of NPC stations in system

    Returns:
      "safe" | "risky" | "unsafe" | "no jump"
    """
    if stations_count <= 0:
        return "no jump"

    labels = {((x or "").strip().lower()) for x in station_cyno_labels if x is not None}
    if not labels:
        return "no jump"

    if "safe" in labels:
        return "safe"
    if "risky" in labels:
        return "risky"
    if "unsafe" in labels or "unknown" in labels or "unk" in labels:
        return "unsafe"

    # Fallback: stations exist, but no recognized label -> treat as unsafe to be conservative.
    return "unsafe"
