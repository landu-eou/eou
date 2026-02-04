"""Ship-class mapping for EOU (editable).

This module is intentionally easy to edit:
- Classifications live in plain Python sets.
- The only logic is `classify_ship()`.

Requested mapping logic:
- If category != "Ship" => "unknown"
- If category == "Ship" and group == "Capsule" => "pod"
- If category == "Ship" and group in GROUPS_SUBCAPITAL => "subcapital"
- If category == "Ship" and group in GROUPS_HAULER => "hauler"
- If category == "Ship" and group in GROUPS_FREIGHTER => "freighter"
- If category == "Ship" and group in GROUPS_CAPITAL => "capital"
- If category == "Ship" and group in GROUPS_SUPERCAPITAL => "supercapital"
- Else => "unknown"

Edit tips:
- Keep group names exactly as they appear in data/sde/types.jsonl.gz (field: "group").
- Prefer adding/removing items in the GROUPS_* sets; no other code changes needed.
"""

from __future__ import annotations

from typing import Optional


# --- Groups per class (EDITABLE) ---

# Pods
GROUPS_POD = {
    "Capsule",
}

# Subcapitals
GROUPS_SUBCAPITAL = {
    "Assault Frigate",
    "Attack Battlecruiser",
    "Battleship",
    "Black Ops",
    "Combat Battlecruiser",
    "Combat Recon Ship",
    "Command Destroyer",
    "Command Ship",
    "Corvette",
    "Covert Ops",
    "Cruiser",
    "Destroyer",
    "Electronic Attack Ship",
    "Exhumer",
    "Expedition Command Ship",
    "Expedition Frigate",
    "Flag Cruiser",
    "Force Recon Ship",
    "Frigate",
    "Heavy Assault Cruiser",
    "Heavy Interdiction Cruiser",
    "Industrial Command Ship",
    "Interceptor",
    "Interdictor",
    "Logistics",
    "Logistics Frigate",
    "Marauder",
    "Mining Barge",
    "Prototype Exploration Ship",
    "Shuttle",
    "Stealth Bomber",
    "Strategic Cruiser",
    "Tactical Destroyer",
}

# Haulers
GROUPS_HAULER = {
    "Hauler",
    "Blockade Runner",
    "Deep Space Transport",
}

# Freighters
GROUPS_FREIGHTER = {
    "Freighter",
    "Jump Freighter",
}

# Capitals
GROUPS_CAPITAL = {
    "Carrier",
    "Dreadnought",
    "Lancer Dreadnought",
    "Force Auxiliary",
    "Capital Industrial Ship",
}

# Supercapitals
GROUPS_SUPERCAPITAL = {
    "Supercarrier",
    "Titan",
}


def classify_ship(category: Optional[str], group: Optional[str]) -> str:
    """Return ship_class according to the hardcoded mapping."""
    if category != "Ship" or not group:
        return "unknown"

    if group in GROUPS_POD:
        return "pod"
    if group in GROUPS_SUBCAPITAL:
        return "subcapital"
    if group in GROUPS_HAULER:
        return "hauler"
    if group in GROUPS_FREIGHTER:
        return "freighter"
    if group in GROUPS_CAPITAL:
        return "capital"
    if group in GROUPS_SUPERCAPITAL:
        return "supercapital"

    return "unknown"
