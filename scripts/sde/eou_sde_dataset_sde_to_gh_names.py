"""EOU · SDE Dataset (SDE → GH) — naming helpers.

Implements the naming rules described in CCP's Static Data documentation for
deriving planet/moon orbit names from the JSONL SDE.

No ESI calls; SDE-only.
"""

from __future__ import annotations

from typing import Dict

_ROMAN = [
    (50, "L"),
    (40, "XL"),
    (10, "X"),
    (9, "IX"),
    (5, "V"),
    (4, "IV"),
    (1, "I"),
]


def to_roman(n: int) -> str:
    """Convert a positive integer to Roman numerals (supports typical planet indices)."""
    if n <= 0:
        return str(n)
    out = []
    x = n
    for value, sym in _ROMAN:
        while x >= value:
            out.append(sym)
            x -= value
    return "".join(out)


def safe_en_name(obj: Dict, fallback: str = "") -> str:
    """Extract name.en with a few fallbacks used across SDE files."""
    name = obj.get("name")
    if isinstance(name, dict):
        en = name.get("en")
        if isinstance(en, str) and en:
            return en

    # Some SDE files historically used other keys for names.
    for k in ("corporationName", "operationName", "stationName", "typeName", "groupName"):
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v

    return fallback


def planet_name(solar_system_name: str, celestial_index: int) -> str:
    # CCP rule: <solarSystemName> <Roman(celestialIndex)>
    return f"{solar_system_name} {to_roman(int(celestial_index))}".strip()


def moon_name(planet_orbit_name: str, orbit_index: int) -> str:
    # CCP rule: <planetOrbitName> - Moon <orbitIndex>
    return f"{planet_orbit_name} - Moon {int(orbit_index)}".strip()
