# scripts/sde/eou_sde_dataset_sde_to_gh_cynodock.py

"""
EOU · SDE Dataset (SDE → GH) — Cyno dock/jump policy

Este fichero está diseñado para ser EDITABLE por el usuario.
Aquí se hardcodea la política de "cynoDockSecurityLevel" + "cynoDockSecurity"
por stationType, y la agregación a nivel sistema ("cynoJumpSecurity").

API pública mínima (contrato):
  - station_cyno(docking: bool, station_type: str | None) -> (level:int|None, label:str|None)
  - system_cyno(station_labels: Iterable[str|None]) -> str
"""

from __future__ import annotations

from typing import Iterable, Optional, Tuple


# ------------------------------------------------------------
# 1) Política por stationType (editable)
# ------------------------------------------------------------
# Nota: si stationType no aparece aquí -> default (15, "unknown")
# Nota: si docking == False -> (None, None) SIEMPRE (según tu regla)
STATIONTYPE_TO_CYNODOCK: dict[str, tuple[int, str]] = {
    "Amarr Industrial Station": (4, "unsafe"),
    "Minmatar Research Station": (5, "unsafe"),
    "Minmatar Station": (5, "unsafe"),
    "Gallente Industrial Station": (7, "unsafe"),
    "Sisters of EVE Industrial Station": (7, "unsafe"),
    "Minmatar Hub": (7, "unsafe"),
    "Minmatar Trade Post": (7, "unsafe"),
    "Caldari Logistics Station": (15, "unsafe"),
    "Caldari Station Hub": (15, "risky"),
    "Amarr Standard Station": (17, "risky"),
    "Gallente Administrative Station": (17, "risky"),
    "Amarr Trade Post": (20, "risky"),
    "Gallente Trading Hub": (20, "risky"),
    "Minmatar Military Station": (20, "risky"),
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

DEFAULT_CYNODOCK: tuple[int, str] = (15, "unknown")


def station_cyno(*, docking: bool, station_type: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """
    Devuelve (cynoDockSecurityLevel, cynoDockSecurity).

    Reglas:
      - Si docking == False: (None, None)
      - Si station_type está en tabla: (level, label)
      - Si no: default -> (15, "unknown")
    """
    if not docking:
        return (None, None)

    st = (station_type or "").strip()
    if not st:
        return DEFAULT_CYNODOCK

    return STATIONTYPE_TO_CYNODOCK.get(st, DEFAULT_CYNODOCK)


_UNSAFE_LABELS = {"unsafe", "unknown", "unk"}


def system_cyno(station_labels: Iterable[Optional[str]]) -> str:
    """
    Agrega labels por estación a nivel sistema, en este orden:

      1) si hay al menos una "safe"  -> "safe"
      2) si hay al menos una "risky" -> "risky"
      3) si hay al menos una "unsafe" o "unknown/unk" -> "unsafe"
      4) si no hay labels (o todas None) -> "no jump"
    """
    seen = {str(x).strip().lower() for x in station_labels if x is not None and str(x).strip()}

    if "safe" in seen:
        return "safe"
    if "risky" in seen:
        return "risky"
    if seen.intersection(_UNSAFE_LABELS):
        return "unsafe"
    return "no jump"
