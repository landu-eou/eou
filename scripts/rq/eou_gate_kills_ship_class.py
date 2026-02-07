#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ship class mapping (editable).
Regla:
- Si category != "Ship" => "unknown"
- Si category == "Ship" y group está en el mapa => valor mapeado
- Si category == "Ship" y group no está => "unknown"
"""

from __future__ import annotations

GROUP_TO_CLASS = {
    "Capsule": "pod",

    # subcapital (según tu lista)
    "Assault Frigate": "subcapital",
    "Attack Battlecruiser": "subcapital",
    "Battleship": "subcapital",
    "Black Ops": "subcapital",
    "Combat Battlecruiser": "subcapital",
    "Combat Recon Ship": "subcapital",
    "Command Destroyer": "subcapital",
    "Command Ship": "subcapital",
    "Destroyer": "subcapital",
    "Cruiser": "subcapital",
    "Corvette": "subcapital",
    "Covert Ops": "subcapital",
    "Electronic Attack Ship": "subcapital",
    "Exhumer": "subcapital",
    "Expedition Command Ship": "subcapital",
    "Expedition Frigate": "subcapital",
    "Flag Cruiser": "subcapital",
    "Force Recon Ship": "subcapital",
    "Frigate": "subcapital",
    "Heavy Assault Cruiser": "subcapital",
    "Heavy Interdiction Cruiser": "subcapital",
    "Industrial Command Ship": "subcapital",
    "Interceptor": "subcapital",
    "Interdictor": "subcapital",
    "Logistics": "subcapital",
    "Logistics Frigate": "subcapital",
    "Marauder": "subcapital",
    "Mining Barge": "subcapital",
    "Prototype Exploration Ship": "subcapital",
    "Shuttle": "subcapital",
    "Stealth Bomber": "subcapital",
    "Strategic Cruiser": "subcapital",
    "Tactical Destroyer": "subcapital",

    # hauler / freighter
    "Hauler": "hauler",
    "Blockade Runner": "hauler",
    "Deep Space Transport": "hauler",
    "Freighter": "freighter",
    "Jump Freighter": "freighter",

    # capital / supercapital
    "Carrier": "capital",
    "Dreadnought": "capital",
    "Lancer Dreadnought": "capital",
    "Force Auxiliary": "capital",
    "Capital Industrial Ship": "capital",
    "Supercarrier": "supercapital",
    "Titan": "supercapital",
}


def ship_class_from(category: str | None, group: str | None) -> str:
    if (category or "") != "Ship":
        return "unknown"
    g = group or ""
    return GROUP_TO_CLASS.get(g, "unknown")
