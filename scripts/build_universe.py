"""Build compact static map data from CCP's JSONL SDE archive."""
import json
import zipfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
with zipfile.ZipFile(root / "sde.zip") as archive:
    def records(name):
        return (json.loads(line) for line in archive.open(name))

    systems = {
        str(x["_key"]): [x["name"].get("en", ""), round(x.get("securityStatus", 0), 3)]
        for x in records("mapSolarSystems.jsonl")
    }
    gates = {
        str(x["_key"]): [
            x["solarSystemID"], x["destination"]["solarSystemID"],
            x["position"]["x"], x["position"]["y"], x["position"]["z"],
        ]
        for x in records("mapStargates.jsonl")
    }
    transport_groups = {28, 380, 513, 902, 1202, 883, 941}
    group_by_type = {
        x["_key"]: x["groupID"] for x in records("types.jsonl")
        if x.get("groupID") in transport_groups
    }

output = root / "src" / "universe.js"
output.write_text(
    "// Generated from CCP EVE Online Static Data Export.\n"
    + "export const systems=" + json.dumps(systems, separators=(",", ":"), ensure_ascii=False) + ";\n"
    + "export const gates=" + json.dumps(gates, separators=(",", ":")) + ";\n"
    + "export const transportTypes=" + json.dumps(group_by_type, separators=(",", ":")) + ";\n",
    encoding="utf-8",
)
print(f"{len(systems)} systems, {len(gates)} gates, {len(group_by_type)} transport types; {output.stat().st_size} bytes")
