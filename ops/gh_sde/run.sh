#!/usr/bin/env bash
set -euo pipefail

: "${SDE_URL:=https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip}"
: "${OUT_DIR:=data/gh_sde}"
: "${META_PATH:=${OUT_DIR}/_meta.json}"

SYSTEMS_OUT="gh_sde_systems.jsonl.gz"
REGIONS_OUT="gh_sde_regions.jsonl.gz"
STATIONS_OUT="gh_sde_stations.jsonl.gz"
TYPES_OUT="gh_sde_types.jsonl.gz"
STARGATES_OUT="gh_sde_stargates.jsonl.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

stored_etag=""
stored_last_modified=""
if [[ -f "$META_PATH" ]]; then
  stored_etag="$(jq -r '.http.etag // empty' "$META_PATH" || true)"
  stored_last_modified="$(jq -r '.http.lastModified // empty' "$META_PATH" || true)"
fi

headers_file="$tmp/headers.txt"

curl_args=(curl -sS -L -I -D "$headers_file" -o /dev/null -w "%{http_code}\n%{url_effective}\n")
if [[ -n "$stored_etag" ]]; then
  curl_args+=(-H "If-None-Match: $stored_etag")
fi
curl_args+=("$SDE_URL")

info="$("${curl_args[@]}")"
http_code="$(printf "%s" "$info" | sed -n '1p')"
url_effective="$(printf "%s" "$info" | sed -n '2p')"

# extrae el último valor (por si hay redirects y headers múltiples)
hdr_get() {
  local key="$1"
  awk -v k="$key" 'BEGIN{IGNORECASE=1} $0 ~ "^"k":" {sub(/^[^:]+:[[:space:]]*/, "", $0); gsub(/\r/,""); print $0}' "$headers_file" | tail -n 1
}

etag="$(hdr_get "ETag")"
last_modified="$(hdr_get "Last-Modified")"
content_length="$(hdr_get "Content-Length")"
cache_control="$(hdr_get "Cache-Control")"

if [[ -z "$etag" || -z "$last_modified" ]]; then
  echo "ERROR: faltan ETag o Last-Modified en headers (no puedo aplicar fuente de verdad)."
  echo "ETag='$etag' Last-Modified='$last_modified'"
  exit 2
fi

should_update="false"
case "$http_code" in
  304)
    should_update="false"
    ;;
  200)
    if [[ -n "$stored_etag" && "$etag" == "$stored_etag" ]]; then
      should_update="false"
    else
      should_update="true"
    fi
    ;;
  *)
    echo "ERROR: HTTP inesperado en HEAD: $http_code"
    exit 2
    ;;
esac

# calcula next_run_ms según tus reglas (UTC)
calc_out="$tmp/sched.json"
python3 - "$should_update" "$last_modified" > "$calc_out" <<'PY'
import sys, json, datetime, email.utils

should_update = (sys.argv[1].lower() == "true")
lm_str = sys.argv[2]

lm_dt = email.utils.parsedate_to_datetime(lm_str)
if lm_dt.tzinfo is None:
  lm_dt = lm_dt.replace(tzinfo=datetime.timezone.utc)
lm_dt = lm_dt.astimezone(datetime.timezone.utc)

now_dt = datetime.datetime.now(datetime.timezone.utc)

if should_update:
  next_run = lm_dt + datetime.timedelta(days=1, minutes=10)
else:
  h = lm_dt.hour
  if 10 <= h < 15:
    next_run = now_dt + datetime.timedelta(hours=1)
  else:
    next_run = lm_dt + datetime.timedelta(days=1, minutes=10)

out = {
  "lm_utc": lm_dt.isoformat().replace("+00:00", "Z"),
  "now_utc": now_dt.isoformat().replace("+00:00", "Z"),
  "next_run_ms": int(next_run.timestamp() * 1000),
}
print(json.dumps(out))
PY

next_run_ms="$(jq -r '.next_run_ms' "$calc_out")"

# expone outputs al workflow
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "updated=$should_update" >> "$GITHUB_OUTPUT"
  echo "next_run_ms=$next_run_ms" >> "$GITHUB_OUTPUT"
fi

# si no hay update, salimos OK
if [[ "$should_update" != "true" ]]; then
  echo "No update needed (ETag unchanged or 304)."
  exit 0
fi

zip_path="$tmp/sde.zip"
curl -sS -L "$SDE_URL" -o "$zip_path"

new_dir="${OUT_DIR}.__new"
old_dir="${OUT_DIR}.__old"

rm -rf "$new_dir"
mkdir -p "$new_dir"

# preserva _README.md si existe
if [[ -f "${OUT_DIR}/_README.md" ]]; then
  cp -f "${OUT_DIR}/_README.md" "${new_dir}/_README.md"
fi

# genera todo en new_dir
python3 - "$zip_path" "$new_dir" "$url_effective" "$etag" "$last_modified" "$content_length" "$cache_control" <<'PY'
import sys, json, gzip, zipfile, datetime, email.utils
from typing import Any, Dict, Optional, Tuple, List

zip_path, out_dir = sys.argv[1], sys.argv[2]
url_effective, etag, last_modified, content_length, cache_control = sys.argv[3:8]

def pick_member(z: zipfile.ZipFile, needles: List[str]) -> str:
    names = z.namelist()
    low_map = [(n, n.lower()) for n in names]
    for needle in needles:
        needle = needle.lower()
        for n, low in low_map:
            if low.endswith(needle) or needle in low:
                return n
    raise RuntimeError(f"No encontré member para {needles}. ZIP contiene: {names[:50]} ... total={len(names)}")

def get_id(o: Dict[str, Any], keys: List[str]) -> Optional[int]:
    for k in keys:
        if k in o and o[k] is not None:
            try:
                return int(o[k])
            except Exception:
                pass
    return None

def get_name(o: Dict[str, Any]) -> Optional[str]:
    n = o.get("name")
    if isinstance(n, dict):
        return n.get("en") or next((v for v in n.values() if isinstance(v, str) and v.strip()), None)
    if isinstance(n, str):
        return n
    # fallback: algunos datasets podrían usar stationName/typeName directamente
    for k in ("systemName","regionName","stationName","typeName"):
        v = o.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None

def write_jsonl_gz(path: str, rows: List[Dict[str, Any]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z")

with zipfile.ZipFile(zip_path) as z:
    # OJO: tras el rework 2025, muchos ficheros son "mapX" :contentReference[oaicite:15]{index=15}
    systems_member = pick_member(z, ["mapsolarsystems.jsonl", "solarsystems.jsonl"])
    regions_member = pick_member(z, ["mapregions.jsonl", "regions.jsonl"])
    stargates_member = pick_member(z, ["mapstargates.jsonl", "stargates.jsonl"])
    types_member = pick_member(z, ["types.jsonl"])
    stations_member = None
    for cand in ["npcstations.jsonl", "stations.jsonl", "mapstations.jsonl"]:
        try:
            stations_member = pick_member(z, [cand])
            break
        except Exception:
            pass
    if stations_member is None:
        raise RuntimeError("No encontré dataset de stations (npcStations/stations/mapStations).")

    # systems
    systems_rows: List[Dict[str, Any]] = []
    system_name_by_id: Dict[int, str] = {}
    with z.open(systems_member) as f:
        for raw in f:
            o = json.loads(raw)
            sid = get_id(o, ["_key","systemId","solarSystemId","solarSystemID"])
            name = get_name(o)
            if sid is None or not name:
                continue
            system_name_by_id[sid] = name
    for sid in sorted(system_name_by_id):
        systems_rows.append({"systemId": sid, "systemName": system_name_by_id[sid]})

    # regions
    region_name_by_id: Dict[int, str] = {}
    with z.open(regions_member) as f:
        for raw in f:
            o = json.loads(raw)
            rid = get_id(o, ["_key","regionId","regionID"])
            name = get_name(o)
            if rid is None or not name:
                continue
            region_name_by_id[rid] = name
    regions_rows = [{"regionId": rid, "regionName": region_name_by_id[rid]} for rid in sorted(region_name_by_id)]

    # types (published=true)
    type_name_by_id: Dict[int, str] = {}
    with z.open(types_member) as f:
        for raw in f:
            o = json.loads(raw)
            pub = o.get("published", None)
            if pub is not True:
                continue
            tid = get_id(o, ["_key","typeId","typeID"])
            name = get_name(o)
            if tid is None or not name:
                continue
            type_name_by_id[tid] = name
    types_rows = [{"typeId": tid, "typeName": type_name_by_id[tid]} for tid in sorted(type_name_by_id)]

    # stations
    station_name_by_id: Dict[int, str] = {}
    with z.open(stations_member) as f:
        for raw in f:
            o = json.loads(raw)
            stid = get_id(o, ["_key","stationID","stationId"])
            name = get_name(o)
            if stid is None or not name:
                continue
            station_name_by_id[stid] = name
    stations_rows = [{"stationID": stid, "stationName": station_name_by_id[stid]} for stid in sorted(station_name_by_id)]

    # stargates (necesita cruce: gate -> origin system, gate -> destination gate -> destination system)
    gate_origin_sys: Dict[int, int] = {}
    gate_dest_gate: Dict[int, int] = {}

    def extract_dest_gate_id(o: Dict[str, Any]) -> Optional[int]:
        # tolerante a esquemas: destinationStargateId, destinationStargateID, destination:{stargateId}, etc.
        for k in ("destinationStargateId","destinationStargateID","destinationGateId","destinationGateID"):
            if k in o and o[k] is not None:
                try: return int(o[k])
                except: pass
        d = o.get("destination")
        if isinstance(d, dict):
            for k in ("stargateId","stargateID","_key","gateId","gateID"):
                if k in d and d[k] is not None:
                    try: return int(d[k])
                    except: pass
        return None

    with z.open(stargates_member) as f:
        for raw in f:
            o = json.loads(raw)
            gid = get_id(o, ["_key","stargateId","stargateID","gateId","gateID"])
            origin_sid = get_id(o, ["systemId","solarSystemId","solarSystemID"])
            dest_gid = extract_dest_gate_id(o)
            if gid is None or origin_sid is None or dest_gid is None:
                continue
            gate_origin_sys[gid] = origin_sid
            gate_dest_gate[gid] = dest_gid

    stargates_rows: List[Dict[str, Any]] = []
    for gid in sorted(gate_origin_sys):
        origin_sid = gate_origin_sys.get(gid)
        dest_gid = gate_dest_gate.get(gid)
        dest_sid = gate_origin_sys.get(dest_gid)  # el gate destino vive en el system destino
        if origin_sid is None or dest_sid is None:
            continue

        o_name = system_name_by_id.get(origin_sid, str(origin_sid))
        d_name = system_name_by_id.get(dest_sid, str(dest_sid))
        stargate_name = f"{o_name} → {d_name}"

        left_sid, right_sid = (origin_sid, dest_sid) if origin_sid <= dest_sid else (dest_sid, origin_sid)
        left_name = system_name_by_id.get(left_sid, str(left_sid))
        right_name = system_name_by_id.get(right_sid, str(right_sid))
        stargate_group = f"{left_name} ↔ {right_name}"

        stargates_rows.append({
            "stargateId": gid,
            "stargateName": stargate_name,
            "stargateGroup": stargate_group
        })

    # write outputs
    write_jsonl_gz(f"{out_dir}/gh_sde_systems.jsonl.gz", systems_rows)
    write_jsonl_gz(f"{out_dir}/gh_sde_regions.jsonl.gz", regions_rows)
    write_jsonl_gz(f"{out_dir}/gh_sde_stations.jsonl.gz", stations_rows)
    write_jsonl_gz(f"{out_dir}/gh_sde_types.jsonl.gz", types_rows)
    write_jsonl_gz(f"{out_dir}/gh_sde_stargates.jsonl.gz", stargates_rows)

    meta = {
        "schemaVersion": 1,
        "source": {
            "latestUrl": "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip",
            "effectiveUrl": url_effective
        },
        "http": {
            "etag": etag,
            "lastModified": last_modified,
            "contentLength": int(content_length) if str(content_length).isdigit() else None,
            "cacheControl": cache_control
        },
        "build": {
            "updated": True,
            "checkedAtUtc": checked_at,
            "generatedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z")
        },
        "outputs": {
            "gh_sde_systems": {"path": "data/gh_sde/gh_sde_systems.jsonl.gz", "records": len(systems_rows)},
            "gh_sde_regions": {"path": "data/gh_sde/gh_sde_regions.jsonl.gz", "records": len(regions_rows)},
            "gh_sde_stations": {"path": "data/gh_sde/gh_sde_stations.jsonl.gz", "records": len(stations_rows)},
            "gh_sde_types": {"path": "data/gh_sde/gh_sde_types.jsonl.gz", "records": len(types_rows)},
            "gh_sde_stargates": {"path": "data/gh_sde/gh_sde_stargates.jsonl.gz", "records": len(stargates_rows)}
        }
    }
    with open(f"{out_dir}/_meta.json", "w", encoding="utf-8") as fmeta:
        json.dump(meta, fmeta, ensure_ascii=False, indent=2)
        fmeta.write("\n")
PY

# SWAP atómico del directorio completo (evita estado parcial)
rm -rf "$old_dir" || true
if [[ -d "$OUT_DIR" ]]; then
  mv "$OUT_DIR" "$old_dir"
fi
mv "$new_dir" "$OUT_DIR"
rm -rf "$old_dir" || true

echo "Updated datasets written to $OUT_DIR"
