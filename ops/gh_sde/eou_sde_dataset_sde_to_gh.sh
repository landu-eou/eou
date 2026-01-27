#!/usr/bin/env bash
set -euo pipefail

: "${SDE_URL:=https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip}"
: "${OUT_DIR:=data/gh_sde}"
: "${META_PATH:=${OUT_DIR}/eou_sde_dataset_sde_to_gh.json}"
: "${FORCE_REBUILD:=false}"   # true|false (case-insensitive)

SYSTEMS_OUT="gh_sde_ss.jsonl.gz"
REGIONS_OUT="gh_sde_rg.jsonl.gz"
STATIONS_OUT="gh_sde_st.jsonl.gz"
TYPES_OUT="gh_sde_ty.jsonl.gz"
STARGATES_OUT="gh_sde_sg.jsonl.gz"

MIN_SYSTEMS=5000
MIN_REGIONS=50
MIN_TYPES=10000
MIN_STATIONS=1000
MIN_STARGATES=1000

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$OUT_DIR"

lower() { printf "%s" "$1" | tr '[:upper:]' '[:lower:]'; }
is_true() { case "$(lower "${1:-false}")" in 1|true|yes|y|on) return 0;; *) return 1;; esac; }

norm_etag() {
  # normaliza: trim + quita comillas dobles exteriores si existen
  local s="${1:-}"
  s="$(printf "%s" "$s" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
  # quita comillas exteriores "...."
  if [[ "$s" =~ ^\".*\"$ ]]; then
    s="${s:1:${#s}-2}"
  fi
  printf "%s" "$s"
}

hdr_get_from() {
  local file="$1" key="$2"
  awk -v k="$key" 'BEGIN{IGNORECASE=1} $0 ~ "^"k":" {sub(/^[^:]+:[[:space:]]*/, "", $0); gsub(/\r/,""); print $0}' "$file" | tail -n 1
}

require_int() {
  local name="$1" val="$2"
  if [[ ! "$val" =~ ^[0-9]+$ ]]; then
    echo "ERROR: '$name' no es numérico: '$val'"
    exit 2
  fi
}

write_outputs() {
  # $1 updated, $2 sde_changed, $3 forced, $4 next_run_ms, $5 expires_sec
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "updated=$1" >> "$GITHUB_OUTPUT"
    echo "sde_changed=$2" >> "$GITHUB_OUTPUT"
    echo "forced=$3" >> "$GITHUB_OUTPUT"
    echo "next_run_ms=$4" >> "$GITHUB_OUTPUT"
    echo "expires_sec=$5" >> "$GITHUB_OUTPUT"
  fi
}

stored_etag_raw=""
stored_last_modified=""
if [[ -f "$META_PATH" ]]; then
  stored_etag_raw="$(jq -r '.http.etag // empty' "$META_PATH" || true)"
  stored_last_modified="$(jq -r '.http.lastModified // empty' "$META_PATH" || true)"
fi

stored_etag="$(norm_etag "$stored_etag_raw")"

forced="false"
if is_true "$FORCE_REBUILD"; then forced="true"; fi

headers_file="$tmp/headers.txt"

curl_head=(curl -sS -L -I -D "$headers_file" -o /dev/null -w "%{http_code}\n%{url_effective}\n")
if [[ "$forced" != "true" ]]; then
  if [[ -n "$stored_etag_raw" ]]; then
    curl_head+=(-H "If-None-Match: $stored_etag_raw")
  elif [[ -n "$stored_last_modified" ]]; then
    curl_head+=(-H "If-Modified-Since: $stored_last_modified")
  fi
fi
curl_head+=("$SDE_URL")

info="$("${curl_head[@]}")"
http_code="$(printf "%s" "$info" | sed -n '1p')"
url_effective="$(printf "%s" "$info" | sed -n '2p')"

etag_raw="$(hdr_get_from "$headers_file" "ETag")"
last_modified="$(hdr_get_from "$headers_file" "Last-Modified")"
content_length="$(hdr_get_from "$headers_file" "Content-Length")"
cache_control="$(hdr_get_from "$headers_file" "Cache-Control")"

if [[ -z "$etag_raw" ]]; then
  echo "ERROR: falta ETag en headers."
  exit 2
fi
etag="$(norm_etag "$etag_raw")"

if [[ -z "$last_modified" && -n "$stored_last_modified" ]]; then
  last_modified="$stored_last_modified"
fi

if [[ -z "$last_modified" ]]; then
  echo "WARN: Last-Modified vacío en HEAD. Reintentando HEAD no condicional..."
  headers_file2="$tmp/headers2.txt"
  info2="$(curl -sS -L -I -D "$headers_file2" -o /dev/null -w "%{http_code}\n%{url_effective}\n" "$SDE_URL")"
  url_effective2="$(printf "%s" "$info2" | sed -n '2p')"
  if [[ -n "$url_effective2" ]]; then url_effective="$url_effective2"; fi
  last_modified="$(hdr_get_from "$headers_file2" "Last-Modified")"
  if [[ -z "$content_length" ]]; then content_length="$(hdr_get_from "$headers_file2" "Content-Length")"; fi
  if [[ -z "$cache_control" ]]; then cache_control="$(hdr_get_from "$headers_file2" "Cache-Control")"; fi
fi

if [[ -z "$last_modified" ]]; then
  echo "ERROR: Last-Modified sigue vacío. No puedo calcular next_run."
  echo "ETag_raw='$etag_raw' Last-Modified='$last_modified'"
  exit 2
fi

# --- Decide sde_changed (solo si hay stored_etag) ---
# Bootstrap (stored_etag vacío) NO cuenta como “cambio upstream”
sde_changed="false"
bootstrap="false"
if [[ -z "$stored_etag" ]]; then
  bootstrap="true"
  sde_changed="false"
else
  if [[ "$etag" != "$stored_etag" ]]; then
    sde_changed="true"
  fi
fi

# --- Decide updated ---
# updated = forced OR sde_changed OR bootstrap (para construir la primera vez)
updated="false"
if [[ "$forced" == "true" || "$sde_changed" == "true" || "$bootstrap" == "true" ]]; then
  updated="true"
fi

# --- Decide should_update network-wise (download/parse) ---
# Si NO updated => salir.
# Si updated => descargar + parsear.
# (Este diseño evita que un “bootstrap” se marque como sdeChanged.)
# Scheduling: usa "sde_changed OR forced" como “update real”; bootstrap cae en “no-update” para ventana 10–15.
real_update="false"
if [[ "$forced" == "true" || "$sde_changed" == "true" ]]; then
  real_update="true"
fi

# --- Compute next_run/expires ---
calc_out="$tmp/sched.json"
python3 - "$real_update" "$last_modified" > "$calc_out" <<'PY'
import sys, json, datetime, email.utils
real_update = (sys.argv[1].lower() == "true")
lm_str = sys.argv[2]
lm_dt = email.utils.parsedate_to_datetime(lm_str)
if lm_dt.tzinfo is None:
    lm_dt = lm_dt.replace(tzinfo=datetime.timezone.utc)
lm_dt = lm_dt.astimezone(datetime.timezone.utc)
now_dt = datetime.datetime.now(datetime.timezone.utc)

if real_update:
    next_run = lm_dt + datetime.timedelta(days=1, minutes=10)
else:
    h = lm_dt.hour
    if 10 <= h < 15:
        next_run = now_dt + datetime.timedelta(hours=1)
    else:
        next_run = lm_dt + datetime.timedelta(days=1, minutes=10)

next_run_ms = int(next_run.timestamp() * 1000)
now_ms = int(now_dt.timestamp() * 1000)
diff_ms = max(0, next_run_ms - now_ms)
expires_sec = (diff_ms + 999) // 1000

print(json.dumps({"next_run_ms": next_run_ms, "expires_sec": expires_sec}))
PY

next_run_ms="$(jq -r '.next_run_ms' "$calc_out")"
expires_sec="$(jq -r '.expires_sec' "$calc_out")"
require_int "next_run_ms" "$next_run_ms"
require_int "expires_sec" "$expires_sec"

write_outputs "$updated" "$sde_changed" "$forced" "$next_run_ms" "$expires_sec"

if [[ "$updated" != "true" ]]; then
  echo "No update needed (same ETag, not forced)."
  exit 0
fi

zip_path="$tmp/sde.zip"
curl -sS -L "$SDE_URL" -o "$zip_path"

new_dir="${OUT_DIR}.__new"
old_dir="${OUT_DIR}.__old"
rm -rf "$new_dir" "$old_dir"
mkdir -p "$new_dir"

if [[ -f "${OUT_DIR}/eou_sde_dataset_sde_to_gh__rm.md" ]]; then
  cp -f "${OUT_DIR}/eou_sde_dataset_sde_to_gh__rm.md" "${new_dir}/eou_sde_dataset_sde_to_gh__rm.md"
fi

python3 - "$zip_path" "$new_dir" "$url_effective" "$etag_raw" "$last_modified" "$content_length" "$cache_control" \
  "$MIN_SYSTEMS" "$MIN_REGIONS" "$MIN_TYPES" "$MIN_STATIONS" "$MIN_STARGATES" \
  "$SYSTEMS_OUT" "$REGIONS_OUT" "$TYPES_OUT" "$STATIONS_OUT" "$STARGATES_OUT" \
  "$bootstrap" "$forced" "$sde_changed" "$real_update" <<'PY'
import sys, os, json, gzip, zipfile, datetime
from typing import Any, Dict, Optional, Tuple

zip_path, out_dir = sys.argv[1], sys.argv[2]
url_effective, etag_raw, last_modified, content_length, cache_control = sys.argv[3:8]
MIN_SYSTEMS, MIN_REGIONS, MIN_TYPES, MIN_STATIONS, MIN_STARGATES = map(int, sys.argv[8:13])
SYSTEMS_OUT, REGIONS_OUT, TYPES_OUT, STATIONS_OUT, STARGATES_OUT = sys.argv[13:18]
bootstrap = (sys.argv[18].lower() == "true")
forced = (sys.argv[19].lower() == "true")
sde_changed = (sys.argv[20].lower() == "true")
real_update = (sys.argv[21].lower() == "true")

def pick_exact(z: zipfile.ZipFile, target_basename: str) -> str:
    t = target_basename.lower()
    for name in z.namelist():
        if os.path.basename(name).lower() == t:
            return name
    raise RuntimeError(f"No encontré '{target_basename}' dentro del ZIP.")

def as_int(v: Any) -> Optional[int]:
    try:
        if v is None: return None
        return int(v)
    except Exception:
        return None

def get_int(o: Dict[str, Any], *keys: str) -> Optional[int]:
    for k in keys:
        if k in o:
            x = as_int(o.get(k))
            if x is not None:
                return x
    return None

def get_name_en(o: Dict[str, Any], field: str = "name") -> Optional[str]:
    v = o.get(field)
    if isinstance(v, dict):
        s = v.get("en") or v.get("en-us")
        if isinstance(s, str) and s.strip():
            return s.strip()
        for s2 in v.values():
            if isinstance(s2, str) and s2.strip():
                return s2.strip()
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

def roman(n: int) -> str:
    vals = [(1000,"M"),(900,"CM"),(500,"D"),(400,"CD"),(100,"C"),(90,"XC"),(50,"L"),(40,"XL"),(10,"X"),(9,"IX"),(5,"V"),(4,"IV"),(1,"I")]
    out = []
    x = n
    for v,s in vals:
        while x >= v:
            out.append(s); x -= v
    return "".join(out) if out else str(n)

def write_jsonl_gz(path: str, rows):
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z")

with zipfile.ZipFile(zip_path) as z:
    m_mapSolarSystems = pick_exact(z, "mapSolarSystems.jsonl")
    m_mapRegions      = pick_exact(z, "mapRegions.jsonl")
    m_mapStargates    = pick_exact(z, "mapStargates.jsonl")
    m_types           = pick_exact(z, "types.jsonl")

    m_mapStars         = pick_exact(z, "mapStars.jsonl")
    m_mapPlanets       = pick_exact(z, "mapPlanets.jsonl")
    m_mapMoons         = pick_exact(z, "mapMoons.jsonl")
    m_mapAsteroidBelts = pick_exact(z, "mapAsteroidBelts.jsonl")

    m_npcStations       = pick_exact(z, "npcStations.jsonl")
    m_npcCorporations   = pick_exact(z, "npcCorporations.jsonl")
    m_stationOperations = pick_exact(z, "stationOperations.jsonl")

    system_name: Dict[int, str] = {}
    with z.open(m_mapSolarSystems) as f:
        for raw in f:
            o = json.loads(raw)
            sid = get_int(o, "_key", "solarSystemID", "solarSystemId", "systemId")
            name = get_name_en(o, "name")
            if sid is None or not name: continue
            system_name[sid] = name
    if len(system_name) < MIN_SYSTEMS:
        raise RuntimeError("systems validation fail")

    region_name: Dict[int, str] = {}
    with z.open(m_mapRegions) as f:
        for raw in f:
            o = json.loads(raw)
            rid = get_int(o, "_key", "regionID", "regionId")
            name = get_name_en(o, "name")
            if rid is None or not name: continue
            region_name[rid] = name
    if len(region_name) < MIN_REGIONS:
        raise RuntimeError("regions validation fail")

    type_name: Dict[int, str] = {}
    with z.open(m_types) as f:
        for raw in f:
            o = json.loads(raw)
            if o.get("published") is not True:
                continue
            tid = get_int(o, "_key", "typeID", "typeId")
            name = get_name_en(o, "name")
            if tid is None or not name: continue
            type_name[tid] = name
    if len(type_name) < MIN_TYPES:
        raise RuntimeError("types validation fail")

    stars: Dict[int, int] = {}
    planets: Dict[int, Tuple[int,int]] = {}
    moons: Dict[int, Tuple[int,int]] = {}
    belts: Dict[int, Tuple[int,int]] = {}
    explicit_name: Dict[int, str] = {}

    def load_explicit_name(o: Dict[str, Any], cid: Optional[int]):
        if cid is None: return
        nm = get_name_en(o, "name")
        if nm: explicit_name[cid] = nm

    with z.open(m_mapStars) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key"); ssid = get_int(o, "solarSystemID", "solarSystemId", "systemId")
            if cid is not None and ssid is not None: stars[cid] = ssid
            load_explicit_name(o, cid)

    with z.open(m_mapPlanets) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key"); ssid = get_int(o, "solarSystemID", "solarSystemId"); cidx = get_int(o, "celestialIndex")
            if cid is not None and ssid is not None and cidx is not None: planets[cid] = (ssid, cidx)
            load_explicit_name(o, cid)

    with z.open(m_mapMoons) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key"); oid = get_int(o, "orbitID", "orbitId"); oidx = get_int(o, "orbitIndex")
            if cid is not None and oid is not None and oidx is not None: moons[cid] = (oid, oidx)
            load_explicit_name(o, cid)

    with z.open(m_mapAsteroidBelts) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key"); oid = get_int(o, "orbitID", "orbitId"); oidx = get_int(o, "orbitIndex")
            if cid is not None and oid is not None and oidx is not None: belts[cid] = (oid, oidx)
            load_explicit_name(o, cid)

    orbit_cache: Dict[int, str] = {}
    def orbit_name(celestial_id: int) -> Optional[str]:
        if celestial_id in orbit_cache: return orbit_cache[celestial_id]
        if celestial_id in explicit_name:
            orbit_cache[celestial_id] = explicit_name[celestial_id]; return orbit_cache[celestial_id]
        if celestial_id in stars:
            ssid = stars[celestial_id]; nm = system_name.get(ssid)
            if nm: orbit_cache[celestial_id] = nm; return nm
        if celestial_id in planets:
            ssid, cidx = planets[celestial_id]; ssn = system_name.get(ssid)
            if ssn:
                nm = f"{ssn} {roman(cidx)}"; orbit_cache[celestial_id] = nm; return nm
        if celestial_id in moons:
            parent_id, oidx = moons[celestial_id]
            parent_nm = orbit_name(parent_id)
            if parent_nm:
                nm = f"{parent_nm} - Moon {oidx}"; orbit_cache[celestial_id] = nm; return nm
        if celestial_id in belts:
            parent_id, oidx = belts[celestial_id]
            parent_nm = orbit_name(parent_id)
            if parent_nm:
                nm = f"{parent_nm} - Asteroid Belt {oidx}"; orbit_cache[celestial_id] = nm; return nm
        return None

    corp_name: Dict[int, str] = {}
    with z.open(m_npcCorporations) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key", "corporationID", "corporationId")
            nm = get_name_en(o, "name")
            if cid is not None and nm: corp_name[cid] = nm

    op_name: Dict[int, str] = {}
    with z.open(m_stationOperations) as f:
        for raw in f:
            o = json.loads(raw)
            oid = get_int(o, "_key", "operationID", "operationId")
            nm = get_name_en(o, "operationName") or get_name_en(o, "name")
            if oid is not None and nm: op_name[oid] = nm

    stations_total = 0
    stations_written = 0
    stations_rows_list = []

    with z.open(m_npcStations) as f:
        for raw in f:
            o = json.loads(raw)
            stations_total += 1
            station_id = get_int(o, "_key", "stationID", "stationId")
            if station_id is None: continue
            explicit_station = get_name_en(o, "name")
            if explicit_station:
                stations_rows_list.append((station_id, {"stationID": station_id, "stationName": explicit_station}))
                stations_written += 1
                continue
            orbit_id = get_int(o, "orbitID", "orbitId")
            owner_id = get_int(o, "ownerID", "ownerId")
            op_id = get_int(o, "operationID", "operationId")
            use_op = (o.get("useOperationName") is True)
            ssid = get_int(o, "solarSystemID", "solarSystemId")
            ssn = system_name.get(ssid) if ssid is not None else None
            orb = orbit_name(orbit_id) if orbit_id is not None else None
            if not orb: orb = ssn or "Unknown"
            corp = corp_name.get(owner_id, f"Corp {owner_id}" if owner_id is not None else "Unknown Corp")
            if use_op:
                opn = op_name.get(op_id, f"Op {op_id}" if op_id is not None else "Unknown Op")
                station_name = f"{orb} - {corp} {opn}"
            else:
                station_name = f"{orb} - {corp}"
            stations_rows_list.append((station_id, {"stationID": station_id, "stationName": station_name}))
            stations_written += 1

    if stations_written < MIN_STATIONS:
        raise RuntimeError("stations validation fail")
    if stations_total > 0 and stations_written / stations_total < 0.90:
        raise RuntimeError("stations ratio validation fail")

    stations_rows = (row for _, row in sorted(stations_rows_list, key=lambda x: x[0]))

    stargates_rows_list = []
    with z.open(m_mapStargates) as f:
        for raw in f:
            o = json.loads(raw)
            gid = get_int(o, "_key", "stargateID", "stargateId")
            if gid is None: continue
            origin_sid = get_int(o, "solarSystemID", "solarSystemId")
            dest = o.get("destination") if isinstance(o.get("destination"), dict) else {}
            dest_sid = get_int(dest, "solarSystemID", "solarSystemId")
            if origin_sid is None or dest_sid is None: continue
            o_name = system_name.get(origin_sid, str(origin_sid))
            d_name = system_name.get(dest_sid, str(dest_sid))
            stargate_name = f"{o_name} → {d_name}"
            left_sid, right_sid = (origin_sid, dest_sid) if origin_sid <= dest_sid else (dest_sid, origin_sid)
            left_name = system_name.get(left_sid, str(left_sid))
            right_name = system_name.get(right_sid, str(right_sid))
            stargate_group = f"{left_name} ↔ {right_name}"
            stargates_rows_list.append((gid, {"stargateId": gid, "stargateName": stargate_name, "stargateGroup": stargate_group}))

    if len(stargates_rows_list) < MIN_STARGATES:
        raise RuntimeError("stargates validation fail")

    stargates_rows = (row for _, row in sorted(stargates_rows_list, key=lambda x: x[0]))

    systems_rows = ({"systemId": sid, "systemName": system_name[sid]} for sid in sorted(system_name))
    regions_rows = ({"regionId": rid, "regionName": region_name[rid]} for rid in sorted(region_name))
    types_rows   = ({"typeId": tid, "typeName": type_name[tid]} for tid in sorted(type_name))

    write_jsonl_gz(os.path.join(out_dir, SYSTEMS_OUT), systems_rows)
    write_jsonl_gz(os.path.join(out_dir, REGIONS_OUT), regions_rows)
    write_jsonl_gz(os.path.join(out_dir, TYPES_OUT), types_rows)
    write_jsonl_gz(os.path.join(out_dir, STATIONS_OUT), stations_rows)
    write_jsonl_gz(os.path.join(out_dir, STARGATES_OUT), stargates_rows)

    meta = {
        "schemaVersion": 1,
        "source": {
            "latestUrl": "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip",
            "effectiveUrl": url_effective
        },
        "http": {
            "etag": etag_raw,
            "lastModified": last_modified,
            "contentLength": int(content_length) if str(content_length).isdigit() else None,
            "cacheControl": cache_control
        },
        "build": {
            "updated": True,
            "bootstrap": bool(bootstrap),
            "forced": bool(forced),
            "sdeChanged": bool(sde_changed),
            "realUpdate": bool(real_update),
            "checkedAtUtc": checked_at,
            "generatedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z")
        }
    }
    with open(os.path.join(out_dir, "eou_sde_dataset_sde_to_gh.json"), "w", encoding="utf-8") as fmeta:
        json.dump(meta, fmeta, ensure_ascii=False, indent=2)
        fmeta.write("\n")
PY

if [[ -d "$OUT_DIR" ]]; then
  mv "$OUT_DIR" "$old_dir"
fi
mv "$new_dir" "$OUT_DIR"
rm -rf "$old_dir"

echo "Updated datasets written to $OUT_DIR"
