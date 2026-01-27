#!/usr/bin/env bash
set -euo pipefail

: "${SDE_URL:=https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip}"
: "${OUT_DIR:=data/gh_sde}"
: "${META_PATH:=${OUT_DIR}/_meta.json}"
: "${FORCE_REBUILD:=false}"   # true|false (case-insensitive). Fuerza rebuild aunque ETag no cambie.

SYSTEMS_OUT="gh_sde_systems.jsonl.gz"
REGIONS_OUT="gh_sde_regions.jsonl.gz"
STATIONS_OUT="gh_sde_stations.jsonl.gz"
TYPES_OUT="gh_sde_types.jsonl.gz"
STARGATES_OUT="gh_sde_stargates.jsonl.gz"

# Validaciones duras (mínimos “razonables” para detectar fallos de parsing)
MIN_SYSTEMS=5000
MIN_REGIONS=50
MIN_TYPES=10000
MIN_STATIONS=1000
MIN_STARGATES=1000

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$OUT_DIR"

# -----------------------------
# Helpers
# -----------------------------
lower() { printf "%s" "$1" | tr '[:upper:]' '[:lower:]'; }

is_true() {
  case "$(lower "${1:-false}")" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

hdr_get_from() {
  local file="$1"
  local key="$2"
  awk -v k="$key" 'BEGIN{IGNORECASE=1} $0 ~ "^"k":" {sub(/^[^:]+:[[:space:]]*/, "", $0); gsub(/\r/,""); print $0}' "$file" | tail -n 1
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

require_int() {
  local name="$1"
  local val="$2"
  if [[ ! "$val" =~ ^[0-9]+$ ]]; then
    echo "ERROR: '$name' no es numérico: '$val'"
    exit 2
  fi
}

# -----------------------------
# Load persisted meta (if any)
# -----------------------------
stored_etag=""
stored_last_modified=""
if [[ -f "$META_PATH" ]]; then
  stored_etag="$(jq -r '.http.etag // empty' "$META_PATH" || true)"
  stored_last_modified="$(jq -r '.http.lastModified // empty' "$META_PATH" || true)"
fi

forced="false"
if is_true "$FORCE_REBUILD"; then
  forced="true"
fi

# -----------------------------
# HEAD request (conditional unless forced)
# -----------------------------
headers_file="$tmp/headers.txt"

curl_head=(curl -sS -L -I -D "$headers_file" -o /dev/null -w "%{http_code}\n%{url_effective}\n")
if [[ "$forced" != "true" ]]; then
  if [[ -n "$stored_etag" ]]; then
    curl_head+=(-H "If-None-Match: $stored_etag")
  elif [[ -n "$stored_last_modified" ]]; then
    curl_head+=(-H "If-Modified-Since: $stored_last_modified")
  fi
fi
curl_head+=("$SDE_URL")

info="$("${curl_head[@]}")"
http_code="$(printf "%s" "$info" | sed -n '1p')"
url_effective="$(printf "%s" "$info" | sed -n '2p')"

etag="$(hdr_get_from "$headers_file" "ETag")"
last_modified="$(hdr_get_from "$headers_file" "Last-Modified")"
content_length="$(hdr_get_from "$headers_file" "Content-Length")"
cache_control="$(hdr_get_from "$headers_file" "Cache-Control")"

# ETag es imprescindible para el flujo normal.
if [[ -z "$etag" ]]; then
  echo "ERROR: falta ETag en headers. Sin ETag no podemos decidir updates correctamente."
  exit 2
fi

# Last-Modified puede faltar en 304 (o por CDN). Recupera de meta si existe.
if [[ -z "$last_modified" && -n "$stored_last_modified" ]]; then
  last_modified="$stored_last_modified"
fi

# Si sigue faltando, reintenta HEAD no condicional (solo para obtener LM)
if [[ -z "$last_modified" ]]; then
  echo "WARN: Last-Modified vacío en HEAD. Reintentando HEAD no condicional..."
  headers_file2="$tmp/headers2.txt"
  info2="$(curl -sS -L -I -D "$headers_file2" -o /dev/null -w "%{http_code}\n%{url_effective}\n" "$SDE_URL")"
  http_code2="$(printf "%s" "$info2" | sed -n '1p')"
  url_effective2="$(printf "%s" "$info2" | sed -n '2p')"
  if [[ -n "$url_effective2" ]]; then
    url_effective="$url_effective2"
  fi
  # OJO: aquí podría cambiar el http_code pero solo nos interesa LM.
  last_modified="$(hdr_get_from "$headers_file2" "Last-Modified")"
  if [[ -z "$content_length" ]]; then
    content_length="$(hdr_get_from "$headers_file2" "Content-Length")"
  fi
  if [[ -z "$cache_control" ]]; then
    cache_control="$(hdr_get_from "$headers_file2" "Cache-Control")"
  fi
fi

if [[ -z "$last_modified" ]]; then
  echo "ERROR: Last-Modified sigue vacío. No puedo calcular next_run de forma fiable."
  echo "ETag='$etag' Last-Modified='$last_modified'"
  exit 2
fi

# -----------------------------
# Decide whether SDE changed / updated
# -----------------------------
sde_changed="false"
case "$http_code" in
  304)
    sde_changed="false"
    ;;
  200)
    if [[ -n "$stored_etag" && "$etag" == "$stored_etag" ]]; then
      sde_changed="false"
    else
      sde_changed="true"
    fi
    ;;
  *)
    # Algunos proxies raros devuelven 302/301 a HEAD (aunque -L debería seguirlos).
    # Aquí preferimos fallar duro para no escribir estado inconsistente.
    echo "ERROR: HTTP inesperado en HEAD: $http_code"
    exit 2
    ;;
esac

updated="false"
if [[ "$forced" == "true" || "$sde_changed" == "true" ]]; then
  updated="true"
fi

# -----------------------------
# Compute scheduling (next_run_ms, expires_sec)
# Reglas:
# - Si updated==true  => next_run = Last-Modified + 1d + 10m
# - Si updated==false =>
#     - si LM hour in [10,15) => next_run = now + 1h
#     - else => next_run = LM + 1d + 10m
# expires_sec = ceil((next_run - now)/1000), clamp >= 0
# -----------------------------
calc_out="$tmp/sched.json"
python3 - "$updated" "$last_modified" > "$calc_out" <<'PY'
import sys, json, datetime, email.utils

updated = (sys.argv[1].lower() == "true")
lm_str = sys.argv[2]

lm_dt = email.utils.parsedate_to_datetime(lm_str)
if lm_dt.tzinfo is None:
    lm_dt = lm_dt.replace(tzinfo=datetime.timezone.utc)
lm_dt = lm_dt.astimezone(datetime.timezone.utc)

now_dt = datetime.datetime.now(datetime.timezone.utc)

if updated:
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

print(json.dumps({
    "lm_utc": lm_dt.isoformat().replace("+00:00","Z"),
    "now_utc": now_dt.isoformat().replace("+00:00","Z"),
    "next_run_ms": next_run_ms,
    "expires_sec": expires_sec,
}))
PY

next_run_ms="$(jq -r '.next_run_ms' "$calc_out")"
expires_sec="$(jq -r '.expires_sec' "$calc_out")"
require_int "next_run_ms" "$next_run_ms"
require_int "expires_sec" "$expires_sec"

# Outputs para Actions SIEMPRE (aunque no haya update)
write_outputs "$updated" "$sde_changed" "$forced" "$next_run_ms" "$expires_sec"

# -----------------------------
# Early exit if not updating
# -----------------------------
if [[ "$updated" != "true" ]]; then
  echo "No update needed (304 or same ETag)."
  exit 0
fi

# -----------------------------
# Download & build datasets (atomic swap)
# -----------------------------
zip_path="$tmp/sde.zip"
curl -sS -L "$SDE_URL" -o "$zip_path"

new_dir="${OUT_DIR}.__new"
old_dir="${OUT_DIR}.__old"
rm -rf "$new_dir" "$old_dir"
mkdir -p "$new_dir"

# preserva README del dataset si existe
if [[ -f "${OUT_DIR}/_README.md" ]]; then
  cp -f "${OUT_DIR}/_README.md" "${new_dir}/_README.md"
fi

python3 - "$zip_path" "$new_dir" "$url_effective" "$etag" "$last_modified" "$content_length" "$cache_control" \
  "$MIN_SYSTEMS" "$MIN_REGIONS" "$MIN_TYPES" "$MIN_STATIONS" "$MIN_STARGATES" "$forced" "$sde_changed" "$updated" <<'PY'
import sys, os, json, gzip, zipfile, datetime
from typing import Any, Dict, Optional, Tuple

zip_path, out_dir = sys.argv[1], sys.argv[2]
url_effective, etag, last_modified, content_length, cache_control = sys.argv[3:8]
MIN_SYSTEMS, MIN_REGIONS, MIN_TYPES, MIN_STATIONS, MIN_STARGATES = map(int, sys.argv[8:13])
forced = (sys.argv[13].lower() == "true")
sde_changed = (sys.argv[14].lower() == "true")
updated = (sys.argv[15].lower() == "true")

def pick_exact(z: zipfile.ZipFile, target_basename: str) -> str:
    t = target_basename.lower()
    for name in z.namelist():
        if os.path.basename(name).lower() == t:
            return name
    raise RuntimeError(f"No encontré '{target_basename}' (basename exacto) dentro del ZIP.")

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
    vals = [
        (1000,"M"),(900,"CM"),(500,"D"),(400,"CD"),
        (100,"C"),(90,"XC"),(50,"L"),(40,"XL"),
        (10,"X"),(9,"IX"),(5,"V"),(4,"IV"),(1,"I")
    ]
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

    # Systems
    system_name: Dict[int, str] = {}
    with z.open(m_mapSolarSystems) as f:
        for raw in f:
            o = json.loads(raw)
            sid = get_int(o, "_key", "solarSystemID", "solarSystemId", "systemId")
            name = get_name_en(o, "name")
            if sid is None or not name:
                continue
            system_name[sid] = name
    if len(system_name) < MIN_SYSTEMS:
        raise RuntimeError(f"VALIDATION FAIL: systems={len(system_name)} < {MIN_SYSTEMS}.")

    # Regions
    region_name: Dict[int, str] = {}
    with z.open(m_mapRegions) as f:
        for raw in f:
            o = json.loads(raw)
            rid = get_int(o, "_key", "regionID", "regionId")
            name = get_name_en(o, "name")
            if rid is None or not name:
                continue
            region_name[rid] = name
    if len(region_name) < MIN_REGIONS:
        raise RuntimeError(f"VALIDATION FAIL: regions={len(region_name)} < {MIN_REGIONS}.")

    # Types (published)
    type_name: Dict[int, str] = {}
    with z.open(m_types) as f:
        for raw in f:
            o = json.loads(raw)
            if o.get("published") is not True:
                continue
            tid = get_int(o, "_key", "typeID", "typeId")
            name = get_name_en(o, "name")
            if tid is None or not name:
                continue
            type_name[tid] = name
    if len(type_name) < MIN_TYPES:
        raise RuntimeError(f"VALIDATION FAIL: published types={len(type_name)} < {MIN_TYPES}.")

    # Celestiales
    stars: Dict[int, int] = {}
    planets: Dict[int, Tuple[int,int]] = {}
    moons: Dict[int, Tuple[int,int]] = {}
    belts: Dict[int, Tuple[int,int]] = {}
    explicit_name: Dict[int, str] = {}

    def load_explicit_name(o: Dict[str, Any], cid: Optional[int]):
        if cid is None: return
        nm = get_name_en(o, "name")
        if nm:
            explicit_name[cid] = nm

    with z.open(m_mapStars) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key")
            ssid = get_int(o, "solarSystemID", "solarSystemId", "systemId")
            if cid is not None and ssid is not None:
                stars[cid] = ssid
            load_explicit_name(o, cid)

    with z.open(m_mapPlanets) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key")
            ssid = get_int(o, "solarSystemID", "solarSystemId")
            cidx = get_int(o, "celestialIndex")
            if cid is not None and ssid is not None and cidx is not None:
                planets[cid] = (ssid, cidx)
            load_explicit_name(o, cid)

    with z.open(m_mapMoons) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key")
            oid = get_int(o, "orbitID", "orbitId")
            oidx = get_int(o, "orbitIndex")
            if cid is not None and oid is not None and oidx is not None:
                moons[cid] = (oid, oidx)
            load_explicit_name(o, cid)

    with z.open(m_mapAsteroidBelts) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key")
            oid = get_int(o, "orbitID", "orbitId")
            oidx = get_int(o, "orbitIndex")
            if cid is not None and oid is not None and oidx is not None:
                belts[cid] = (oid, oidx)
            load_explicit_name(o, cid)

    orbit_cache: Dict[int, str] = {}

    def orbit_name(celestial_id: int) -> Optional[str]:
        if celestial_id in orbit_cache:
            return orbit_cache[celestial_id]
        if celestial_id in explicit_name:
            orbit_cache[celestial_id] = explicit_name[celestial_id]
            return orbit_cache[celestial_id]

        if celestial_id in stars:
            ssid = stars[celestial_id]
            nm = system_name.get(ssid)
            if nm:
                orbit_cache[celestial_id] = nm
                return nm

        if celestial_id in planets:
            ssid, cidx = planets[celestial_id]
            ssn = system_name.get(ssid)
            if ssn:
                nm = f"{ssn} {roman(cidx)}"
                orbit_cache[celestial_id] = nm
                return nm

        if celestial_id in moons:
            parent_id, oidx = moons[celestial_id]
            parent_nm = orbit_name(parent_id)
            if parent_nm:
                nm = f"{parent_nm} - Moon {oidx}"
                orbit_cache[celestial_id] = nm
                return nm

        if celestial_id in belts:
            parent_id, oidx = belts[celestial_id]
            parent_nm = orbit_name(parent_id)
            if parent_nm:
                nm = f"{parent_nm} - Asteroid Belt {oidx}"
                orbit_cache[celestial_id] = nm
                return nm

        return None

    # Corp & Operations
    corp_name: Dict[int, str] = {}
    with z.open(m_npcCorporations) as f:
        for raw in f:
            o = json.loads(raw)
            cid = get_int(o, "_key", "corporationID", "corporationId")
            nm = get_name_en(o, "name")
            if cid is not None and nm:
                corp_name[cid] = nm

    op_name: Dict[int, str] = {}
    with z.open(m_stationOperations) as f:
        for raw in f:
            o = json.loads(raw)
            oid = get_int(o, "_key", "operationID", "operationId")
            nm = get_name_en(o, "operationName") or get_name_en(o, "name")
            if oid is not None and nm:
                op_name[oid] = nm

    # Stations
    stations_total = 0
    stations_written = 0
    stations_rows_list = []

    with z.open(m_npcStations) as f:
        for raw in f:
            o = json.loads(raw)
            stations_total += 1

            station_id = get_int(o, "_key", "stationID", "stationId")
            if station_id is None:
                continue

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
            if not orb:
                orb = ssn or "Unknown"

            corp = corp_name.get(owner_id, f"Corp {owner_id}" if owner_id is not None else "Unknown Corp")
            if use_op:
                opn = op_name.get(op_id, f"Op {op_id}" if op_id is not None else "Unknown Op")
                station_name = f"{orb} - {corp} {opn}"
            else:
                station_name = f"{orb} - {corp}"

            stations_rows_list.append((station_id, {"stationID": station_id, "stationName": station_name}))
            stations_written += 1

    stations_rows = (row for _, row in sorted(stations_rows_list, key=lambda x: x[0]))

    if stations_written < MIN_STATIONS:
        raise RuntimeError(f"VALIDATION FAIL: stations_written={stations_written} < {MIN_STATIONS}.")
    if stations_total > 0:
        ratio = stations_written / stations_total
        if ratio < 0.90:
            raise RuntimeError(f"VALIDATION FAIL: stations_written_ratio={ratio:.3f} < 0.90.")

    # Stargates
    stargates_rows_list = []
    with z.open(m_mapStargates) as f:
        for raw in f:
            o = json.loads(raw)
            gid = get_int(o, "_key", "stargateID", "stargateId")
            if gid is None:
                continue

            origin_sid = get_int(o, "solarSystemID", "solarSystemId")
            dest = o.get("destination") if isinstance(o.get("destination"), dict) else {}
            dest_sid = get_int(dest, "solarSystemID", "solarSystemId")

            if origin_sid is None or dest_sid is None:
                continue

            o_name = system_name.get(origin_sid, str(origin_sid))
            d_name = system_name.get(dest_sid, str(dest_sid))
            stargate_name = f"{o_name} → {d_name}"

            left_sid, right_sid = (origin_sid, dest_sid) if origin_sid <= dest_sid else (dest_sid, origin_sid)
            left_name = system_name.get(left_sid, str(left_sid))
            right_name = system_name.get(right_sid, str(right_sid))
            stargate_group = f"{left_name} ↔ {right_name}"

            stargates_rows_list.append((gid, {"stargateId": gid, "stargateName": stargate_name, "stargateGroup": stargate_group}))

    stargates_count = len(stargates_rows_list)
    if stargates_count < MIN_STARGATES:
        raise RuntimeError(f"VALIDATION FAIL: stargates={stargates_count} < {MIN_STARGATES}.")

    stargates_rows = (row for _, row in sorted(stargates_rows_list, key=lambda x: x[0]))

    # Escribir outputs
    systems_rows = ({"systemId": sid, "systemName": system_name[sid]} for sid in sorted(system_name))
    regions_rows = ({"regionId": rid, "regionName": region_name[rid]} for rid in sorted(region_name))
    types_rows   = ({"typeId": tid, "typeName": type_name[tid]} for tid in sorted(type_name))

    write_jsonl_gz(os.path.join(out_dir, "gh_sde_systems.jsonl.gz"), systems_rows)
    write_jsonl_gz(os.path.join(out_dir, "gh_sde_regions.jsonl.gz"), regions_rows)
    write_jsonl_gz(os.path.join(out_dir, "gh_sde_types.jsonl.gz"), types_rows)
    write_jsonl_gz(os.path.join(out_dir, "gh_sde_stations.jsonl.gz"), stations_rows)
    write_jsonl_gz(os.path.join(out_dir, "gh_sde_stargates.jsonl.gz"), stargates_rows)

    # Verificar que no estén vacíos (hard)
    def gz_has_lines(path: str) -> int:
        n = 0
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for _ in f:
                n += 1
                if n >= 3:
                    break
        return n

    chk = {
        "systems": gz_has_lines(os.path.join(out_dir, "gh_sde_systems.jsonl.gz")),
        "regions": gz_has_lines(os.path.join(out_dir, "gh_sde_regions.jsonl.gz")),
        "types": gz_has_lines(os.path.join(out_dir, "gh_sde_types.jsonl.gz")),
        "stations": gz_has_lines(os.path.join(out_dir, "gh_sde_stations.jsonl.gz")),
        "stargates": gz_has_lines(os.path.join(out_dir, "gh_sde_stargates.jsonl.gz")),
    }
    for k,v in chk.items():
        if v == 0:
            raise RuntimeError(f"VALIDATION FAIL: output {k} gzip tiene 0 líneas (vacío).")

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
            "updated": bool(updated),
            "forced": bool(forced),
            "sdeChanged": bool(sde_changed),
            "checkedAtUtc": checked_at,
            "generatedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z")
        },
        "validation": {
            "min": {
                "systems": MIN_SYSTEMS,
                "regions": MIN_REGIONS,
                "types": MIN_TYPES,
                "stations": MIN_STATIONS,
                "stargates": MIN_STARGATES
            },
            "counts": {
                "systems": len(system_name),
                "regions": len(region_name),
                "types": len(type_name),
                "stations_written": stations_written,
                "stations_total": stations_total,
                "stargates": stargates_count
            }
        },
        "outputs": {
            "gh_sde_systems": {"path": "data/gh_sde/gh_sde_systems.jsonl.gz", "records": len(system_name)},
            "gh_sde_regions": {"path": "data/gh_sde/gh_sde_regions.jsonl.gz", "records": len(region_name)},
            "gh_sde_types": {"path": "data/gh_sde/gh_sde_types.jsonl.gz", "records": len(type_name)},
            "gh_sde_stations": {"path": "data/gh_sde/gh_sde_stations.jsonl.gz", "records": stations_written},
            "gh_sde_stargates": {"path": "data/gh_sde/gh_sde_stargates.jsonl.gz", "records": stargates_count}
        }
    }

    with open(os.path.join(out_dir, "_meta.json"), "w", encoding="utf-8") as fmeta:
        json.dump(meta, fmeta, ensure_ascii=False, indent=2)
        fmeta.write("\n")

PY

# Swap atómico del directorio completo (evita estado parcial)
if [[ -d "$OUT_DIR" ]]; then
  mv "$OUT_DIR" "$old_dir"
fi
mv "$new_dir" "$OUT_DIR"
rm -rf "$old_dir"

echo "Updated datasets written to $OUT_DIR"

# Validación extra en bash: tamaños no nulos
for f in "$OUT_DIR/$SYSTEMS_OUT" "$OUT_DIR/$REGIONS_OUT" "$OUT_DIR/$TYPES_OUT" "$OUT_DIR/$STATIONS_OUT" "$OUT_DIR/$STARGATES_OUT" "$OUT_DIR/_meta.json"; do
  if [[ ! -s "$f" ]]; then
    echo "ERROR: archivo esperado vacío o inexistente: $f"
    exit 3
  fi
done
