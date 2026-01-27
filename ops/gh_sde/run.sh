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

# Validaciones duras (mínimos “razonables” para detectar fallos de parsing)
MIN_SYSTEMS=5000
MIN_REGIONS=50
MIN_TYPES=10000
MIN_STATIONS=1000
MIN_STARGATES=1000

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$OUT_DIR"

stored_etag=""
stored_last_modified=""
if [[ -f "$META_PATH" ]]; then
  stored_etag="$(jq -r '.http.etag // empty' "$META_PATH" || true)"
  stored_last_modified="$(jq -r '.http.lastModified // empty' "$META_PATH" || true)"
fi

headers_file="$tmp/headers.txt"

curl_head=(curl -sS -L -I -D "$headers_file" -o /dev/null -w "%{http_code}\n%{url_effective}\n")
if [[ -n "$stored_etag" ]]; then
  curl_head+=(-H "If-None-Match: $stored_etag")
elif [[ -n "$stored_last_modified" ]]; then
  curl_head+=(-H "If-Modified-Since: $stored_last_modified")
fi
curl_head+=("$SDE_URL")

info="$("${curl_head[@]}")"
http_code="$(printf "%s" "$info" | sed -n '1p')"
url_effective="$(printf "%s" "$info" | sed -n '2p')"

hdr_get() {
  local key="$1"
  awk -v k="$key" 'BEGIN{IGNORECASE=1} $0 ~ "^"k":" {sub(/^[^:]+:[[:space:]]*/, "", $0); gsub(/\r/,""); print $0}' "$headers_file" | tail -n 1
}

etag="$(hdr_get "ETag")"
last_modified="$(hdr_get "Last-Modified")"
content_length="$(hdr_get "Content-Length")"
cache_control="$(hdr_get "Cache-Control")"

if [[ -z "$etag" || -z "$last_modified" ]]; then
  echo "ERROR: faltan ETag o Last-Modified en headers. CCP indica que Static Data soporta ambos; sin ellos no podemos decidir updates correctamente."
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

# next_run según tu regla (basado en Last-Modified + ventana 10-15 UTC)
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

print(json.dumps({
  "lm_utc": lm_dt.isoformat().replace("+00:00","Z"),
  "now_utc": now_dt.isoformat().replace("+00:00","Z"),
  "next_run_ms": int(next_run.timestamp() * 1000),
}))
PY

next_run_ms="$(jq -r '.next_run_ms' "$calc_out")"

# outputs para GitHub Actions
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "updated=$should_update" >> "$GITHUB_OUTPUT"
  echo "next_run_ms=$next_run_ms" >> "$GITHUB_OUTPUT"
fi

if [[ "$should_update" != "true" ]]; then
  echo "No update needed (304 or same ETag)."
  exit 0
fi

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
  "$MIN_SYSTEMS" "$MIN_REGIONS" "$MIN_TYPES" "$MIN_STATIONS" "$MIN_STARGATES" <<'PY'
import sys, os, json, gzip, zipfile, datetime, email.utils
from typing import Any, Dict, Optional, Tuple

zip_path, out_dir = sys.argv[1], sys.argv[2]
url_effective, etag, last_modified, content_length, cache_control = sys.argv[3:8]
MIN_SYSTEMS, MIN_REGIONS, MIN_TYPES, MIN_STATIONS, MIN_STARGATES = map(int, sys.argv[8:13])

def pick_exact(z: zipfile.ZipFile, target_basename: str) -> str:
    """Selecciona un member por basename exacto (case-insensitive), evitando falsos positivos."""
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
        # fallback: cualquier idioma si no hay en
        for s2 in v.values():
            if isinstance(s2, str) and s2.strip():
                return s2.strip()
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

def roman(n: int) -> str:
    # suficiente para celestialIndex típico
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
    # Members exactos (baseline robusta)
    m_mapSolarSystems = pick_exact(z, "mapSolarSystems.jsonl")
    m_mapRegions      = pick_exact(z, "mapRegions.jsonl")
    m_mapStargates    = pick_exact(z, "mapStargates.jsonl")
    m_types           = pick_exact(z, "types.jsonl")

    # Celestiales para deducir orbitName (CCP: no suelen tener 'name', se deduce) :
    # Stars / Planets / Moons / AsteroidBelts
    m_mapStars         = pick_exact(z, "mapStars.jsonl")
    m_mapPlanets       = pick_exact(z, "mapPlanets.jsonl")
    m_mapMoons         = pick_exact(z, "mapMoons.jsonl")
    m_mapAsteroidBelts = pick_exact(z, "mapAsteroidBelts.jsonl")

    # Stations naming: npcStations + npcCorporations + stationOperations
    m_npcStations      = pick_exact(z, "npcStations.jsonl")
    m_npcCorporations  = pick_exact(z, "npcCorporations.jsonl")
    m_stationOperations= pick_exact(z, "stationOperations.jsonl")

    # ---- Systems (systemId -> systemName) ----
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
        raise RuntimeError(f"VALIDATION FAIL: systems={len(system_name)} < {MIN_SYSTEMS}. ¿mapSolarSystems parsing roto?")

    systems_rows = ({"systemId": sid, "systemName": system_name[sid]} for sid in sorted(system_name))

    # ---- Regions (regionId -> regionName) ----
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
        raise RuntimeError(f"VALIDATION FAIL: regions={len(region_name)} < {MIN_REGIONS}. ¿mapRegions parsing roto?")
    regions_rows = ({"regionId": rid, "regionName": region_name[rid]} for rid in sorted(region_name))

    # ---- Types published (typeId -> typeName) ----
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
        raise RuntimeError(
            f"VALIDATION FAIL: published types={len(type_name)} < {MIN_TYPES}. "
            f"Esto suele indicar que NO se ha leído 'types.jsonl' correcto o que 'published/name' no se están parseando bien."
        )
    types_rows = ({"typeId": tid, "typeName": type_name[tid]} for tid in sorted(type_name))

    # ---- Celestial maps (para orbitName) ----
    # Guardamos solo campos mínimos
    stars: Dict[int, int] = {}          # starId -> solarSystemID
    planets: Dict[int, Tuple[int,int]] = {}  # planetId -> (solarSystemID, celestialIndex)
    moons: Dict[int, Tuple[int,int]] = {}    # moonId -> (orbitID, orbitIndex)
    belts: Dict[int, Tuple[int,int]] = {}    # beltId -> (orbitID, orbitIndex)
    explicit_name: Dict[int, str] = {}  # excepciones: si el celestial trae 'name', se usa

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

    # ---- Corp & Operations ----
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

    # ---- Stations (npcStations) ----
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

            # Excepción: si viene name explícito, lo usamos
            explicit_station = get_name_en(o, "name")
            if explicit_station:
                stations_rows_list.append((station_id, {"stationID": station_id, "stationName": explicit_station}))
                stations_written += 1
                continue

            orbit_id = get_int(o, "orbitID", "orbitId")
            owner_id = get_int(o, "ownerID", "ownerId")
            op_id = get_int(o, "operationID", "operationId")
            use_op = (o.get("useOperationName") is True)

            # solarSystemID solo para fallback
            ssid = get_int(o, "solarSystemID", "solarSystemId")
            ssn = system_name.get(ssid) if ssid is not None else None

            orb = orbit_name(orbit_id) if orbit_id is not None else None
            if not orb:
                # fallback conservador: evita perder la estación completa
                orb = ssn or "Unknown"

            corp = corp_name.get(owner_id, f"Corp {owner_id}" if owner_id is not None else "Unknown Corp")

            if use_op:
                opn = op_name.get(op_id, f"Op {op_id}" if op_id is not None else "Unknown Op")
                station_name = f"{orb} - {corp} {opn}"
            else:
                station_name = f"{orb} - {corp}"

            stations_rows_list.append((station_id, {"stationID": station_id, "stationName": station_name}))
            stations_written += 1

    # Orden estable por stationID
    stations_rows = (row for _, row in sorted(stations_rows_list, key=lambda x: x[0]))

    if stations_written < MIN_STATIONS:
        raise RuntimeError(f"VALIDATION FAIL: stations_written={stations_written} < {MIN_STATIONS}. Deduction/parsing roto.")
    if stations_total > 0:
        ratio = stations_written / stations_total
        if ratio < 0.90:
            raise RuntimeError(f"VALIDATION FAIL: stations_written_ratio={ratio:.3f} < 0.90. Algo grave falla en parsing/deducción.")

    # ---- Stargates ----
    # CCP indica que para stargates se usa destination.solarSystemID para resolver nombre destino. (y solarSystemID para origen)
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
        raise RuntimeError(f"VALIDATION FAIL: stargates={stargates_count} < {MIN_STARGATES}. ¿mapStargates parsing roto o falta destination.solarSystemID?")

    stargates_rows = (row for _, row in sorted(stargates_rows_list, key=lambda x: x[0]))

    # ---- Escribir outputs ----
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
            "updated": True,
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

# Fin python
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
