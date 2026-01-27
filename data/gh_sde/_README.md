
---

## `data/gh_sde/_README.md`

```md
# GH SDE · Dataset en repo (persistente)

Este directorio contiene salidas derivadas del **SDE oficial de CCP** (ZIP JSONL “latest”) para que otros workflows resuelvan IDs → nombres sin usar ESI.

## Principios
- **Persistente en repo** (no artifacts).
- **Actualización sólo cuando el SDE cambia**, detectado por `ETag` / `Last-Modified` HTTP.
- **Ficheros comprimidos**: JSONL + gzip (`*.jsonl.gz`) para eficiencia y streaming.

## Archivos
- `_meta.json`  
  Fuente de verdad y metadatos del build (ETag/Last-Modified, etc.)
- `gh_sde_systems.jsonl.gz`
- `gh_sde_regions.jsonl.gz`
- `gh_sde_stations.jsonl.gz`
- `gh_sde_types.jsonl.gz`
- `gh_sde_stargates.jsonl.gz`

---

## Contrato: formato común (`*.jsonl.gz`)
- Compresión: gzip
- Contenido: JSON Lines (1 JSON por línea)
- Encoding: UTF-8
- Orden: ordenado ascendente por el ID principal (differences reproducibles)
- Sin campos extra (contrato estricto)

---

## `gh_sde_systems.jsonl.gz`
Cada línea:
```json
{"systemId": 30000001, "systemName": "Tanoo"}
