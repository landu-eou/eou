# GH SDE (SDE → GitHub) · Operación

Este módulo genera y mantiene en el repo (persistente) un “mini-dataset” derivado del **SDE oficial de CCP** (ZIP JSONL “latest”) para que otros workflows puedan resolver IDs → nombres sin llamar a ESI.

**Objetivo**: mantener en `data/gh_sde/` los ficheros `*.jsonl.gz` + `_meta.json`, actualizándolos **solo cuando cambie el SDE** (según headers HTTP) y haciendo el reemplazo **atómico** (sin estados parciales).

## Fuente de datos (solo CCP oficial)
- URL “latest”:
  - https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip
- CCP documenta que los recursos de Static Data soportan **ETag** y **Last-Modified** y que “solo se actualizan cuando cambian”.  
  (Esto permite un “check” barato mediante HEAD/condicional y evita descargas innecesarias.)

## Fuente de verdad (actualización sí/no)
Persistimos en `data/gh_sde/_meta.json`:
- `http.etag` (primaria)
- `http.lastModified` (secundaria y usada para scheduling)

El flujo usa request condicional:
- si existe `etag` guardado → HEAD con `If-None-Match: <etag>`
- si responde `304 Not Modified` → **no hay update**
- si responde `200` con ETag distinto → **sí hay update**

## Concurrencia (anti-solapamiento)
El workflow usa `concurrency` a nivel workflow con:
- `cancel-in-progress: false`

Comportamiento esperado según GitHub:
- En un mismo grupo puede haber **máximo 1 run “running” y 1 run “pending”**.
- Si entra otro disparo mientras hay uno pending, GitHub cancela el pending anterior y deja el más reciente.
- Para cancelar el “running” haría falta `cancel-in-progress: true` (NO lo usamos para evitar cortar actualizaciones a medias).

## Sin artifacts; commit en repo (persistencia)
No se generan artifacts de GitHub Actions.
Las salidas se escriben en `data/gh_sde/` y se comitean en un único commit cuando hay update.

## Escritura atómica (sin estados parciales)
`run.sh` genera todo en `data/gh_sde.__new/` y al final hace un “swap” de directorio:
1) mueve `data/gh_sde/` → `data/gh_sde.__old/`
2) mueve `data/gh_sde.__new/` → `data/gh_sde/`
3) borra `data/gh_sde.__old/`

Así:
- nunca quedan ficheros a medias,
- nunca quedan “restos” del dataset anterior.

## Requisitos del runner
En `ubuntu-latest` ya vienen normalmente:
- bash, curl, jq, openssl, python3

## Secrets necesarios (gateway)
Este workflow reporta START/FINISH al gateway, siguiendo el patrón del workflow de test.

Secrets esperados:
- `GATEWAY_URL`
- `GATEWAY_SECRET`

## Permisos para commitear
Se usa `GITHUB_TOKEN`.
Requisitos:
- en el YAML: `permissions: contents: write`
- en Settings del repo: “Workflow permissions” debe permitir escritura (read/write) si el repo está en modo restringido.

## Ejecución local (debug)
Ejemplo (sin gateway):
```bash
export SDE_URL="https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip"
export OUT_DIR="data/gh_sde"
export META_PATH="data/gh_sde/_meta.json"
bash ops/gh_sde/run.sh

