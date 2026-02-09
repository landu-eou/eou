# route_test

Genera una ruta (walk) que empieza en **Jita** y visita todos los sistemas alcanzables por stargate usando solo SDE local.

## Inputs (repo)
- data/sde/solarsystems.jsonl.gz
- data/sde/stargates.jsonl.gz

## Outputs (route_test/)
- route.jsonl          -> {"route":[...]}
- route_names.json     -> ["Jita", ...] (pretty)
- route.meta.json      -> métricas + sha256 de inputs

## Run local
python route_test/build_route_cover.py
