-- Extrae todos los corporation_id únicos presentes en la tabla fuente.
-- corporationID es un ARRAY<STRUCT<corporation_id INT64>>, por eso se usa UNNEST.

SELECT DISTINCT
  corp.corporation_id AS corporation_id
FROM `{{SOURCE_TABLE}}`,
UNNEST(corporationID) AS corp
WHERE corp.corporation_id IS NOT NULL
ORDER BY corporation_id ASC
