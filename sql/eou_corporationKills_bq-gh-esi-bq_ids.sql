SELECT DISTINCT
  corp.corporation_id AS corporation_id
FROM `{{SOURCE_TABLE}}`,
UNNEST(corporationID) AS corp
WHERE corp.corporation_id IS NOT NULL
ORDER BY corporation_id ASC
