SELECT DISTINCT
  corp.corporation_id AS corporation_id
FROM `eou-ht.eou.gate_kills`,
UNNEST(corporationID) AS corp
WHERE corp.corporation_id IS NOT NULL
ORDER BY corporation_id ASC
