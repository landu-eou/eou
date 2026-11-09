ALTER TABLE kills ADD COLUMN gate_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kills ADD COLUMN is_transport INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS activity_hour (
  hour_utc TEXT NOT NULL,
  solar_system_id INTEGER NOT NULL,
  gate_id INTEGER NOT NULL DEFAULT 0,
  kills INTEGER NOT NULL DEFAULT 0,
  ganks INTEGER NOT NULL DEFAULT 0,
  transports INTEGER NOT NULL DEFAULT 0,
  high_value INTEGER NOT NULL DEFAULT 0,
  isk_value REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (hour_utc, solar_system_id, gate_id)
);
CREATE INDEX IF NOT EXISTS activity_hour_system ON activity_hour(solar_system_id, hour_utc);
INSERT INTO activity_hour(hour_utc,solar_system_id,gate_id,kills,ganks,transports,high_value,isk_value)
SELECT substr(occurred_at,1,13) || ':00:00',solar_system_id,0,count(*),sum(is_gank),0,
       sum(CASE WHEN isk_value >= 1000000000 THEN 1 ELSE 0 END),coalesce(sum(isk_value),0)
FROM kills GROUP BY substr(occurred_at,1,13),solar_system_id;
