CREATE TABLE IF NOT EXISTS kills (
  killmail_id INTEGER PRIMARY KEY,
  sequence_id INTEGER NOT NULL,
  occurred_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  solar_system_id INTEGER NOT NULL,
  victim_ship_type_id INTEGER,
  attacker_count INTEGER NOT NULL DEFAULT 0,
  is_gank INTEGER NOT NULL DEFAULT 0,
  isk_value REAL
);
CREATE INDEX IF NOT EXISTS kills_system_time ON kills(solar_system_id, occurred_at);
CREATE TABLE IF NOT EXISTS ingest_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  next_sequence INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  status TEXT NOT NULL
);
