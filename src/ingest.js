import { gates, transportTypes } from './universe.js';

const FEED = 'https://r2z2.zkillboard.com/ephemeral/';
const HEADERS = { 'User-Agent': 'EVE-Route-Intel/0.2 (public route intelligence)' };
const gatesBySystem = new Map();
for (const [id, [system, , x, y, z]] of Object.entries(gates)) {
  if (!gatesBySystem.has(system)) gatesBySystem.set(system, []);
  gatesBySystem.get(system).push([Number(id), x, y, z]);
}

function gateAt(system, position) {
  if (!position || !Number.isFinite(position.x) || !Number.isFinite(position.y) || !Number.isFinite(position.z)) return 0;
  let best = 100000 ** 2;
  let nearest = 0;
  for (const [id, x, y, z] of gatesBySystem.get(system) || []) {
    const distance = (x - position.x) ** 2 + (y - position.y) ** 2 + (z - position.z) ** 2;
    if (distance < best) { best = distance; nearest = id; }
  }
  return nearest;
}

export async function ingest(env) {
  let state = await env.DB.prepare('SELECT next_sequence FROM ingest_state WHERE id=1').first();
  if (!state) {
    const result = await fetch(FEED + 'sequence.json', { headers: HEADERS });
    if (!result.ok) throw new Error(`R2Z2 sequence ${result.status}`);
    const { sequence } = await result.json();
    if (!Number.isSafeInteger(sequence)) throw new Error('Invalid R2Z2 sequence');
    await env.DB.prepare("INSERT INTO ingest_state(id,next_sequence,updated_at,status) VALUES(1,?,datetime('now'),'starting')").bind(sequence).run();
    state = { next_sequence: sequence };
  }
  let next = state.next_sequence;
  const statements = [];
  for (let i = 0; i < 40; i++) {
    const result = await fetch(FEED + next + '.json', { headers: HEADERS });
    if (result.status === 404) break;
    if (!result.ok) throw new Error(`R2Z2 ${next}: ${result.status}`);
    const item = await result.json();
    const mail = item.esi || item;
    const id = item.killmail_id || mail.killmail_id;
    const system = mail.solar_system_id;
    const occurred = mail.killmail_time?.replace('T', ' ').replace('Z', '');
    if (id && system && occurred) {
      const gate = gateAt(system, mail.victim?.position);
      const transport = transportTypes[mail.victim?.ship_type_id] ? 1 : 0;
      const gank = item.zkb?.labels?.includes('ganked') ? 1 : 0;
      const isk = Number(item.zkb?.totalValue) || 0;
      const hour = occurred.slice(0, 13) + ':00:00';
      statements.push(
        env.DB.prepare("INSERT OR IGNORE INTO kills(killmail_id,sequence_id,occurred_at,received_at,solar_system_id,victim_ship_type_id,attacker_count,is_gank,isk_value,gate_id,is_transport) VALUES(?,?,?,datetime('now'),?,?,?,?,?,?,?)")
          .bind(id, next, occurred, system, mail.victim?.ship_type_id ?? null, mail.attackers?.length ?? 0, gank, isk, gate, transport),
        env.DB.prepare('INSERT INTO activity_hour(hour_utc,solar_system_id,gate_id,kills,ganks,transports,high_value,isk_value) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(hour_utc,solar_system_id,gate_id) DO UPDATE SET kills=kills+1,ganks=ganks+excluded.ganks,transports=transports+excluded.transports,high_value=high_value+excluded.high_value,isk_value=isk_value+excluded.isk_value')
          .bind(hour, system, gate, 1, gank, transport, isk >= 1e9 ? 1 : 0, isk),
      );
    }
    next++;
  }
  await env.DB.batch([...statements,
    env.DB.prepare("UPDATE ingest_state SET next_sequence=?,updated_at=datetime('now'),status='ok' WHERE id=1").bind(next),
  ]);
}
