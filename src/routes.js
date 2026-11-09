import { systems, gates } from './universe.js';

const ESI = 'https://esi.evetech.net';
const HEADERS = { 'User-Agent': 'EVE-Route-Intel/0.2 (public route intelligence)', 'X-Compatibility-Date': '2025-09-30' };
const names = new Map(Object.entries(systems).map(([id, [name]]) => [name.toLowerCase(), Number(id)]));
const edgeGate = new Map(Object.entries(gates).map(([id, [from, to]]) => [`${from}:${to}`, Number(id)]));

export function systemId(value) {
  const text = String(value || '').trim();
  if (/^\d{8}$/.test(text) && systems[text]) return Number(text);
  return names.get(text.toLowerCase());
}

export function searchSystems(q) {
  const text = String(q || '').trim().toLowerCase();
  if (text.length < 2) return [];
  return Object.entries(systems).filter(([, [name]]) => name.toLowerCase().includes(text)).slice(0, 12)
    .map(([id, [name, security]]) => ({ id: Number(id), name, security }));
}

async function esiRoute(from, to, preference = 'Shorter', avoid = []) {
  const result = await fetch(`${ESI}/route/${from}/${to}`, {
    method: 'POST', headers: { ...HEADERS, 'Content-Type': 'application/json' },
    body: JSON.stringify({ preference, security_penalty: 50, avoid_systems: avoid }),
  });
  if (!result.ok) throw new Error(`ESI route ${result.status}`);
  const data = await result.json();
  return Array.isArray(data) ? data : data.route;
}

export async function latestState(db) {
  const state = await db.prepare('SELECT next_sequence,updated_at,status FROM ingest_state WHERE id=1').first();
  const age = state ? Date.now() - Date.parse(state.updated_at.replace(' ', 'T') + 'Z') : null;
  return { ...state, age_seconds: age === null ? null : Math.max(0, Math.round(age / 1000)), fresh: age !== null && age < 4 * 60 * 1000 };
}

export function risk(stats, security, profile, fresh) {
  if (!fresh) return { level: 'unknown', score: null, reason: 'Datos desactualizados' };
  const recent = Number(stats.kills_2h || 0);
  const transport = Number(stats.transports_2h || 0);
  const ganks = Number(stats.ganks_2h || 0);
  const high = Number(stats.high_value_2h || 0);
  const gate = Number(stats.gate_kills_2h || 0);
  if (profile === 'freighter' && security < 0.45) return { level: 'red', score: 12, reason: 'Lowsec o nullsec: riesgo crítico para cargueros' };
  const weight = profile === 'freighter' ? 1.5 : profile === 'industrial' ? 1.2 : 1;
  const score = Math.round(weight * (recent + transport * 3 + ganks * 4 + high * 2 + gate * 2) + (security < 0.45 ? 4 : 0));
  const level = score >= 12 ? 'red' : score >= 4 ? 'yellow' : 'green';
  const reason = ganks ? `${ganks} gank(s) recientes` : transport ? `${transport} transporte(s) destruidos` : gate ? `${gate} kill(s) cerca de esta puerta` : recent ? `${recent} kill(s) recientes` : security < 0.45 ? 'Espacio de seguridad baja o nula' : 'Sin actividad publicada reciente';
  return { level, score, reason };
}

export async function recentStats(db, ids) {
  const map = new Map();
  for (let start = 0; start < ids.length; start += 80) {
    const batch = ids.slice(start, start + 80);
    if (!batch.length) continue;
    const sql = `SELECT solar_system_id,gate_id,count(*) AS kills_24h,sum(CASE WHEN occurred_at>=datetime('now','-2 hours') THEN 1 ELSE 0 END) AS kills_2h,sum(CASE WHEN occurred_at>=datetime('now','-2 hours') THEN is_gank ELSE 0 END) AS ganks_2h,sum(CASE WHEN occurred_at>=datetime('now','-2 hours') THEN is_transport ELSE 0 END) AS transports_2h,sum(CASE WHEN occurred_at>=datetime('now','-2 hours') AND isk_value>=1000000000 THEN 1 ELSE 0 END) AS high_value_2h,max(occurred_at) AS latest_kill FROM kills WHERE solar_system_id IN (${batch.map(() => '?').join(',')}) AND occurred_at>=datetime('now','-1 day') GROUP BY solar_system_id,gate_id`;
    const { results } = await db.prepare(sql).bind(...batch).all();
    for (const row of results) {
      if (!map.has(row.solar_system_id)) map.set(row.solar_system_id, []);
      map.get(row.solar_system_id).push(row);
    }
  }
  return map;
}

function decorate(ids, stats, profile, fresh) {
  return ids.map((id, index) => {
    const [name, security] = systems[id] || [String(id), 0];
    const next = ids[index + 1];
    const gateId = next ? edgeGate.get(`${id}:${next}`) || 0 : 0;
    const rows = stats.get(id) || [];
    const sum = (key) => rows.reduce((total, row) => total + Number(row[key] || 0), 0);
    const values = { kills_24h: sum('kills_24h'), kills_2h: sum('kills_2h'), ganks_2h: sum('ganks_2h'), transports_2h: sum('transports_2h'), high_value_2h: sum('high_value_2h'), gate_kills_2h: Number(rows.find(row => row.gate_id === gateId)?.kills_2h || 0) };
    return { id, name, security, gate_id: gateId, gate_to: next ? systems[next]?.[0] || String(next) : null, ...values, ...risk(values, security, profile, fresh) };
  });
}

export async function buildRoute(db, from, to, profile) {
  const state = await latestState(db);
  const shortest = await esiRoute(from, to);
  const global = await db.prepare("SELECT solar_system_id,count(*) AS kills_2h,sum(is_gank) AS ganks_2h,sum(is_transport) AS transports_2h,sum(CASE WHEN isk_value>=1000000000 THEN 1 ELSE 0 END) AS high_value_2h FROM kills WHERE occurred_at>=datetime('now','-2 hours') GROUP BY solar_system_id").all();
  const avoid = state.fresh ? global.results.filter(row => risk(row, systems[row.solar_system_id]?.[1] ?? 0, profile, true).level === 'red').map(row => row.solar_system_id).filter(id => id !== from && id !== to).slice(0, 100) : [];
  const alternatePreference = profile === 'freighter' || profile === 'industrial' ? 'Safer' : 'Shorter';
  const [safe, alternate] = await Promise.allSettled([esiRoute(from, to, 'Safer'), esiRoute(from, to, alternatePreference, avoid)]);
  const routes = [
    { kind: 'shortest', name: 'Más corta', ids: shortest },
    { kind: 'safer', name: 'Más segura (ESI)', ids: safe.status === 'fulfilled' ? safe.value : shortest },
    { kind: 'avoid', name: 'Evitar rojos', ids: alternate.status === 'fulfilled' ? alternate.value : shortest, available: alternate.status === 'fulfilled' },
  ];
  const allIds = [...new Set(routes.flatMap(r => r.ids))];
  const stats = await recentStats(db, allIds);
  return { from, to, profile, freshness: state, routes: routes.map(r => ({ kind: r.kind, name: r.name, available: r.available !== false, jumps: Math.max(0, r.ids.length - 1), systems: decorate(r.ids, stats, profile, state.fresh) })), caveat: 'Los killmails publicados pueden llegar tarde. Verde no garantiza seguridad.' };
}

export async function history(db, id, gate, days) {
  if (!id || ![7, 14, 30, 60, 90, 180].includes(days) || !Number.isSafeInteger(gate) || gate < 0 || (gate && gates[gate]?.[0] !== id)) return null;
  const sql = "SELECT substr(hour_utc,1,10) AS day,sum(kills) AS kills,sum(ganks) AS ganks,sum(transports) AS transports,sum(high_value) AS high_value,sum(isk_value) AS isk_value FROM activity_hour WHERE solar_system_id=? AND hour_utc>=datetime('now',?)" + (gate ? ' AND gate_id=?' : '') + ' GROUP BY substr(hour_utc,1,10) ORDER BY day';
  const rows = await db.prepare(sql).bind(...(gate ? [id, `-${days} days`, gate] : [id, `-${days} days`])).all();
  return { system: { id, name: systems[id][0] }, gate: gate ? { id: gate, to: systems[gates[gate][1]]?.[0] } : null, days, series: rows.results, retention: 'Agregados horarios durante seis meses desde el inicio de la recogida.' };
}

export function decorateSystems(ids, stats, fresh) { return decorate(ids, stats, 'freighter', fresh); }
