const $ = (id) => document.getElementById(id);
const state = { data: null, active: 'avoid', selected: null, scope: 'system', busy: false };
const labels = { green: 'VERDE', yellow: 'AMARILLO', red: 'ROJO', unknown: 'SIN DATOS' };
const escapeHtml = (text) => String(text ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const formatAge = (seconds) => seconds == null ? 'Sin lectura' : seconds < 60 ? `Hace ${seconds} s` : `Hace ${Math.floor(seconds / 60)} min`;

async function request(path) {
  const response = await fetch(path, { headers: { Accept: 'application/json' } });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Error al cargar los datos');
  return data;
}

async function feedStatus() {
  try {
    const data = await request('/api/health');
    const badge = $('feed-status');
    badge.className = `feed-status ${data.state.fresh ? 'online' : 'offline'}`;
    badge.textContent = data.state.fresh ? `Feed activo · ${formatAge(data.state.age_seconds)}` : 'Feed retrasado';
  } catch {
    $('feed-status').className = 'feed-status offline';
    $('feed-status').textContent = 'Feed no disponible';
  }
}

let searchTimer;
async function suggest(input) {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(async () => {
    const q = input.value.trim();
    if (q.length < 2) return;
    try {
      const data = await request(`/api/search?q=${encodeURIComponent(q)}`);
      $('system-options').replaceChildren(...data.systems.map(system => {
        const option = document.createElement('option');
        option.value = system.name;
        option.label = `${system.name} · ${system.security.toFixed(1)}`;
        return option;
      }));
    } catch { /* Suggestions are optional. */ }
  }, 180);
}

function activeRoute() {
  return state.data?.routes.find(route => route.kind === state.active) || state.data?.routes[0];
}

function render() {
  const data = state.data;
  if (!data) return;
  $('results').classList.remove('hidden');
  $('route-title').textContent = `${data.routes[0].systems[0].name} → ${data.routes[0].systems.at(-1).name}`;
  $('data-age').textContent = data.freshness.fresh ? `Última lectura: ${formatAge(data.freshness.age_seconds)}` : 'Datos desactualizados: semáforos suspendidos';
  const tabs = $('route-tabs');
  tabs.replaceChildren(...data.routes.map(route => {
    const button = document.createElement('button');
    button.type = 'button';
    button.setAttribute('role', 'tab');
    button.setAttribute('aria-selected', String(route.kind === state.active));
    button.className = route.kind === state.active ? 'active' : '';
    button.textContent = route.name;
    button.disabled = !route.available;
    button.onclick = () => { state.active = route.kind; state.selected = null; render(); selectSystem(activeRoute().systems[0]); };
    return button;
  }));
  const route = activeRoute();
  const reds = route.systems.filter(s => s.level === 'red').length;
  const yellows = route.systems.filter(s => s.level === 'yellow').length;
  $('route-summary').innerHTML = `<strong>${route.jumps} saltos</strong><span class="red-count">${reds} rojos</span><span>${yellows} amarillos</span><span>Perfil: ${escapeHtml($('ship').selectedOptions[0].textContent)}</span>`;
  const list = $('route-list');
  list.replaceChildren(...route.systems.map((system, index) => {
    const li = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `route-row ${state.selected?.id === system.id ? 'selected' : ''}`;
    button.innerHTML = `<span class="route-row-main"><span class="step-no">${String(index + 1).padStart(2, '0')}</span><span><span class="system-name">${escapeHtml(system.name)}</span><span class="gate-name">${system.gate_to ? `Puerta hacia ${escapeHtml(system.gate_to)}` : 'Destino final'} · sec ${system.security.toFixed(1)}</span></span></span><span class="activity-count">${system.kills_2h} / 2 h</span><span class="risk-chip ${system.level}">${labels[system.level]}</span>`;
    button.setAttribute('aria-label', `${system.name}, riesgo ${labels[system.level]}, ${system.kills_2h} kills en dos horas`);
    button.onclick = () => selectSystem(system);
    li.append(button);
    return li;
  }));
}

async function selectSystem(system) {
  state.selected = system;
  state.scope = 'system';
  $('detail-name').textContent = system.name;
  $('detail-subtitle').textContent = `${system.reason}. Seguridad ${system.security.toFixed(1)}. ${system.gate_to ? `Puerta hacia ${system.gate_to}.` : 'Destino final.'}`;
  $('detail-metrics').innerHTML = `<div class="metric"><small>KILLS / 2 H</small><strong>${system.kills_2h}</strong></div><div class="metric"><small>KILLS / 24 H</small><strong>${system.kills_24h}</strong></div><div class="metric"><small>TRANSPORTES / 2 H</small><strong>${system.transports_2h}</strong></div><div class="metric"><small>GANKS / 2 H</small><strong>${system.ganks_2h}</strong></div>`;
  const scope = $('history-scope');
  scope.replaceChildren();
  const all = document.createElement('button');
  all.textContent = 'Sistema'; all.className = 'active';
  all.onclick = () => { state.scope = 'system'; updateScope(); loadHistory(); };
  scope.append(all);
  if (system.gate_id) {
    const gate = document.createElement('button');
    gate.textContent = `Puerta a ${system.gate_to}`;
    gate.onclick = () => { state.scope = 'gate'; updateScope(); loadHistory(); };
    scope.append(gate);
  }
  render();
  await loadHistory();
}

function updateScope() {
  [...$('history-scope').children].forEach((button, index) => button.classList.toggle('active', (state.scope === 'system' ? 0 : 1) === index));
}

async function loadHistory() {
  const system = state.selected;
  if (!system) return;
  const selectedId = system.id;
  const days = Number($('history-days').value);
  const gate = state.scope === 'gate' ? system.gate_id : 0;
  $('history-chart').innerHTML = '<span class="empty">Cargando histórico…</span>';
  try {
    const data = await request(`/api/history?system=${selectedId}&gate=${gate}&days=${days}`);
    if (state.selected?.id !== selectedId) return;
    const chart = $('history-chart');
    chart.replaceChildren();
    if (!data.series.length) chart.innerHTML = '<span class="empty">Aún no hay actividad registrada en este periodo. El histórico empieza desde la activación del recolector.</span>';
    else {
      const max = Math.max(1, ...data.series.map(day => day.kills));
      for (const day of data.series) {
        const bar = document.createElement('div');
        bar.className = 'bar';
        bar.style.height = `${Math.max(4, Math.round(day.kills / max * 148))}px`;
        bar.title = `${day.day}: ${day.kills} kills, ${day.transports} transportes, ${day.ganks} ganks`;
        chart.append(bar);
      }
    }
    chart.setAttribute('aria-label', `${data.series.reduce((sum, day) => sum + day.kills, 0)} kills registrados en ${days} días`);
    $('history-note').textContent = data.retention;
  } catch (error) {
    $('history-chart').innerHTML = `<span class="empty">${escapeHtml(error.message)}</span>`;
  }
}

async function plan() {
  if (state.busy) return;
  state.busy = true;
  const button = $('route-form').querySelector('button[type="submit"]');
  button.disabled = true;
  $('form-message').textContent = 'Calculando rutas y leyendo actividad…';
  try {
    const query = new URLSearchParams({ from: $('from').value.trim(), to: $('to').value.trim(), ship: $('ship').value });
    state.data = await request(`/api/route?${query}`);
    state.active = state.data.routes.find(route => route.kind === 'avoid' && route.available)?.kind || 'shortest';
    state.selected = null;
    render();
    if (activeRoute().systems.length) await selectSystem(activeRoute().systems[0]);
    $('form-message').textContent = state.data.caveat;
  } catch (error) {
    $('form-message').textContent = error.message;
  } finally { state.busy = false; button.disabled = false; }
}

async function refreshLive() {
  await feedStatus();
  if (!state.data || document.hidden || state.busy) return;
  const route = activeRoute();
  const ids = route.systems.map(system => system.id).slice(0, 50);
  try {
    const data = await request(`/api/systems?ids=${ids.join(',')}`);
    const fresh = new Map(data.systems.map(system => [system.id, system]));
    let newRed = false;
    for (const item of route.systems) {
      const update = fresh.get(item.id);
      if (!update) continue;
      if (item.level !== 'red' && update.level === 'red') newRed = true;
      Object.assign(item, update);
    }
    state.data.freshness = data.freshness;
    if (newRed) await plan();
    else { render(); if (state.selected) { const current = route.systems.find(item => item.id === state.selected.id); if (current) state.selected = current; } }
  } catch { /* Feed badge already shows connection state. */ }
}

for (const id of ['from', 'to']) $(id).addEventListener('input', event => suggest(event.target));
$('route-form').addEventListener('submit', event => { event.preventDefault(); plan(); });
$('history-days').addEventListener('change', loadHistory);
feedStatus();
plan();
setInterval(refreshLive, 30000);
