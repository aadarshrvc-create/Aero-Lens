'use strict';

const state = {
  session: null,
  files: [],
  selectedFiles: new Set(),
  selectedHists: new Set(),
  payload: null,
  errors: null,         // /api/errors report for the current window
  errorsKey: null,      // window+file fingerprint the report was built for
  tab: 'latency',
  range: null,          // shared zoom window [t0, t1] in epoch seconds
  cluster: null,
  reports: [],
  savedReport: null,    // set while viewing a report loaded from disk
  dataSource: 'fetch', // fetch | upload | live
  liveInventory: null,
  liveCluster: '',
  selectedLiveNodes: new Set(),
  fetchCatalog: null,
  fetchCluster: '',
  fetchJobId: null,
};

const OP_COLORS = {
  read: '#73bf69',
  write: '#5794f2',
  'batch read': '#ff9830',
  'batch write': '#b877d9',
  'batch index': '#fade2a',
  'batch udf': '#ff7383',
  udf: '#8ab8ff',
  query: '#f2cc0c',
  'read touch': '#56a64b',
  'replica write': '#c0d8ff',
  're-replicate': '#e0b400',
  proxy: '#ffb357',
  info: '#8e9095',
  other: '#9fa7b3',
};
const THRESHOLD_COLORS = ['#73bf69', '#fade2a', '#ff9830', '#f2495c', '#b877d9', '#5794f2', '#ff7383', '#8ab8ff'];

const $ = (sel) => document.querySelector(sel);
const el = (tag, attrs = {}, children = []) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (v === true) node.setAttribute(k, '');
    else if (v !== false && v != null) node.setAttribute(k, v);
  }
  for (const child of [].concat(children)) if (child) node.append(child);
  return node;
};

const fmtBytes = (n) => {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(i && v < 10 ? 1 : 0)} ${units[i]}`;
};
const fmtCount = (n) => {
  if (n == null) return '-';
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}G`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return n.toFixed(abs < 10 && !Number.isInteger(n) ? 1 : 0);
};
const fmtPct = (n) => (n == null ? '-' : `${n.toFixed(2)}%`);
const pad2 = (n) => String(n).padStart(2, '0');
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// ---------------------------------------------------------------------------
// display timezone
//
// Aerospike writes its logs in GMT, so every timestamp the backend hands back --
// epochs, "YYYY-MM-DD HH:MM:SS" stamps, and stamps embedded in headline prose --
// is GMT. That is shifted into the viewer's chosen zone exactly once, here, and
// the window fields are converted back before they are sent to the backend.
// ---------------------------------------------------------------------------

const offsetLabel = (m) => `${m < 0 ? '-' : '+'}${pad2(Math.floor(Math.abs(m) / 60))}:${pad2(Math.abs(m) % 60)}`;
const browserOffset = -new Date().getTimezoneOffset();
const TZ_CHOICES = {
  ist: { minutes: 330, label: 'IST' },
  local: { minutes: browserOffset, label: `UTC${offsetLabel(browserOffset)}` },
  gmt: { minutes: 0, label: 'GMT' },
};
let TZ = TZ_CHOICES.ist;

// A Date whose UTC fields read as the display zone's wall clock.
const shifted = (t) => new Date((t + TZ.minutes * 60) * 1000);
const stampToEpoch = (stamp) => Date.parse(`${stamp.replace(' ', 'T')}Z`) / 1000;

function fmtTime(t, withDate) {
  const d = shifted(t);
  const clock = `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}:${pad2(d.getUTCSeconds())}`;
  return withDate ? `${MONTHS[d.getUTCMonth()]} ${pad2(d.getUTCDate())} ${clock}` : clock;
}

// "YYYY-MM-DD HH:MM:SS" in the display zone.
const fmtFull = (t) => shifted(t).toISOString().slice(0, 19).replace('T', ' ');

// Convert one GMT stamp from the backend. Unparseable text is passed through so a
// format change upstream shows up as-is rather than as "NaN".
function fmtStamp(gmt) {
  if (!gmt) return '';
  const t = stampToEpoch(gmt);
  return Number.isNaN(t) ? gmt : fmtFull(t);
}

// Headline and reason strings are composed server-side with GMT stamps in them.
const GMT_STAMP = /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/g;
const localize = (text) => (text == null ? text : String(text)
  .replace(GMT_STAMP, (m) => fmtStamp(m))
  .replace(/\bGMT\b/g, TZ.label));

const toInput = (gmt) => (gmt ? fmtStamp(gmt).replace(' ', 'T') : '');
const epochToInput = (t) => fmtFull(t).replace(' ', 'T');
// The window fields hold display-zone text; the backend and the log only know GMT.
const inputEpoch = (value) => (value
  ? Date.parse(`${value.replace(' ', 'T')}Z`) / 1000 - TZ.minutes * 60
  : null);
function inputToGmt(value) {
  const t = inputEpoch(value);
  return t == null || Number.isNaN(t) ? '' : new Date(t * 1000).toISOString().slice(0, 19).replace('T', ' ');
}

// ---------------------------------------------------------------------------
// Application version and health
// ---------------------------------------------------------------------------

fetch('/api/health').then((r) => r.json()).then((info) => {
  const node = $('#toolStatus');
  const version = info.ui_version ? `Version ${info.ui_version}` : 'Version unavailable';
  node.textContent = version;
  const warnings = [];
  if (info.fetch_enabled === false) warnings.push('Cluster fetch is not configured.');
  if (!info.tool_exists) warnings.push('Latency analysis tool is unavailable.');
  const stale = info.stale_sources || [];
  if (stale.length) {
    // The browser always gets fresh static files, so a reloaded page can sit in front
    // of a server still running the code it was started with.
    node.textContent = `${version} · Restart required`;
    warnings.push(`${stale.join(' and ')} changed on disk. Restart the server to load the changes.`);
  }
  node.classList.toggle('bad', !!stale.length || !info.tool_exists);
  node.title = [version, ...warnings].join('\n');
}).catch(() => { $('#toolStatus').textContent = 'Version unavailable'; });

// ---------------------------------------------------------------------------
// Ansible fetch (cluster + date)
// ---------------------------------------------------------------------------

function fetchClusterMeta() {
  return state.fetchCatalog?.clusters?.find((c) => c.id === state.fetchCluster);
}

async function loadFetchClusters() {
  $('#runStatus').textContent = 'loading fetch catalog…';
  try {
    const res = await fetch('/api/fetch/clusters');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    state.fetchCatalog = data;
    const sel = $('#fetchCluster');
    sel.textContent = '';
    const clusters = data.clusters || [];
    if (!clusters.length) {
      state.fetchCluster = '';
      $('#runStatus').textContent = 'no clusters in fetch_clusters.json';
      return;
    }
    if (!clusters.some((c) => c.id === state.fetchCluster)) state.fetchCluster = clusters[0].id;
    clusters.forEach((c) => {
      const label = c.label && c.label !== c.id ? `${c.label} (${c.id})` : c.id;
      const opt = el('option', { value: c.id, text: label });
      if (c.id === state.fetchCluster) opt.selected = true;
      sel.append(opt);
    });
    renderFetchClusterInfo();
    $('#runStatus').textContent = `fetch catalog · ${clusters.length} cluster(s) · ${data.retention_days || 30}d retention`;
  } catch (err) {
    $('#runStatus').textContent = `fetch catalog: ${err.message}`;
  }
}

function renderFetchClusterInfo() {
  const c = fetchClusterMeta();
  if (!c) {
    $('#clusterInfo').textContent = 'fetch — configure fetch_clusters.json on server';
    return;
  }
  const nodes = c.node_count != null ? `${c.node_count} node(s)` : 'Ansible inventory';
  $('#clusterInfo').textContent = `fetch · ${c.id} · ${nodes} · pick date → Fetch logs`;
}

function applySessionPayload(data) {
  state.session = data.session;
  state.files = data.files || [];
  state.errorsKey = null;
  state.selectedFiles.clear();
  state.selectedHists.clear();
  (data.files || []).forEach((f) => state.selectedFiles.add(f.id));
  renderCluster(data.cluster || {});
  renderSkipped(data.skipped);
  renderLogs();
  $('#logsPanel').hidden = false;
  showTab();
  if (data.upload_hint) {
    const row = el('div', { class: 'dim small', text: data.upload_hint });
    $('#uploadLog').append(row);
  }
}

async function pollFetchJob(jobId, pollPath) {
  const maxTries = 120;
  for (let i = 0; i < maxTries; i += 1) {
    await new Promise((r) => setTimeout(r, 5000));
    const res = await fetch(pollPath || `/api/fetch/jobs/${encodeURIComponent(jobId)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    const status = data.status || '';
    $('#runStatus').textContent = `fetch ${status}… (${((i + 1) * 5)}s) — Ansible collecting logs`;
    if (status === 'completed' && data.files?.length) {
      applySessionPayload(data);
      const lat = data.latency_logs ?? 0;
      $('#runStatus').textContent = `fetch done · ${lat} latency log(s) · job ${jobId.slice(0, 8)}`;
      $('#clusterInfo').textContent = `fetched · ${state.fetchCluster} · ${$('#fetchDate').value}`;
      return;
    }
    if (status === 'failed') {
      throw new Error(data.error || data.ansible?.detail || 'fetch failed');
    }
    if (status === 'completed' && !(data.files || []).length) {
      throw new Error('fetch completed but no logs were ingested');
    }
  }
  throw new Error('fetch timed out — check /tmp/aero_lens_<job>.log on Ansible host');
}

async function startClusterFetch() {
  const cluster = state.fetchCluster || $('#fetchCluster').value;
  const date = $('#fetchDate').value;
  if (!cluster) { $('#runStatus').textContent = 'select a cluster'; return; }
  if (!date) { $('#runStatus').textContent = 'pick an incident date'; return; }

  $('#fetchLogsBtn').disabled = true;
  $('#dropzone').hidden = true;
  $('#uploadLog').textContent = '';
  $('#runStatus').textContent = 'starting Ansible fetch…';
  try {
    const res = await fetch('/api/fetch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cluster, date }),
    });
    const data = await res.json();
    if (res.status === 200 && data.files) {
      applySessionPayload(data);
      $('#runStatus').textContent = 'fetch complete';
      return;
    }
    if (!res.ok && res.status !== 202) throw new Error(data.error || res.statusText);
    if (!data.job_id) throw new Error('no job_id from server');
    state.fetchJobId = data.job_id;
    state.session = data.session;
    await pollFetchJob(data.job_id, data.poll);
  } catch (err) {
    $('#runStatus').textContent = `fetch failed: ${err.message}`;
  } finally {
    $('#fetchLogsBtn').disabled = false;
  }
}

$('#fetchCluster').addEventListener('change', () => {
  state.fetchCluster = $('#fetchCluster').value;
  renderFetchClusterInfo();
});

$('#fetchLogsBtn').addEventListener('click', () => startClusterFetch());

(function initFetchDate() {
  const today = new Date();
  const y = today.getFullYear();
  const m = pad2(today.getMonth() + 1);
  const d = pad2(today.getDate());
  if ($('#fetchDate') && !$('#fetchDate').value) $('#fetchDate').value = `${y}-${m}-${d}`;
}());

// ---------------------------------------------------------------------------
// live ingest (forwarded logs on the server)
// ---------------------------------------------------------------------------

function liveClusterEntry() {
  return (state.liveInventory?.clusters || []).find((c) => c.cluster === state.liveCluster);
}

function syncDataSourceUI() {
  const live = state.dataSource === 'live';
  const fetch = state.dataSource === 'fetch';
  $('#fetchControls').hidden = !fetch;
  $('#uploadControls').hidden = fetch || live;
  $('#liveControls').hidden = !live;
  $('#dropzone').hidden = fetch || live || (!state.files.length && !state.payload && !state.errors);
  renderLogs();
  if (live) renderLiveClusterInfo();
  else if (fetch) renderFetchClusterInfo();
  else if (!state.files.length && !state.cluster) $('#clusterInfo').textContent = 'no logs loaded';
  showTab();
}

async function fetchLiveInventory() {
  $('#runStatus').textContent = 'loading ingest inventory…';
  try {
    const res = await fetch('/api/ingest/inventory');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    state.liveInventory = data;
    const names = data.clusters.map((c) => c.cluster);
    if (!names.length) {
      state.liveCluster = '';
      state.selectedLiveNodes.clear();
      $('#liveCluster').textContent = '';
      $('#clusterInfo').textContent = 'live ingest — no data yet (start a forwarder)';
      renderLogs();
      $('#runStatus').textContent = `store: ${data.dir}`;
      return;
    }
    if (!names.includes(state.liveCluster)) state.liveCluster = names[0];
    const sel = $('#liveCluster');
    sel.textContent = '';
    names.forEach((name) => {
      const opt = el('option', { value: name, text: name });
      if (name === state.liveCluster) opt.selected = true;
      sel.append(opt);
    });
    const centry = liveClusterEntry();
    centry.nodes.forEach((n) => {
      if (!state.selectedLiveNodes.size) state.selectedLiveNodes.add(n.node);
    });
    renderLiveClusterInfo();
    renderLogs();
    $('#runStatus').textContent = `${data.clusters.length} cluster(s) · store ${data.dir}`;
  } catch (err) {
    $('#runStatus').textContent = `inventory failed: ${err.message}`;
  }
}

function renderLiveClusterInfo() {
  const c = liveClusterEntry();
  if (!c) {
    $('#clusterInfo').textContent = 'live ingest — no clusters in store';
    return;
  }
  $('#clusterInfo').textContent = `live · ${c.cluster} · ${c.node_count} forwarded node(s) · `
    + `${state.selectedLiveNodes.size} selected for analysis`;
}

$('#dataSource').addEventListener('change', () => {
  state.dataSource = $('#dataSource').value;
  syncDataSourceUI();
  if (state.dataSource === 'live') fetchLiveInventory();
  if (state.dataSource === 'fetch') loadFetchClusters();
});

$('#refreshLiveBtn').addEventListener('click', () => fetchLiveInventory());

$('#liveCluster').addEventListener('change', () => {
  state.liveCluster = $('#liveCluster').value;
  state.selectedLiveNodes.clear();
  liveClusterEntry()?.nodes.forEach((n) => state.selectedLiveNodes.add(n.node));
  renderLiveClusterInfo();
  renderLogs();
});

function renderLiveLogs() {
  const grid = $('#logsGrid');
  grid.textContent = '';
  $('#logsCount').textContent = String(state.selectedLiveNodes.size);
  $('#skippedList').textContent = state.liveInventory?.dir
    ? `Ingest store: ${state.liveInventory.dir} — select nodes, set a time window, then Run.`
    : '';
  const c = liveClusterEntry();
  if (!c) {
    grid.append(el('div', { class: 'empty', text: 'No forwarded logs yet. Deploy forwarder/forwarder.py on each Aerospike node.' }));
    return;
  }
  c.nodes.forEach((node) => {
    const box = el('input', { type: 'checkbox' });
    box.checked = state.selectedLiveNodes.has(node.node);
    box.addEventListener('change', () => {
      if (box.checked) state.selectedLiveNodes.add(node.node);
      else state.selectedLiveNodes.delete(node.node);
      renderLiveClusterInfo();
      renderLogs();
    });
    const identity = el('div', { class: 'log-id' }, [
      el('div', { text: node.node }),
      el('div', { text: node.hostname && node.hostname !== node.node ? node.hostname : '—' }),
      el('div', { text: `${fmtBytes(node.bytes)} ingested` }),
      el('div', { text: node.first_log
        ? `${fmtStamp(node.first_log)} → ${fmtStamp(node.last_log)} ${TZ.label}` : 'no timestamps yet' }),
    ]);
    grid.append(el('div', { class: 'log-row' }, [box, identity]));
  });
}

function liveSelectionReady() {
  return state.dataSource === 'live' && state.liveCluster && state.selectedLiveNodes.size;
}

// ---------------------------------------------------------------------------
// saved reports (count shown on load; listing only reads the small meta files)
//
// A report is the finished analysis, not the logs, so reopening one costs a single
// fetch instead of an upload and a re-parse. The server keeps them on disk; the only
// thing a saved report cannot do is re-run, because its session is long gone.
// ---------------------------------------------------------------------------

$('#saveBtn').addEventListener('click', async () => {
  if (!state.payload && !state.errors) return;
  const suggested = `${state.cluster || 'aerospike'}-${(state.errors || state.payload).window?.start || ''}`
    .replace(/[: ]/g, '').slice(0, 40);
  const name = window.prompt('Name this report', suggested);
  if (name === null) return;
  $('#saveBtn').disabled = true;
  $('#runStatus').textContent = 'saving…';
  try {
    const body = {
      name,
      cluster: state.cluster || null,
      latency: state.payload,
      errors: state.errors,
      logs: state.files.map((f) => ({ label: f.name || f.label, node: f.node, size: f.size })),
      nodes: (state.errors?.nodes || []).map((n) => n.node),
    };
    const res = await fetch('/api/reports', { method: 'POST', body: JSON.stringify(body) });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    $('#runStatus').textContent = `saved to ${data.saved.path}`;
    await loadReports();
  } catch (err) {
    $('#runStatus').textContent = `save failed: ${err.message}`;
  } finally {
    $('#saveBtn').disabled = false;
  }
});

$('#reportsBtn').addEventListener('click', async () => {
  const panel = $('#reportsPanel');
  panel.hidden = !panel.hidden;
  if (!panel.hidden) await loadReports();
});
$('#closeReports').addEventListener('click', () => { $('#reportsPanel').hidden = true; });

async function loadReports() {
  try {
    const data = await (await fetch('/api/reports')).json();
    state.reports = data.reports || [];
    $('#reportsDir').textContent = data.dir || '';
    $('#reportsCount').textContent = String(state.reports.length);
    renderReports();
  } catch (err) {
    $('#reportsList').textContent = `could not list saved reports: ${err.message}`;
  }
}

function renderReports() {
  const box = $('#reportsList');
  box.textContent = '';
  if (!state.reports.length) {
    box.append(el('div', { class: 'empty',
      text: 'Nothing saved yet. Run an analysis, then press Save.' }));
    return;
  }
  state.reports.forEach((report) => {
    const has = [report.has_latency && 'latency', report.has_errors && 'errors']
      .filter(Boolean).join(' + ');
    const win = report.window || {};
    const open = el('button', { class: 'btn sm', text: 'Open' });
    const del = el('button', { class: 'btn sm', text: 'Delete' });
    open.addEventListener('click', () => openReport(report.id));
    del.addEventListener('click', async () => {
      if (!window.confirm(`Delete "${report.name}"?`)) return;
      await fetch('/api/reports/delete', { method: 'POST', body: JSON.stringify({ id: report.id }) });
      await loadReports();
    });
    box.append(el('div', { class: 'erow report-row' }, [
      el('div', { class: 'row-head' }, [
        el('span', { class: 'row-title', text: report.name }),
        el('span', { class: 'dim small', text: report.cluster || 'cluster unknown' }),
        el('span', { class: 'badge info', text: has || 'empty' }),
        el('span', { class: 'spacer' }),
        el('span', { class: 'dim small',
          text: win.start ? `${fmtStamp(win.start)} → ${fmtStamp(win.end)} ${TZ.label}` : '' }),
        el('span', { class: 'dim small', text: `saved ${fmtStamp(report.saved_at)}` }),
        el('span', { class: 'dim small', text: fmtBytes(report.bytes || 0) }),
        open, del,
      ]),
    ]));
  });
}

async function openReport(id) {
  $('#runStatus').textContent = 'opening saved report…';
  try {
    const report = await (await fetch(`/api/reports/${encodeURIComponent(id)}`)).json();
    state.payload = report.latency || null;
    state.errors = report.errors || null;
    state.errorsKey = null;
    state.savedReport = report;
    state.range = null;
    state.cluster = report.cluster;
    $('#reportsPanel').hidden = true;
    $('#logsPanel').hidden = true;
    $('#dropzone').hidden = true;
    $('#clusterInfo').textContent = `${report.cluster || 'saved report'} · ${report.name}`;
    $('#savedBanner').hidden = false;
    $('#savedBannerText').textContent = `Viewing the saved report "${report.name}" from `
      + `${fmtStamp(report.saved_at)}. The original logs are not loaded, so Run is disabled.`;
    // Land on whichever tab this report actually contains.
    if (!state.payload && state.errors) state.tab = 'errors';
    if (!state.errors && state.payload) state.tab = 'latency';
    syncTabButtons();
    if (state.payload) renderDashboard(state.payload);
    if (state.errors) renderErrors(state.errors);
    showTab();
    $('#runStatus').textContent = `saved report · ${report.name}`;
  } catch (err) {
    $('#runStatus').textContent = `could not open report: ${err.message}`;
  }
}

loadReports().catch(() => {});

$('#exitSaved').addEventListener('click', () => {
  state.savedReport = null;
  $('#savedBanner').hidden = true;
  $('#clusterInfo').textContent = state.cluster || 'no logs loaded';
  showTab();
});

// ---------------------------------------------------------------------------
// upload
// ---------------------------------------------------------------------------

$('#uploadBtn').addEventListener('click', () => {
  $('#dropzone').hidden = false;
  $('#fileInput').click();
});
$('#fileInput').addEventListener('change', () => { uploadAll([...$('#fileInput').files]); $('#fileInput').value = ''; });
['dragenter', 'dragover'].forEach((ev) => document.addEventListener(ev, (e) => {
  e.preventDefault(); $('#dropzone').hidden = false; $('#dropzone').classList.add('drag');
}));
['dragleave', 'drop'].forEach((ev) => document.addEventListener(ev, (e) => {
  e.preventDefault(); $('#dropzone').classList.remove('drag');
}));
document.addEventListener('drop', (e) => uploadAll([...e.dataTransfer.files]));

async function uploadAll(files) {
  for (const file of files) {
    const row = el('div', { text: `uploading ${file.name} (${fmtBytes(file.size)})…` });
    $('#uploadLog').append(row);
    try {
      const headers = { 'X-Filename': file.name, 'Content-Type': 'application/octet-stream' };
      if (state.session) headers['X-Session'] = state.session;
      const res = await fetch('/api/upload', { method: 'POST', headers, body: file });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || res.statusText);
      data.added?.forEach((id) => state.selectedFiles.add(id));
      applySessionPayload(data);
      const lat = data.latency_logs ?? data.added?.length ?? 0;
      const errOnly = data.error_only_logs ?? 0;
      let summary = `${file.name}: ${lat} latency log(s)`;
      if (errOnly) summary += `, ${errOnly} error-only`;
      summary += ` in ${data.scan_seconds}s`;
      row.textContent = summary;
      if (data.upload_hint) {
        row.append(el('div', { class: 'dim small', text: data.upload_hint }));
      }
    } catch (err) {
      row.className = 'err';
      row.textContent = `${file.name}: ${err.message}`;
    }
  }
}

function renderCluster(cluster) {
  const bits = [];
  state.cluster = cluster.name || null;
  // A fresh upload replaces whatever saved report was on screen.
  state.savedReport = null;
  $('#savedBanner').hidden = true;
  if (cluster.name) bits.push(`cluster ${cluster.name}`);
  if (cluster.size) bits.push(`${cluster.size} nodes`);
  bits.push(`${cluster.logs} log${cluster.logs === 1 ? '' : 's'} loaded`);
  if (cluster.known_nodes) bits.push(`${cluster.known_nodes} node IPs from collectinfo`);
  $('#clusterInfo').textContent = bits.join(' · ');
}

function renderSkipped(skipped) {
  if (!skipped || !skipped.length) return;
  $('#skippedList').textContent = `Ignored: ${skipped.slice(0, 8).map((s) => `${s.file} (${s.reason})`).join('; ')}`;
}

// ---------------------------------------------------------------------------
// logs panel
// ---------------------------------------------------------------------------

$('#logsBtn').addEventListener('click', () => { $('#logsPanel').hidden = !$('#logsPanel').hidden; });
$('#closeLogs').addEventListener('click', () => { $('#logsPanel').hidden = true; });
$('#selectAllLogs').addEventListener('click', () => {
  if (state.dataSource === 'live') {
    liveClusterEntry()?.nodes.forEach((n) => state.selectedLiveNodes.add(n.node));
    renderLiveClusterInfo();
  } else state.files.forEach((f) => state.selectedFiles.add(f.id));
  renderLogs();
});

$('#selectCovering').addEventListener('click', () => {
  if (!requestedWindow()) {
    $('#runStatus').textContent = 'set a custom range first';
    return;
  }
  if (state.dataSource === 'live') {
    liveClusterEntry()?.nodes.forEach((n) => {
      const file = { first_iso: n.first_log, last_iso: n.last_log };
      if (coversWindow(file)) state.selectedLiveNodes.add(n.node);
      else state.selectedLiveNodes.delete(n.node);
    });
    renderLiveClusterInfo();
  } else {
    state.files.forEach((f) => {
      if (coversWindow(f)) state.selectedFiles.add(f.id); else state.selectedFiles.delete(f.id);
    });
  }
  renderLogs();
});

// Always GMT, whatever zone the fields are displaying.
function requestedWindow() {
  if ($('#windowMode').value !== 'range') return null;
  const from = inputToGmt($('#fromDt').value), to = inputToGmt($('#toDt').value);
  return from || to ? { from, to } : null;
}

// String compare is safe here: both sides are "YYYY-MM-DD HH:MM:SS".
function coversWindow(file) {
  const window = requestedWindow();
  if (!window || !file.first_iso) return true;
  if (window.from && file.last_iso < window.from) return false;
  if (window.to && file.first_iso > window.to) return false;
  return true;
}
async function clearSession() {
  if (state.session) {
    try {
      const res = await fetch('/api/reset', { method: 'POST', body: JSON.stringify({ session: state.session }) });
      if (!res.ok) throw new Error(`server returned ${res.status}`);
    } catch (err) {
      $('#runStatus').textContent = `Could not clear logs: ${err.message}`;
      return;
    }
  }
  Object.assign(state, { session: null, files: [], payload: null, range: null,
    errors: null, errorsKey: null, cluster: null, savedReport: null });
  state.selectedFiles.clear(); state.selectedHists.clear();
  state.selectedLiveNodes.clear();
  charts.clear();
  brushingChart = null;
  tip.style.display = 'none';
  $('#fileInput').value = '';
  $('#windowMode').value = 'full';
  $('#rangeFields').hidden = true;
  $('#fromDt').value = ''; $('#toDt').value = '';
  showLocalWindow();
  $('#uploadLog').textContent = ''; $('#skippedList').textContent = '';
  $('#logsPanel').hidden = true;
  $('#savedBanner').hidden = true;
  $('#errBadge').hidden = true;
  $('#clusterInfo').textContent = 'no logs loaded';
  $('#runStatus').textContent = 'Logs cleared. Upload logs to start a new analysis.';
  syncDataSourceUI();
  if (state.dataSource === 'upload') $('#dropzone').hidden = false;
}
$('#clearSession').addEventListener('click', clearSession);
$('#clearUploadsBtn').addEventListener('click', clearSession);

function renderLogs() {
  if (state.dataSource === 'live') {
    renderLiveLogs();
    return;
  }
  const grid = $('#logsGrid');
  grid.textContent = '';
  $('#logsCount').textContent = String(state.selectedFiles.size);
  for (const file of state.files) {
    const box = el('input', { type: 'checkbox' });
    box.checked = state.selectedFiles.has(file.id);
    box.addEventListener('change', () => {
      if (box.checked) state.selectedFiles.add(file.id); else state.selectedFiles.delete(file.id);
      renderLogs();
    });

    const node = file.node || {};
    const identity = el('div', { class: 'log-id' }, [
      el('div', { text: node.ip ? `${node.ip}${node.port ? `:${node.port}` : ''} (${node.ip_source})` : 'IP not in log — add the collectinfo bundle' }),
      el('div', { text: [node.node_id ? `node-id ${node.node_id}` : null, node.build ? `build ${node.build}` : null].filter(Boolean).join(' · ') || '—' }),
      el('div', { text: file.errors_only
        ? `${fmtBytes(file.size)} · errors only (no histogram dumps)`
        : `${fmtBytes(file.size)} · ${file.dumps.toLocaleString()} dumps` }),
      el('div', { text: file.first_iso ? `${fmtStamp(file.first_iso)} → ${fmtStamp(file.last_iso)} ${TZ.label}` : '' }),
    ]);

    const chips = el('div', { class: 'chips' }, file.histograms.map((h) => {
      const chip = el('div', {
        class: `chip${state.selectedHists.has(h.name) ? ' on' : ''}`,
        text: h.name,
        title: `${h.dumps} dumps · unit ${h.unit || 'msec'}`,
      });
      chip.addEventListener('click', () => {
        if (state.selectedHists.has(h.name)) state.selectedHists.delete(h.name);
        else state.selectedHists.add(h.name);
        renderLogs();
      });
      return chip;
    }));

    const outside = !coversWindow(file);
    grid.append(el('div', { class: `log-card${box.checked ? ' on' : ''}${outside ? ' outside' : ''}` }, [
      el('div', { class: 'log-top' }, [
        box,
        el('div', { class: 'log-name', text: node.name || file.label }),
        file.errors_only ? el('span', { class: 'badge warning', text: 'errors only' }) : null,
        outside ? el('span', { class: 'badge idle', text: 'outside window' }) : null,
      ]),
      identity, chips,
    ]));
  }
  prefillWindow();
}

// ---------------------------------------------------------------------------
// window controls
// ---------------------------------------------------------------------------

$('#windowMode').addEventListener('change', () => {
  $('#rangeFields').hidden = $('#windowMode').value !== 'range';
  if (state.files.length) renderLogs();
  showLocalWindow();
});
['#fromDt', '#toDt'].forEach((sel) => $(sel).addEventListener('change', () => {
  if (state.files.length) renderLogs();
  showLocalWindow();
}));

// The fields read in the display zone, so show the GMT equivalent too -- that is what
// you need when grepping the raw log alongside the page.
function showLocalWindow() {
  const box = $('#windowLocal');
  if (!TZ.minutes) { box.textContent = ''; return; }
  const from = inputToGmt($('#fromDt').value), to = inputToGmt($('#toDt').value);
  box.textContent = from || to ? `= ${from || '…'} → ${to || '…'} GMT in the log` : '';
}

$('#tzMode').addEventListener('change', () => applyTz($('#tzMode').value));

// Re-renders from the payloads already in hand: switching zones must never re-run a scan.
function applyTz(key) {
  const from = inputEpoch($('#fromDt').value);
  const to = inputEpoch($('#toDt').value);
  TZ = TZ_CHOICES[key] || TZ_CHOICES.ist;
  try { localStorage.setItem('aslat.tz', key); } catch (err) { /* private mode */ }
  $('#tzMode').value = key;
  $('#windowTz').textContent = TZ.label;
  if (from != null && !Number.isNaN(from)) $('#fromDt').value = epochToInput(from);
  if (to != null && !Number.isNaN(to)) $('#toDt').value = epochToInput(to);
  showLocalWindow();
  if (state.files.length) renderLogs();
  if (state.payload) renderDashboard(state.payload);
  if (state.errors) renderErrors(state.errors);
  if (state.range) updateZoomInfo();
}

(function initTz() {
  let saved = null;
  try { saved = localStorage.getItem('aslat.tz'); } catch (err) { /* private mode */ }
  TZ = TZ_CHOICES[saved] || TZ_CHOICES.ist;
  $('#tzMode').value = TZ_CHOICES[saved] ? saved : 'ist';
  $('#windowTz').textContent = TZ.label;
}());

function detectedRange() {
  if (state.dataSource === 'live') {
    const nodes = liveClusterEntry()?.nodes.filter((n) => state.selectedLiveNodes.has(n.node)) || [];
    const starts = nodes.map((n) => n.first_log).filter(Boolean).sort();
    const ends = nodes.map((n) => n.last_log).filter(Boolean).sort();
    return { start: starts[0], end: ends[ends.length - 1] };
  }
  const selected = state.files.filter((f) => state.selectedFiles.has(f.id));
  const starts = selected.map((f) => f.first_iso).filter(Boolean).sort();
  const ends = selected.map((f) => f.last_iso).filter(Boolean).sort();
  return { start: starts[0], end: ends[ends.length - 1] };
}

function prefillWindow() {
  const { start, end } = detectedRange();
  if (start && !$('#fromDt').value) $('#fromDt').value = toInput(start);
  if (end && !$('#toDt').value) $('#toDt').value = toInput(end);
  showLocalWindow();
}

document.querySelectorAll('.btn[data-last]').forEach((btn) => btn.addEventListener('click', () => {
  const { end } = detectedRange();
  if (!end) return;
  const endT = Date.parse(`${end.replace(' ', 'T')}Z`) / 1000;
  $('#fromDt').value = epochToInput(endT - Number(btn.dataset.last));
  $('#toDt').value = epochToInput(endT);
  showLocalWindow();
}));

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

$('#runBtn').addEventListener('click', () => (state.tab === 'errors' ? runErrors(true) : runLatency()));

async function runLatency() {
  if (state.dataSource === 'live') {
    if (!liveSelectionReady()) { $('#runStatus').textContent = 'select cluster and at least one node'; return; }
  } else if (!state.selectedFiles.size) { $('#runStatus').textContent = 'select at least one log'; return; }
  else {
    const withHist = state.files.filter((f) => state.selectedFiles.has(f.id) && f.histograms?.length);
    if (!withHist.length) {
      $('#runStatus').textContent = 'selected logs have no histogram dumps — use Errors tab or upload aerospike.log';
      return;
    }
  }
  const range = requestedWindow();
  const body = state.dataSource === 'live' ? {
    cluster: state.liveCluster,
    nodes: [...state.selectedLiveNodes],
    histograms: [...state.selectedHists],
    slice_seconds: Number($('#sliceSeconds').value),
    num_buckets: 6,
    every_nth: 1,
    alert_pct: Number($('#alertPct').value) || 1,
    alert_threshold: Number($('#alertThreshold').value) || 8,
    min_ops_rate: Number($('#minOpsRate').value) || 0,
    from: range ? range.from : null,
    to: range ? range.to : null,
  } : {
    session: state.session,
    files: [...state.selectedFiles],
    histograms: [...state.selectedHists],
    slice_seconds: Number($('#sliceSeconds').value),
    num_buckets: 6,
    every_nth: 1,
    alert_pct: Number($('#alertPct').value) || 1,
    alert_threshold: Number($('#alertThreshold').value) || 8,
    min_ops_rate: Number($('#minOpsRate').value) || 0,
    from: range ? range.from : null,
    to: range ? range.to : null,
  };

  $('#runBtn').disabled = true;
  const started = Date.now();
  const ticker = setInterval(() => {
    $('#runStatus').textContent = `running… ${((Date.now() - started) / 1000).toFixed(0)}s`;
  }, 500);
  $('#runStatus').textContent = 'running…';
  try {
    const url = state.dataSource === 'live' ? '/api/ingest/analyze' : '/api/analyze';
    const res = await fetch(url, { method: 'POST', body: JSON.stringify(body) });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    state.payload = data;
    state.range = null;
    $('#logsPanel').hidden = true;
    $('#dropzone').hidden = true;
    renderDashboard(data);
    $('#runStatus').textContent = `${data.totals.with_data}/${data.totals.runs} histograms · ${data.elapsed_seconds}s`;
  } catch (err) {
    $('#runStatus').textContent = `failed: ${err.message}`;
  } finally {
    clearInterval(ticker);
    $('#runBtn').disabled = false;
  }
}

$('#downloadJson').addEventListener('click', () => {
  if (!state.payload) return;
  download(JSON.stringify(state.payload), 'asloglatency-analysis.json', 'application/json');
});

function download(text, filename, type) {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = el('a', { href: url, download: filename });
  document.body.append(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// dashboard
// ---------------------------------------------------------------------------

function renderDashboard(data) {
  chartOwner = 'latency';
  clearCharts('latency');
  showTab();              // panels must be laid out before any canvas is sized
  renderTiles(data);
  renderHeadline(data);
  renderOverviewCharts(data);
  renderOpTable(data);
  renderNodeTable(data);
  renderDetailList(data);
  updateZoomInfo();
}

function renderTiles(data) {
  const cum = data.cumulative;
  const ops = cum.by_op;
  const parents = new Set((data.aggregate && data.aggregate.parent_ops) || []);
  const totalOps = ops.reduce((acc, r) => acc + (parents.has(r.op) ? 0 : r.total_ops), 0);
  const peak = data.aggregate ? Math.max(...data.aggregate.total) : 0;
  const worst = ops.reduce((acc, r) => (r.worst_pct > (acc?.worst_pct ?? -1) ? r : acc), null);
  const breaches = ops.reduce((acc, r) => acc + r.breaches, 0);
  const span = data.aggregate ? data.aggregate.t[data.aggregate.t.length - 1] - data.aggregate.t[0] : 0;
  const hours = span / 3600;
  const worstClass = !worst ? '' : worst.severity === 'critical' ? 'crit' : worst.severity === 'warning' ? 'warn' : 'ok';

  const tiles = [
    ['operations', fmtCount(totalOps), `${hours >= 1 ? `${hours.toFixed(1)} h` : `${Math.round(span / 60)} min`} of log${parents.size ? `, excl. ${[...parents].join(', ')}` : ''}`],
    ['peak throughput', `${fmtCount(peak)}/s`, `busiest slice, ${cum.by_node.length} node(s) combined`],
    ['worst latency', worst ? fmtPct(worst.worst_pct) : '-',
      worst ? `${worst.op} > ${worst.threshold} ${worst.unit} on ${worst.worst_node || 'unknown'}` : ''],
    ['breaching slices', String(breaches),
      `above ${data.options.alert_pct}% over ${data.options.alert_threshold} ms, min ${fmtCount(data.options.min_ops_rate)} ops/sec`],
    ['nodes analyzed', String(cum.by_node.length), data.cluster.size ? `of ${data.cluster.size} in cluster` : ''],
  ];
  const box = $('#statTiles');
  box.textContent = '';
  tiles.forEach(([k, v, s], i) => {
    box.append(el('div', { class: `tile${i === 2 ? ` ${worstClass}` : ''}` }, [
      el('div', { class: 'k', text: k }),
      el('div', { class: 'v', text: v }),
      el('div', { class: 's', text: s }),
    ]));
  });
}

function renderHeadline(data) {
  const box = $('#headlinePanel');
  box.textContent = '';
  const lines = [...data.cumulative.headline];
  if (data.cumulative.absent_count) {
    lines.push(`${data.cumulative.absent_count} histogram/node combination(s) exist in the log but had no traffic in this window`);
  }
  if (data.cumulative.distributions.length) {
    lines.push(`${data.cumulative.distributions.length} count/size distribution histogram(s) excluded from latency alerting`);
  }
  lines.slice(0, 4).forEach((text) => box.append(el('div', { class: 'headline', text: localize(text) })));
}

function renderOverviewCharts(data) {
  const aggregate = data.aggregate;
  $('#opsChart').textContent = '';
  $('#latChart').textContent = '';
  if (!aggregate) {
    $('#opsChart').append(el('div', { class: 'empty', text: 'No latency histogram produced data for this window.' }));
    return;
  }
  $('#opsPanelHint').textContent = `summed across ${data.cumulative.by_node.length} node(s), ${aggregate.step}s slices`;
  $('#latTitle').textContent = `Latency by operation — % of operations slower than ${aggregate.threshold} ${aggregate.unit}`;

  const opsSeries = aggregate.order.map((op) => ({
    name: op, color: OP_COLORS[op] || OP_COLORS.other, t: aggregate.t, v: aggregate.ops[op],
  }));
  opsSeries.push({
    name: aggregate.parent_ops.length ? `all operations (excl. ${aggregate.parent_ops.join(', ')})` : 'all operations',
    color: '#d8d9da', t: aggregate.t, v: aggregate.total,
  });
  $('#opsChart').append(new Chart({ series: opsSeries, unit: 'ops/sec', height: 220 }).el);

  const latSeries = aggregate.order.map((op) => ({
    name: op, color: OP_COLORS[op] || OP_COLORS.other, t: aggregate.t, v: aggregate.pct[op],
  }));
  $('#latChart').append(new Chart({ series: latSeries, unit: '%', height: 220 }).el);
}

function severityBadge(severity) {
  const cls = ['ok', 'warning', 'critical'].includes(severity) ? severity : 'info';
  return el('span', { class: `badge ${cls}`, text: severity });
}

function buildTable(headers, rows) {
  const table = el('table');
  table.append(el('thead', {}, el('tr', {}, headers.map((h) => el('th', { class: 'num', text: h })))));
  // Cells may be ready-made <td> nodes or plain values; wrap the latter.
  const cell = (value) => (value instanceof Node ? value : el('td', { text: value == null ? '-' : String(value) }));
  table.append(el('tbody', {}, rows.map((cells) => el('tr', {}, cells.map(cell)))));
  return el('div', { class: 'table-wrap' }, table);
}

function renderOpTable(data) {
  const box = $('#opTable');
  box.textContent = '';
  const rows = data.cumulative.by_op.map((r) => [
    el('td', { text: r.op }),
    el('td', { text: r.namespaces.join(', ') || '—' }),
    el('td', { class: 'num', text: String(r.nodes) }),
    el('td', { class: 'num', text: fmtCount(r.total_ops) }),
    el('td', { class: 'num', text: fmtCount(r.avg_ops) }),
    el('td', { class: 'num', text: fmtCount(r.peak_ops) }),
    el('td', { class: `num${r.breaches ? ' breach' : ''}`, text: fmtPct(r.worst_pct) }),
    el('td', { text: r.worst_node || '—' }),
    el('td', { class: 'num', text: String(r.breaches) }),
    el('td', {}, severityBadge(r.severity)),
  ]);
  if (!rows.length) { box.append(el('div', { class: 'empty', text: 'no latency histograms in this window' })); return; }
  const first = data.cumulative.by_op[0];
  box.append(buildTable(
    ['operation', 'namespace', 'nodes', 'total ops', 'avg ops/sec', 'peak ops/sec',
      `worst % > ${first.threshold} ${first.unit}`, 'worst node', 'breaches', 'state'], rows));
}

function renderNodeTable(data) {
  const box = $('#nodeTable');
  box.textContent = '';
  const rows = data.cumulative.by_node.map((n) => [
    el('td', { text: n.name }),
    el('td', { text: n.node_id || '—' }),
    el('td', { text: n.build || '—' }),
    el('td', { class: 'num', text: String(n.files) }),
    el('td', { class: 'num', text: String(n.histograms) }),
    el('td', { class: 'num', text: fmtCount(n.total_ops) }),
    el('td', { class: 'num', text: fmtCount(n.avg_ops) }),
    el('td', { class: 'num', text: fmtCount(n.peak_ops) }),
    el('td', { text: n.worst_op || '—' }),
    el('td', { class: `num${n.breaches ? ' breach' : ''}`, text: fmtPct(n.worst_pct) }),
    el('td', { class: 'num', text: String(n.breaches) }),
    el('td', {}, severityBadge(n.severity)),
  ]);
  if (!rows.length) { box.append(el('div', { class: 'empty', text: 'no nodes with data' })); return; }
  box.append(buildTable(
    ['node', 'node-id', 'build', 'logs', 'histograms', 'total ops', 'avg ops/sec', 'peak ops/sec',
      'worst operation', 'worst %', 'breaches', 'state'], rows));
}

// ---------------------------------------------------------------------------
// per histogram detail (built lazily when expanded)
// ---------------------------------------------------------------------------

function renderDetailList(data) {
  const box = $('#detailList');
  box.textContent = '';
  for (const result of data.results) {
    const summary = result.summary;
    const body = el('div', { class: 'detail-body' });
    body.hidden = true;
    const caret = el('span', { class: 'caret', text: '▸' });
    const head = el('div', { class: 'detail-head' }, [
      caret,
      el('span', { class: 'hist', text: result.histogram }),
      el('span', { class: 'node', text: result.node.ip || result.node.name }),
      severityBadge(summary.status === 'ok' ? summary.severity : summary.status),
      el('span', { class: 'spacer' }),
      el('span', { class: 'dim small', text: summary.status === 'ok'
        ? `${summary.slice_count} slices · avg ${fmtCount((summary.avg || {}).ops)}/s · worst ${fmtPct((summary.worst || {}).pct)} > ${(summary.worst || {}).threshold} ${summary.unit}`
        : localize(summary.reason || result.messages[0] || 'no data') }),
    ]);
    let built = false;
    head.addEventListener('click', () => {
      body.hidden = !body.hidden;
      caret.textContent = body.hidden ? '▸' : '▾';
      if (!body.hidden && !built) { built = true; buildDetail(body, result, data); }
    });
    box.append(el('div', { class: 'detail-row' }, [head, body]));
  }
}

function buildDetail(body, result, data) {
  chartOwner = 'latency';
  if (result.summary.status === 'ok') {
    const series = result.thresholds.map((threshold, i) => ({
      name: `> ${threshold} ${result.unit}`,
      color: THRESHOLD_COLORS[i % THRESHOLD_COLORS.length],
      t: result.series.t,
      v: result.series.overs[i],
    }));
    body.append(new Chart({ series, unit: '%', height: 190 }).el);
    body.append(new Chart({
      series: [{ name: 'ops/sec', color: OP_COLORS[result.op] || OP_COLORS.other, t: result.series.t, v: result.series.ops }],
      unit: 'ops/sec', height: 150,
    }).el);
    body.append(slicesTable(result));
  }
  const actions = el('div', { class: 'detail-actions' }, [
    el('button', { class: 'btn sm', text: 'CSV' }),
  ]);
  actions.firstChild.addEventListener('click', () => downloadCsv(result));
  // asloglatency's raw output stays in the session, which a saved report has outlived.
  actions.append(state.savedReport
    ? el('span', { class: 'dim small',
      text: `raw output (${result.raw_line_count} lines) is not kept in saved reports` })
    : el('a', { class: 'btn sm', href: `/api/raw?session=${data.session}&run=${result.id}`,
      target: '_blank', text: `raw output (${result.raw_line_count} lines)` }));
  body.append(actions);
  body.append(el('div', { class: 'cmd', text: result.command }));
}

function slicesTable(result) {
  const summary = result.summary;
  const primary = summary.primary_index ?? 0;
  const alert = summary.kind === 'latency' ? summary.alert_pct : Infinity;
  const headers = [`slice end (${TZ.label})`, ...result.thresholds.map((t) => `% > ${t} ${result.unit}`), 'ops/sec'];
  const times = result.series.t;
  const limit = 300;
  const wrap = el('div', { class: 'table-wrap' });
  const table = el('table');
  table.append(el('thead', {}, el('tr', {}, headers.map((h) => el('th', { class: 'num', text: h })))));
  const tbody = el('tbody');
  let shown = 0;
  const addRows = (count) => {
    const end = Math.min(times.length, shown + count);
    for (let i = shown; i < end; i += 1) {
      const cells = [el('td', { text: fmtTime(times[i], true) })];
      result.series.overs.forEach((column, c) => cells.push(el('td', {
        class: `num${c === primary && column[i] > alert ? ' breach' : ''}`,
        text: column[i].toFixed(2),
      })));
      cells.push(el('td', { class: 'num', text: result.series.ops[i].toFixed(1) }));
      tbody.append(el('tr', {}, cells));
    }
    shown = end;
  };
  addRows(limit);
  table.append(tbody);
  wrap.append(table);
  if (times.length > shown) {
    const more = el('button', { class: 'btn sm', text: `show ${Math.min(limit, times.length - shown)} more of ${times.length} rows` });
    more.addEventListener('click', () => {
      addRows(limit);
      if (shown >= times.length) more.remove();
      else more.textContent = `show ${Math.min(limit, times.length - shown)} more of ${times.length} rows`;
    });
    return el('div', {}, [wrap, more]);
  }
  return wrap;
}

function downloadCsv(result) {
  // Column name carries the zone, and each value carries its offset, so a spreadsheet
  // cannot silently reinterpret the export.
  const header = [`timestamp_${TZ.label.toLowerCase()}`,
    ...result.thresholds.map((t) => `pct_over_${t}${result.unit}`), 'ops_per_sec'];
  const lines = [header.join(',')];
  result.series.t.forEach((t, i) => {
    lines.push([`${fmtFull(t)}${TZ.minutes ? offsetLabel(TZ.minutes) : 'Z'}`,
      ...result.series.overs.map((c) => c[i].toFixed(2)),
      result.series.ops[i].toFixed(1)].join(','));
  });
  download(lines.join('\n'), `${result.node.name}-${result.histogram.replace(/[^\w.-]/g, '_')}.csv`, 'text/csv');
}

// ---------------------------------------------------------------------------
// shared zoom
// ---------------------------------------------------------------------------

const charts = new Set();
// One document-level listener for all charts: panels are rebuilt on every run, and
// per-chart window listeners would accumulate and keep dead charts alive.
let brushingChart = null;
// Each tab rebuilds its own panels; charts are tagged so one tab's re-render does not
// drop the other tab's charts out of the shared zoom.
let chartOwner = 'latency';

function clearCharts(owner) {
  charts.forEach((chart) => { if (chart.owner === owner) charts.delete(chart); });
}

window.addEventListener('mouseup', () => {
  const chart = brushingChart;
  brushingChart = null;
  if (!chart || !chart.brush) return;
  const { x0, x1 } = chart.brush;
  chart.brush = null;
  if (Math.abs(x1 - x0) > 6) {
    setGlobalRange([Math.floor(chart.xToTime(Math.min(x0, x1))), Math.ceil(chart.xToTime(Math.max(x0, x1)))]);
  } else {
    chart.draw();
  }
});

function setGlobalRange(range) {
  state.range = range;
  charts.forEach((chart) => chart.setRange(range));
  updateZoomInfo();
}

function updateZoomInfo() {
  const info = $('#zoomInfo');
  if (!state.range) { info.hidden = true; return; }
  info.hidden = false;
  const [t0, t1] = state.range;
  const span = t1 - t0;
  const length = span >= 3600 ? `${(span / 3600).toFixed(1)} h` : `${Math.round(span / 60)} min`;
  $('#zoomLabel').textContent = `${fmtTime(t0, true)} → ${fmtTime(t1, false)} ${TZ.label} (${length})`;
}

$('#zoomReset').addEventListener('click', () => setGlobalRange(null));

// Zooming only magnifies the slices already computed; re-running asloglatency on
// the zoomed window gives real extra resolution.
$('#zoomRerun').addEventListener('click', () => {
  if (!state.range) return;
  const [t0, t1] = state.range;
  $('#windowMode').value = 'range';
  $('#rangeFields').hidden = false;
  $('#fromDt').value = epochToInput(t0);
  $('#toDt').value = epochToInput(t1);
  showLocalWindow();
  const span = t1 - t0;
  const slice = span <= 1800 ? '10' : span <= 7200 ? '30' : span <= 43200 ? '60' : '300';
  if ([...$('#sliceSeconds').options].some((o) => o.value === slice)) $('#sliceSeconds').value = slice;
  $('#runBtn').click();
});

// ---------------------------------------------------------------------------
// tabs
// ---------------------------------------------------------------------------

document.querySelectorAll('.tab').forEach((btn) => btn.addEventListener('click', () => {
  state.tab = btn.dataset.tab;
  syncTabButtons();
  showTab();
  const errors = state.tab === 'errors';
  if (errors && !state.savedReport
    && (state.files.length || liveSelectionReady())
    && state.errorsKey !== errorsKey()) {
    runErrors(false);
  }
}));

function syncTabButtons() {
  document.querySelectorAll('.tab').forEach((b) => b.classList.toggle('on', b.dataset.tab === state.tab));
}

function showTab() {
  const errors = state.tab === 'errors';
  const live = state.dataSource === 'live';
  $('#latencyOpts').hidden = errors;
  $('#errorOpts').hidden = !errors;
  $('#dash').hidden = errors || !state.payload;
  $('#errdash').hidden = !errors || !state.errors;
  $('#zoomInfo').hidden = !state.range;
  // Saving needs a finished analysis; re-running needs the session that produced it.
  $('#saveBtn').disabled = !state.payload && !state.errors;
  $('#runBtn').disabled = !!state.savedReport;
  $('#clearUploadsBtn').hidden = state.dataSource !== 'upload'
    || !(state.files.length || state.payload || state.errors);

  const ready = errors ? state.errors : state.payload;
  const hint = $('#tabHint');
  const fetch = state.dataSource === 'fetch';
  hint.hidden = !!ready || (!state.savedReport && (live ? !liveClusterEntry() : !state.files.length && !fetch));
  if (state.savedReport && !ready) {
    hint.textContent = `This saved report does not include ${errors ? 'an error scan' : 'a latency analysis'}.`;
    charts.forEach((chart) => { if (chart.owner === state.tab) chart.draw(); });
    return;
  }
  if (fetch && !state.files.length) {
    hint.textContent = 'Choose cluster and incident date, then Fetch logs. When logs appear, press Run.';
  } else {
    hint.textContent = errors
      ? (live ? 'Press Run to scan forwarded logs for errors.' : 'Press Run to scan the selected logs for errors.')
      : (live ? 'Press Run to analyse latency from the ingest store.' : 'Press Run to analyse latency for the selected logs.');
  }

  // A canvas sized while its tab was display:none has zero width and draw() skips it,
  // so charts are redrawn whenever their tab comes back into view.
  charts.forEach((chart) => { if (chart.owner === state.tab) chart.draw(); });
}

// ---------------------------------------------------------------------------
// errors view
// ---------------------------------------------------------------------------

const SEVERITY_COLORS = { critical: '#f2495c', error: '#ff9830', warning: '#fade2a', info: '#8e9095' };
const ROUTINE_EVENT_KINDS = new Set(['new cluster key', 'applied cluster size', 'fill-migrations released']);

function errorsKey() {
  const range = requestedWindow();
  if (state.dataSource === 'live') {
    return JSON.stringify([
      'live',
      state.liveCluster,
      [...state.selectedLiveNodes].sort(),
      range ? range.from : null,
      range ? range.to : null,
    ]);
  }
  return JSON.stringify([
    [...state.selectedFiles].sort(),
    range ? range.from : null,
    range ? range.to : null,
  ]);
}

async function runErrors(force) {
  if (state.dataSource === 'live') {
    if (!liveSelectionReady()) { $('#runStatus').textContent = 'select cluster and at least one node'; return; }
  } else if (!state.selectedFiles.size) { $('#runStatus').textContent = 'select at least one log'; return; }
  const key = errorsKey();
  if (!force && state.errorsKey === key && state.errors) return;
  const range = requestedWindow();
  const body = state.dataSource === 'live' ? {
    cluster: state.liveCluster,
    nodes: [...state.selectedLiveNodes],
    from: range ? range.from : null,
    to: range ? range.to : null,
  } : {
    session: state.session,
    files: [...state.selectedFiles],
    from: range ? range.from : null,
    to: range ? range.to : null,
  };

  $('#runBtn').disabled = true;
  const started = Date.now();
  const ticker = setInterval(() => {
    $('#runStatus').textContent = `scanning for errors… ${((Date.now() - started) / 1000).toFixed(0)}s`;
  }, 500);
  $('#runStatus').textContent = 'scanning for errors…';
  try {
    const url = state.dataSource === 'live' ? '/api/ingest/errors' : '/api/errors';
    const res = await fetch(url, { method: 'POST', body: JSON.stringify(body) });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    state.errors = data;
    state.errorsKey = key;
    $('#logsPanel').hidden = true;
    $('#dropzone').hidden = true;
    renderErrors(data);
    const counts = data.severity_totals || {};
    $('#runStatus').textContent = `${fmtCount(data.scanned.lines)} lines · ${data.scanned.seconds}s`
      + `${data.cached ? ' (cached)' : ''}`;
    const serious = (counts.CRITICAL || 0) + (counts.ERROR || 0);
    const badge = $('#errBadge');
    badge.hidden = false;
    badge.textContent = fmtCount(serious || counts.WARNING || 0);
    badge.className = `count${serious ? ' crit' : counts.WARNING ? ' warn' : ''}`;
  } catch (err) {
    $('#runStatus').textContent = `failed: ${err.message}`;
  } finally {
    clearInterval(ticker);
    $('#runBtn').disabled = false;
  }
}

// Report series arrive as dense arrays over a fixed axis; charts want timestamps.
function axisTimes(axis) {
  const out = new Array(axis.buckets);
  for (let i = 0; i < axis.buckets; i += 1) out[i] = axis.t0 + i * axis.step;
  return out;
}

function renderErrors(data) {
  chartOwner = 'errors';
  clearCharts('errors');
  showTab();
  renderErrTiles(data);
  renderErrHeadline(data);
  renderErrConditions(data);
  renderErrTimeline(data);
  renderErrEvents(data);
  renderErrCounters(data);
  renderErrGauges(data);
  renderErrNodes(data);
  renderErrCluster(data);
}

function renderErrTiles(data) {
  const counts = data.severity_totals || {};
  const serious = data.events.filter((e) => e.impact === 'critical' || e.impact === 'error');
  const failures = data.counters.filter((c) => c.class === 'failure');
  const failed = failures.reduce((acc, c) => acc + c.total, 0);
  const worstNode = data.nodes[0];
  const membership = data.cluster_events.filter((e) => !ROUTINE_EVENT_KINDS.has(e.kind)).length;
  const span = data.axis.step * data.axis.buckets / 3600;

  const cond = (data.conditions || []).filter((c) => c.severity === 'critical');
  const tiles = [
    ['operational alerts', String((data.conditions || []).length),
      cond.length ? cond[0].title : (data.conditions || []).length ? 'see conditions panel' : 'none detected'],
    ['serious problems', String(serious.length),
      serious.length ? serious[0].title || serious[0].shape : 'nothing client-visible logged'],
    ['warning lines', fmtCount(counts.WARNING || 0),
      `${data.events.length} distinct message(s) across ${data.scanned.logs} log(s)`],
    ['failed transactions', fmtCount(failed),
      failures.length ? `${failures.length} error/timeout counter(s) moved` : 'no error counter moved'],
    ['worst node', worstNode ? worstNode.node : '-',
      worstNode ? `${fmtCount(worstNode.failures)} failed txns, ${fmtCount(worstNode.warning + worstNode.critical)} log lines` : ''],
    ['cluster events', String(membership),
      data.restarts ? `${data.restarts} node restart(s) detected` : `${span.toFixed(1)} h of log`],
  ];
  const box = $('#errTiles');
  box.textContent = '';
  tiles.forEach(([k, v, s], i) => {
    const bad = (i === 0 && serious.length) || (i === 2 && failed);
    box.append(el('div', { class: `tile${bad ? ' crit' : ''}` }, [
      el('div', { class: 'k', text: k }),
      el('div', { class: 'v', text: v }),
      el('div', { class: 's', text: s }),
    ]));
  });
}

function renderErrConditions(data) {
  const box = $('#errConditions');
  box.textContent = '';
  const rows = data.conditions || [];
  if (!rows.length) {
    box.append(el('div', { class: 'empty',
      text: 'No proto-fd-max refusals, storage queue overload, stop-writes, active migrations, or orphan-node signals in this window.' }));
    return;
  }
  rows.forEach((cond) => {
    const head = el('div', { class: 'row-head' }, [
      el('span', { class: `badge ${cond.severity}`, text: cond.severity }),
      el('span', { class: 'row-title', text: cond.title }),
      el('span', { class: 'spacer' }),
      cond.total ? el('span', { class: 'num-cell', text: `${fmtCount(cond.total)}×` }) : null,
      cond.peak_per_bucket ? el('span', { class: 'dim small',
        text: `peak ${fmtCount(cond.peak_per_bucket)}/bucket` }) : null,
      cond.fd_in_use_peak != null && cond.fd_limit != null
        ? el('span', { class: 'dim small',
          text: `fds ${fmtCount(cond.fd_in_use_peak)} / ${fmtCount(cond.fd_limit)}` }) : null,
      cond.first ? el('span', { class: 'dim small',
        text: `${fmtStamp(cond.first)} → ${fmtStamp(cond.last)} ${TZ.label}` }) : null,
      el('span', { class: 'dim small', text: `${cond.node_count} node(s)` }),
    ]);
    const body = el('div', { class: 'row-body', hidden: true });
    const row = el('div', { class: 'erow' }, [head, body]);
    head.addEventListener('click', () => {
      body.hidden = !body.hidden;
      row.classList.toggle('open', !body.hidden);
      if (!body.hasChildNodes()) {
        chartOwner = 'errors';
        if (cond.meaning) body.append(el('div', { class: 'note', text: cond.meaning }));
        if (cond.action) body.append(el('div', { class: 'note action', text: `What to do: ${cond.action}` }));
        if (cond.peak_at) {
          body.append(el('div', { class: 'dim small',
            text: `Worst bucket ends ${fmtStamp(cond.peak_at)} ${TZ.label} — align this with app startup / deploy times.` }));
        }
        if (cond.namespaces && cond.namespaces.length) {
          body.append(el('div', { class: 'dim small',
            text: `Namespaces: ${cond.namespaces.map((n) => `${n.namespace} (${fmtCount(n.count)})`).join(', ')}` }));
        }
        if (cond.nodes && cond.nodes.length) {
          const headers = cond.id === 'migrations'
            ? ['node', 'namespaces with active migration']
            : ['node', 'count / detail', 'proto-fd-max'];
          const tableRows = cond.nodes.map((n) => {
            if (cond.id === 'migrations') {
              return [n.node, (n.namespaces || []).join(', ') || '-'];
            }
            return [n.node, fmtCount(n.count || 0), n.proto_fd_max != null ? fmtCount(n.proto_fd_max) : '-'];
          });
          body.append(buildTable(headers, tableRows));
        }
        if (cond.series && cond.series.some((v) => v)) {
          body.append(new Chart({
            series: [{ name: cond.title, color: cond.severity === 'critical' ? '#f2495c' : '#ff9830',
              t: axisTimes(data.axis), v: cond.series }],
            unit: 'per bucket', height: 160,
          }).el);
        }
      }
    });
    box.append(row);
  });
}

function renderErrHeadline(data) {
  const box = $('#errHeadline');
  box.textContent = '';
  box.append(el('div', { class: 'dim small',
    text: `${data.window.mode}: ${fmtStamp(data.window.start)} → ${fmtStamp(data.window.end)} ${TZ.label}, `
      + `${data.scanned.logs} log(s), ${fmtCount(data.scanned.lines)} lines in ${data.scanned.seconds}s` }));
  data.headline.forEach((text) => box.append(el('div', { class: 'headline', text: localize(text) })));
}

function renderErrTimeline(data) {
  const box = $('#errTimeline');
  box.textContent = '';
  const t = axisTimes(data.axis);
  const series = [];
  Object.entries(data.severity_series || {}).forEach(([name, values]) => {
    series.push({ name: `${name.toLowerCase()} log lines`, color: SEVERITY_COLORS[name.toLowerCase()] || '#fade2a', t, v: values });
  });
  if (data.failure_series.some((v) => v)) {
    series.push({ name: 'failed transactions', color: '#f2495c', t, v: data.failure_series });
  }
  if ((data.connection_refusal_series || []).some((v) => v)) {
    series.push({ name: 'connection refusals (proto-fd-max)', color: '#ff9830', t,
      v: data.connection_refusal_series });
  }
  if ((data.storage_queue_series || []).some((v) => v)) {
    series.push({ name: 'storage queue too deep (device overload)', color: '#b877d9', t,
      v: data.storage_queue_series });
  }
  $('#errTimelineHint').textContent = `${data.axis.step}s buckets`;
  if (!series.length) {
    box.append(el('div', { class: 'empty', text: 'No warnings, errors or failed transactions in this window.' }));
    return;
  }
  box.append(new Chart({ series, unit: 'per bucket', height: 200 }).el);
}

function renderErrEvents(data) {
  const box = $('#errEvents');
  box.textContent = '';
  if (!data.events.length) {
    box.append(el('div', { class: 'empty', text: 'No WARNING, ERROR or CRITICAL lines in this window.' }));
    return;
  }
  const priority = new Set([
    'Device write queue full (AS_ERR_DEVICE_OVERLOAD 18)',
    'Client connection limit reached',
    'Writes stopped or high-water mark breached',
    'Node evicted from the cluster',
    'Client transactions blocked (orphan node)',
  ]);
  const sorted = [...data.events].sort((a, b) => {
    const pa = priority.has(a.title) ? 0 : 1;
    const pb = priority.has(b.title) ? 0 : 1;
    if (pa !== pb) return pa - pb;
    return (b.count || 0) - (a.count || 0);
  });
  sorted.forEach((event) => {
    const head = el('div', { class: 'row-head' }, [
      el('span', { class: `badge ${event.impact}`, text: event.severity.toLowerCase() }),
      el('span', { class: 'row-title', text: event.title || event.shape }),
      el('span', { class: 'dim small', text: `${event.context} · ${event.site}` }),
      el('span', { class: 'spacer' }),
      el('span', { class: 'num-cell', text: `${fmtCount(event.count)}×` }),
      el('span', { class: 'dim small', text: `${event.node_count} node(s)` }),
      el('span', { class: 'dim small', text: `${fmtStamp(event.first)} → ${fmtStamp(event.last)}` }),
    ]);
    const body = el('div', { class: 'row-body', hidden: true });
    const row = el('div', { class: 'erow' }, [head, body]);
    head.addEventListener('click', () => {
      body.hidden = !body.hidden;
      row.classList.toggle('open', !body.hidden);
      if (!body.hasChildNodes()) buildEventDetail(body, event, data);
    });
    box.append(row);
  });
}

function buildEventDetail(body, event, data) {
  chartOwner = 'errors';
  if (event.meaning) body.append(el('div', { class: 'note', text: event.meaning }));
  if (event.action) body.append(el('div', { class: 'note action', text: `What to do: ${event.action}` }));
  if (event.collapsed) {
    body.append(el('div', { class: 'dim small',
      text: `The server collapsed repeats: ${fmtCount(event.collapsed)} of these `
        + `${fmtCount(event.count)} occurrences were reported as "(repeated:N)" rather than as separate lines.` }));
  }
  body.append(el('div', { class: 'kv' }, [
    el('span', { class: 'dim small', text: 'message pattern' }),
    el('code', { text: event.shape }),
  ]));
  body.append(el('div', { class: 'kv' }, [
    el('span', { class: 'dim small', text: 'first logged line' }),
    el('code', { text: event.example }),
  ]));
  body.append(buildTable(['node', 'occurrences'],
    event.nodes.map((n) => [n.node, fmtCount(n.count)])));
  if (event.series.some((v) => v)) {
    body.append(new Chart({
      series: [{ name: event.title || event.shape, color: SEVERITY_COLORS[event.impact] || '#fade2a',
        t: axisTimes(data.axis), v: event.series }],
      unit: 'per bucket', height: 140,
    }).el);
  }
}

function renderErrCounters(data) {
  const box = $('#errCounters');
  box.textContent = '';
  const rows = data.counters.filter((c) => c.total);
  if (!rows.length) {
    box.append(el('div', { class: 'empty',
      text: 'No failure, hot-key, record-too-big, query, retransmit or duplicate-resolution '
        + 'counter moved in this window.' }));
    return;
  }
  rows.forEach((counter) => {
    const label = `${counter.source} ${counter.op} ${counter.field.replace('_', ' ')}`;
    const head = el('div', { class: 'row-head' }, [
      el('span', { class: `badge ${counter.class === 'failure' ? 'critical' : 'info'}`,
        text: counter.class === 'failure' ? 'failure' : 'watch' }),
      el('span', { class: 'row-title', text: label }),
      el('span', { class: 'dim small', text: counter.namespace === '-' ? 'node level' : `{${counter.namespace}}` }),
      el('span', { class: 'spacer' }),
      el('span', { class: 'num-cell', text: fmtCount(counter.total) }),
      el('span', { class: 'dim small',
        text: counter.rate_pct == null ? 'no success counter to compare' : `${counter.rate_pct}% of attempts` }),
      el('span', { class: 'dim small', text: `${counter.node_count} node(s)` }),
    ]);
    const body = el('div', { class: 'row-body', hidden: true });
    const row = el('div', { class: 'erow' }, [head, body]);
    head.addEventListener('click', () => {
      body.hidden = !body.hidden;
      row.classList.toggle('open', !body.hidden);
      if (!body.hasChildNodes()) {
        chartOwner = 'errors';
        body.append(el('div', { class: 'note', text: counterHelp(counter) }));
        body.append(buildTable(['node', counter.field.replace('_', ' ')],
          counter.nodes.map((n) => [n.node, fmtCount(n.count)])));
        body.append(new Chart({
          series: [{ name: label, color: counter.class === 'failure' ? '#f2495c' : '#5794f2',
            t: axisTimes(data.axis), v: counter.series }],
          unit: 'per bucket', height: 150,
        }).el);
      }
    });
    box.append(row);
  });
}

// Explanations for the counters that need more than their own name, keyed by source.
const SOURCE_HELP = {
  'special-errors': (counter) => ({
    'key-busy': 'Hot key: more concurrent operations piled onto one record than '
      + 'transaction-pending-limit allows, so the server rejected the extras with '
      + 'AS_ERR_KEY_BUSY (14). Only operations that use the rw-hash are counted, so reads '
      + 'appear here only while duplicate resolution is running.',
    'key-busy (xdr)': 'Same hot-key rejection, but for an XDR write waiting on a previous '
      + 'version to ship — AS_ERR_XDR_KEY_BUSY (32).',
    'record-too-big': 'A write exceeded max-record-size and was rejected with '
      + 'AS_ERR_RECORD_TOO_BIG (13). Counts client writes on the master only.',
    'lost-conflict': 'A client write lost to a conflicting XDR write under bin convergence '
      + '— AS_ERR_LOST_CONFLICT (28).',
    'lost-conflict (xdr)': 'An XDR write lost the same conflict resolution.',
  }[counter.op] || 'A namespace-level special error.'),
  retransmits: () => 'The server resent this message to another node because the previous '
    + 'attempt timed out. A retransmit does not necessarily fail the client request, but a '
    + 'rising rate means inter-node messages are being lost or are too slow — check the '
    + 'fabric and the receiving node before blaming the client.',
  'dup-res': (counter) => (counter.op === 'ask'
    ? 'Duplicate resolution requests this node sent to other nodes. Expected during '
      + 'migrations, when two copies of a record may disagree and the master must compare '
      + 'them before answering. Outside migrations this should be flat.'
    : 'Duplicate resolution requests this node answered for others, split by whether the '
      + 'record had to be read from storage. The read variety is the expensive one.'),
};

function counterHelp(counter) {
  const custom = SOURCE_HELP[counter.source];
  if (custom) return custom(counter);

  const where = {
    client: 'transactions sent straight from a client',
    'from-proxy': 'transactions another node proxied here',
    'batch-sub': 'the individual record operations inside a batch',
    'from-proxy-batch-sub': 'proxied batch sub-operations',
    'batch-index': 'the batch parent transactions themselves',
    'xdr-client': 'writes arriving from XDR',
    'xdr-from-proxy': 'proxied XDR writes',
    'pi-query': 'primary index query jobs (scans)',
    'si-query': 'secondary index query jobs',
    scan: 'scan jobs (the pre-6.0 name for primary index queries)',
  }[counter.source] || counter.source;
  const what = {
    error: 'failed with a server error, so the client saw a failure',
    timeout: 'ran out of time on the server before completing',
    abort: 'stopped before returning all their results — either cancelled, or given up on '
      + 'because the cluster changed mid-query',
    not_found: 'found no record, which is a normal outcome for reads and deletes, not an error',
    filtered_out: 'were skipped by a filter expression, which is a normal outcome',
  }[counter.field] || counter.field;
  const scope = counter.op === 'tsvc'
    ? ' These failed in the transaction service before any record work began.'
    : '';
  const kind = {
    'short-basic': ' Short queries are declared by the client and are not monitored.',
    'long-basic': ' Long queries are monitored and are not time bounded.',
    'udf-bg': ' Background UDF jobs.',
    'ops-bg': ' Background operations jobs.',
    aggr: ' Aggregation jobs.',
  }[counter.op] || '';
  return `Counts ${where} that ${what}.${scope}${kind}`;
}

function renderErrGauges(data) {
  const box = $('#errGauges');
  box.textContent = '';
  const t = axisTimes(data.axis);
  const moving = data.gauges.filter((g) => !g.flat);
  const flat = data.gauges.filter((g) => g.flat);

  moving.forEach((gauge) => {
    const limit = gauge.critical_at != null ? gauge.critical_at : gauge.warn_at;
    const series = [{ name: gauge.name, color: gauge.breaches ? '#f2495c' : '#5794f2', t, v: gauge.series }];
    if (limit != null) {
      series.push({ name: `limit ${limit}${gauge.unit ? ` ${gauge.unit}` : ''}`,
        color: '#f2495c', t: [t[0], t[t.length - 1]], v: [limit, limit] });
    }
    const worstValue = gauge.worse === 'high' ? gauge.max : gauge.min;
    box.append(el('div', { class: 'gauge' }, [
      el('div', { class: 'panel-head' }, [
        el('h3', { text: gauge.name }),
        el('span', { class: `dim small${gauge.breaches ? ' bad' : ''}`,
          text: `${gauge.worse === 'high' ? 'peak' : 'low'} ${fmtCount(worstValue)}${gauge.unit ? ` ${gauge.unit}` : ''}`
            + ` on ${gauge.worst_node}${gauge.breaches ? ` · ${gauge.breaches} bucket(s) past the limit` : ''}` }),
      ]),
      new Chart({ series, unit: gauge.unit, height: 150 }).el,
      gauge.help ? el('div', { class: 'dim small', text: gauge.help }) : null,
    ]));
  });

  if (flat.length) {
    box.append(el('div', { class: 'dim small',
      text: `Flat across the whole window (nothing to plot): ${flat.map((g) =>
        `${g.name} = ${g.min}${g.unit ? ` ${g.unit}` : ''}`).join(', ')}.` }));
  }
  if (!moving.length && !flat.length) {
    box.append(el('div', { class: 'empty', text: 'No ticker health lines in this window.' }));
  }
}

function renderErrNodes(data) {
  const box = $('#errNodes');
  box.textContent = '';
  if (!data.nodes.length) { box.append(el('div', { class: 'empty', text: 'no nodes resolved' })); return; }
  const rows = data.nodes.map((n) => [
    n.node,
    n.critical ? String(n.critical) : '-',
    fmtCount(n.warning),
    fmtCount(n.failures),
    n.cluster_sizes.length ? n.cluster_sizes.join(' → ') : '-',
    n.size_changes ? String(n.size_changes) : '-',
    n.restarts ? String(n.restarts) : '-',
    n.proto_fd_max ? fmtCount(n.proto_fd_max) : '-',
  ]);
  box.append(buildTable(
    ['node', 'critical', 'warnings', 'failed txns', 'cluster size seen', 'size changes', 'restarts', 'proto-fd-max'],
    rows));
}

function renderErrCluster(data) {
  const box = $('#errCluster');
  box.textContent = '';
  const hide = $('#hideRoutine').checked;
  const events = data.cluster_events.filter((e) => !hide || !ROUTINE_EVENT_KINDS.has(e.kind));
  if (!events.length) {
    box.append(el('div', { class: 'empty', text: 'No cluster membership or rebalance events in this window.' }));
    return;
  }
  box.append(buildTable([`time (${TZ.label})`, 'node', 'event', 'log line'],
    events.slice(0, 300).map((e) => [fmtStamp(e.iso), e.node, e.kind, e.text])));
  const extra = data.cluster_event_count - events.length;
  if (extra > 0) {
    box.append(el('div', { class: 'dim small',
      text: `${extra} more event(s) not shown${hide ? ' (routine chatter hidden)' : ''}.` }));
  }
}

$('#hideRoutine').addEventListener('change', () => { if (state.errors) renderErrCluster(state.errors); });

$('#downloadErrors').addEventListener('click', () => {
  if (!state.errors) return;
  download(JSON.stringify(state.errors), 'aerospike-errors.json', 'application/json');
});

// ---------------------------------------------------------------------------
// canvas time series chart with drag-to-zoom
// ---------------------------------------------------------------------------

const tip = el('div', { class: 'chart-tip' });
document.body.append(tip);

const TICK_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600,
  7200, 10800, 21600, 43200, 86400, 172800];

class Chart {
  constructor({ series, unit, height }) {
    this.series = series
      .filter((s) => s.t && s.t.length)
      .map((s) => {
        if (s.t.every((t) => t != null)) return s;
        const t = [], v = [];
        s.t.forEach((time, i) => { if (time != null) { t.push(time); v.push(s.v[i]); } });
        return { ...s, t, v };
      })
      .filter((s) => s.t.length);
    this.unit = unit;
    this.height = height || 200;
    this.hidden = new Set();
    this.range = state.range;
    this.pad = { l: 56, r: 12, t: 8, b: 22 };
    this.brush = null;

    this.canvas = el('canvas', { height: this.height });
    this.canvas.style.height = `${this.height}px`;
    this.legend = el('div', { class: 'legend' }, this.series.map((s) => {
      const item = el('span', {}, [el('i', { style: `background:${s.color}` }), el('span', { text: s.name })]);
      item.addEventListener('click', () => {
        if (this.hidden.has(s.name)) this.hidden.delete(s.name); else this.hidden.add(s.name);
        item.classList.toggle('off', this.hidden.has(s.name));
        this.draw();
      });
      return item;
    }));
    this.el = el('div', { class: 'chart' }, [this.canvas, this.legend]);
    this.owner = chartOwner;

    this.bind();
    charts.add(this);
    requestAnimationFrame(() => this.draw());
    if (!Chart.resizeHooked) {
      Chart.resizeHooked = true;
      let pending = null;
      window.addEventListener('resize', () => {
        if (pending) cancelAnimationFrame(pending);
        pending = requestAnimationFrame(() => charts.forEach((c) => c.draw()));
      });
    }
  }

  visibleSeries() { return this.series.filter((s) => !this.hidden.has(s.name)); }

  dataRange() {
    let t0 = Infinity, t1 = -Infinity;
    for (const s of this.series) {
      t0 = Math.min(t0, s.t[0]);
      t1 = Math.max(t1, s.t[s.t.length - 1]);
    }
    return t0 < t1 ? [t0, t1] : [t0, t0 + 60];
  }

  activeRange() {
    const [d0, d1] = this.dataRange();
    if (!this.range) return [d0, d1];
    return [Math.max(d0, this.range[0]), Math.min(d1, this.range[1])];
  }

  setRange(range) { this.range = range; this.draw(); }

  bind() {
    const canvas = this.canvas;
    canvas.addEventListener('mousedown', (event) => {
      if (event.button !== 0) return;
      this.brush = { x0: this.localX(event), x1: this.localX(event) };
      brushingChart = this;
      event.preventDefault();
    });
    canvas.addEventListener('mousemove', (event) => {
      if (this.brush) {
        this.brush.x1 = this.localX(event);
        this.draw();
        return;
      }
      this.showTip(event);
    });
    canvas.addEventListener('mouseleave', () => {
      tip.style.display = 'none';
      this.hover = null;
      this.draw();
    });
    canvas.addEventListener('dblclick', () => setGlobalRange(null));
  }

  localX(event) {
    return event.clientX - this.canvas.getBoundingClientRect().left;
  }

  plotWidth() { return this.canvas.clientWidth - this.pad.l - this.pad.r; }

  xToTime(x) {
    const [t0, t1] = this.activeRange();
    const ratio = Math.min(1, Math.max(0, (x - this.pad.l) / this.plotWidth()));
    return t0 + ratio * (t1 - t0);
  }

  timeToX(t) {
    const [t0, t1] = this.activeRange();
    return this.pad.l + ((t - t0) / (t1 - t0 || 1)) * this.plotWidth();
  }

  showTip(event) {
    const [t0, t1] = this.activeRange();
    const time = this.xToTime(this.localX(event));
    if (time < t0 || time > t1) { tip.style.display = 'none'; return; }
    const rows = [];
    for (const s of this.visibleSeries()) {
      const index = nearestIndex(s.t, time);
      if (index == null) continue;
      rows.push({ name: s.name, color: s.color, value: s.v[index], t: s.t[index] });
    }
    if (!rows.length) { tip.style.display = 'none'; return; }
    rows.sort((a, b) => b.value - a.value);
    this.hover = time;
    this.draw();

    const table = el('table', {}, rows.map((r) => el('tr', {}, [
      el('td', {}, el('i', { style: `display:inline-block;width:9px;height:2px;background:${r.color}` })),
      el('td', { text: r.name }),
      el('td', { class: 'v', text: this.unit === '%' ? fmtPct(r.value) : `${fmtCount(r.value)}/s` }),
    ])));
    tip.textContent = '';
    tip.append(el('div', { class: 't', text: `${fmtTime(rows[0].t, true)} ${TZ.label}` }), table);
    tip.style.display = 'block';
    const box = tip.getBoundingClientRect();
    tip.style.left = `${Math.min(event.clientX + 14, window.innerWidth - box.width - 10)}px`;
    tip.style.top = `${Math.min(event.clientY + 14, window.innerHeight - box.height - 10)}px`;
  }

  draw() {
    const canvas = this.canvas;
    const width = canvas.clientWidth;
    if (!width) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(this.height * dpr);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, this.height);

    const { l, r, t: top, b } = this.pad;
    const plotW = width - l - r;
    const plotH = this.height - top - b;
    const [t0, t1] = this.activeRange();

    // y scale from visible window only
    let maxY = 0;
    const slices = [];
    for (const s of this.visibleSeries()) {
      const i0 = lowerBound(s.t, t0);
      const i1 = Math.min(s.t.length - 1, lowerBound(s.t, t1) + 1);
      slices.push({ s, i0, i1 });
      for (let i = i0; i <= i1; i += 1) if (s.v[i] > maxY) maxY = s.v[i];
    }
    const yTop = niceCeil(maxY);
    const yToPx = (value) => top + plotH - (value / yTop) * plotH;

    ctx.font = '11px -apple-system, system-ui, sans-serif';
    ctx.textBaseline = 'middle';

    // horizontal grid
    ctx.strokeStyle = '#2c3235';
    ctx.fillStyle = '#8e9095';
    ctx.lineWidth = 1;
    ctx.textAlign = 'right';
    for (let i = 0; i <= 4; i += 1) {
      const value = (yTop * i) / 4;
      const y = Math.round(yToPx(value)) + 0.5;
      ctx.beginPath(); ctx.moveTo(l, y); ctx.lineTo(width - r, y); ctx.stroke();
      ctx.fillText(this.unit === '%' ? `${formatAxis(value)}%` : formatAxis(value), l - 7, y);
    }

    // time axis
    const span = t1 - t0;
    const step = TICK_STEPS.find((s) => span / s <= 7) || TICK_STEPS[TICK_STEPS.length - 1];
    ctx.textAlign = 'center';
    const firstTick = Math.ceil(t0 / step) * step;
    let lastDay = null;
    for (let t = firstTick; t <= t1; t += step) {
      const x = Math.round(this.timeToX(t)) + 0.5;
      ctx.strokeStyle = '#23272b';
      ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, top + plotH); ctx.stroke();
      const day = shifted(t).getUTCDate();
      const label = fmtTime(t, step >= 3600 || (lastDay != null && day !== lastDay));
      lastDay = day;
      ctx.fillStyle = '#8e9095';
      ctx.fillText(label, x, this.height - b / 2);
    }

    // Name the clock on every chart: the axis shows the log's own GMT timestamps,
    // not the viewer's timezone.
    ctx.textAlign = 'left';
    ctx.fillStyle = '#6e7278';
    ctx.fillText(TZ.label, 2, this.height - b / 2);

    // series, max-downsampled to one point per pixel so long windows stay fast
    for (const { s, i0, i1 } of slices) {
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 1.4;
      ctx.beginPath();
      let started = false;
      let pixel = -1;
      let pixelMax = -Infinity;
      let pixelTime = 0;
      const flush = () => {
        if (pixelMax === -Infinity) return;
        const x = this.timeToX(pixelTime);
        const y = yToPx(pixelMax);
        if (started) ctx.lineTo(x, y); else { ctx.moveTo(x, y); started = true; }
      };
      for (let i = i0; i <= i1; i += 1) {
        const x = Math.round(this.timeToX(s.t[i]));
        if (x !== pixel) { flush(); pixel = x; pixelMax = -Infinity; }
        if (s.v[i] > pixelMax) { pixelMax = s.v[i]; pixelTime = s.t[i]; }
      }
      flush();
      ctx.stroke();
    }

    // crosshair
    if (this.hover != null && !this.brush) {
      const x = Math.round(this.timeToX(this.hover)) + 0.5;
      ctx.strokeStyle = '#6e7681';
      ctx.setLineDash([3, 3]);
      ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, top + plotH); ctx.stroke();
      ctx.setLineDash([]);
    }

    // brush selection
    if (this.brush) {
      const x0 = Math.min(this.brush.x0, this.brush.x1);
      const x1 = Math.max(this.brush.x0, this.brush.x1);
      ctx.fillStyle = 'rgba(87,148,242,0.18)';
      ctx.fillRect(x0, top, x1 - x0, plotH);
      ctx.strokeStyle = '#5794f2';
      ctx.beginPath();
      ctx.moveTo(x0 + 0.5, top); ctx.lineTo(x0 + 0.5, top + plotH);
      ctx.moveTo(x1 + 0.5, top); ctx.lineTo(x1 + 0.5, top + plotH);
      ctx.stroke();
    }

    // frame
    ctx.strokeStyle = '#2c3235';
    ctx.beginPath();
    ctx.moveTo(l, top); ctx.lineTo(l, top + plotH); ctx.lineTo(width - r, top + plotH);
    ctx.stroke();
  }
}

function lowerBound(array, value) {
  let low = 0, high = array.length - 1;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (array[mid] < value) low = mid + 1; else high = mid;
  }
  return low;
}

function nearestIndex(array, value) {
  if (!array.length) return null;
  const index = lowerBound(array, value);
  if (index === 0) return 0;
  return Math.abs(array[index] - value) < Math.abs(array[index - 1] - value) ? index : index - 1;
}

function niceCeil(value) {
  if (!(value > 0)) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const factor of [1, 1.5, 2, 3, 4, 5, 7.5, 10]) {
    if (value <= factor * magnitude) return factor * magnitude;
  }
  return 10 * magnitude;
}

function formatAxis(value) {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
  if (value >= 1e3) return `${(value / 1e3).toFixed(value >= 1e4 ? 0 : 1)}k`;
  if (value >= 10) return value.toFixed(0);
  if (value >= 1) return value.toFixed(1);
  return value.toFixed(2);
}

// Initialize after chart state and all event handlers exist. syncDataSourceUI()
// calls showTab(), which reads charts even when no logs have been loaded yet.
state.dataSource = $('#dataSource').value;
syncDataSourceUI();
if (state.dataSource === 'fetch') loadFetchClusters();
else if (state.dataSource === 'live') fetchLiveInventory();
