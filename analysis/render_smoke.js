// Runs app.js against a minimal DOM stub and drives both tabs with real API
// payloads, so render bugs surface here instead of in the browser.
const fs = require('fs');
const path = require('path');

const WEBAPP = path.join(__dirname, '..', 'webapp');
const html = fs.readFileSync(path.join(WEBAPP, 'static', 'index.html'), 'utf8');
const ids = [...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]);
const tabButtons = [];

class Node {
  constructor(tag) {
    this.tagName = (tag || 'div').toUpperCase();
    this.children = [];
    this.attrs = {};
    this.listeners = {};
    this.classList = {
      set: new Set(),
      add: (c) => this.classList.set.add(c),
      remove: (c) => this.classList.set.delete(c),
      toggle: (c, on) => (on ? this.classList.set.add(c) : this.classList.set.delete(c)),
      contains: (c) => this.classList.set.has(c),
    };
    this.style = {};
    this.dataset = {};
    this._text = '';
    this.hidden = false;
    this.checked = false;
    this.value = '';
  }
  set textContent(v) { this._text = v; this.children = []; }
  get textContent() { return this._text; }
  set className(v) { this.attrs.class = v; }
  get className() { return this.attrs.class || ''; }
  setAttribute(k, v) {
    this.attrs[k] = v;
    if (k === 'hidden') this.hidden = true;
    if (k.startsWith('data-')) this.dataset[k.slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] = v;
  }
  getAttribute(k) { return this.attrs[k]; }
  append(...kids) {
    for (const kid of kids) this.children.push(kid instanceof Node ? kid : new Text(String(kid)));
  }
  appendChild(kid) { this.append(kid); }
  remove() {}
  addEventListener(ev, fn) { (this.listeners[ev] = this.listeners[ev] || []).push(fn); }
  fire(ev, arg) { (this.listeners[ev] || []).forEach((fn) => fn(arg || { preventDefault() {}, button: 0 })); }
  click() { this.clicks = (this.clicks || 0) + 1; this.fire('click'); }
  hasChildNodes() { return this.children.length > 0 || !!this._text; }
  get firstChild() { return this.children[0]; }
  querySelector() { return new Node('div'); }
  querySelectorAll() { return []; }
  getContext() { return ctx2d; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 900, height: 200 }; }
  // Walk for assertions.
  find(pred, out = []) {
    if (pred(this)) out.push(this);
    this.children.forEach((c) => c instanceof Node && c.find(pred, out));
    return out;
  }
  text() {
    return (this._text || '') + this.children.map((c) => (c instanceof Node ? c.text() : c.data)).join(' ');
  }
}
class Text { constructor(d) { this.data = d; } }
global.Node = Node;

const ctx2d = new Proxy({}, { get: () => () => {} });
const registry = new Map();
ids.forEach((id) => registry.set(id, new Node('div')));
registry.get('hideRoutine').checked = true;
registry.get('dataSource').value = 'fetch';
registry.get('windowMode').value = 'full';
registry.get('sliceSeconds').value = '10';
registry.get('alertPct').value = '1';
registry.get('alertThreshold').value = '8';
registry.get('minOpsRate').value = '50';

['tabLatency', 'tabErrors'].forEach((id, i) => {
  const node = registry.get(id);
  node.dataset.tab = i === 0 ? 'latency' : 'errors';
  node.classList.set.add(i === 0 ? 'on' : 'off');
  tabButtons.push(node);
});

global.document = {
  createElement: (tag) => new Node(tag),
  querySelector: (sel) => {
    if (sel.startsWith('#')) {
      const id = sel.slice(1);
      if (!registry.has(id)) throw new Error(`app.js wants #${id}, which is not in index.html`);
      return registry.get(id);
    }
    return new Node('div');
  },
  querySelectorAll: (sel) => (sel === '.tab' ? tabButtons : []),
  addEventListener() {},
  body: new Node('body'),
};
global.window = { addEventListener() {}, devicePixelRatio: 1 };
global.requestAnimationFrame = (fn) => fn();
global.cancelAnimationFrame = () => {};
global.setInterval = () => 0;
global.clearInterval = () => {};
global.fetch = async () => ({ ok: true, json: async () => ({}) });
global.URL = { createObjectURL: () => 'blob:', revokeObjectURL() {} };
global.Blob = class {};
const store = new Map();
global.localStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => store.set(k, v),
};

const source = fs.readFileSync(path.join(WEBAPP, 'static', 'app.js'), 'utf8');
// app.js is a plain script; eval it in this global so its top-level code runs.
const run = new Function(`${source}
return { state, renderErrors, renderDashboard, buildEventDetail, renderErrCluster, showTab,
         requestedWindow, fmtTime, fmtStamp, localize, applyTz, tz: () => TZ };`);
const api = run();

console.log('--- startup and upload controls ---');
if (!registry.get('uploadControls').hidden) throw new Error('upload controls visible in fetch mode');
registry.get('dataSource').value = 'upload';
registry.get('dataSource').fire('change');
if (registry.get('uploadControls').hidden || !registry.get('fetchControls').hidden) {
  throw new Error('switching to manual upload did not update the controls');
}
registry.get('uploadBtn').fire('click');
if (registry.get('fileInput').clicks !== 1) throw new Error('Upload logs did not open the file input');
if (registry.get('dropzone').hidden) throw new Error('upload feedback is hidden');
for (const id of ['runBtn', 'logsBtn', 'tabLatency', 'tabErrors']) {
  if (!registry.get(id).listeners.click?.length) throw new Error(`#${id} was not initialized`);
}
console.log('  default startup, source switching, upload picker, and button bindings passed');

// API fixtures are optional when checking only startup and upload controls.
if (process.argv[2]) {
const errorsReport = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const latency = process.argv[3] ? JSON.parse(fs.readFileSync(process.argv[3], 'utf8')) : null;

console.log('--- rendering errors tab ---');
api.state.tab = 'errors';
api.state.errors = errorsReport;
api.renderErrors(errorsReport);
for (const id of ['errTiles', 'errHeadline', 'errTimeline', 'errEvents', 'errCounters',
  'errGauges', 'errNodes', 'errCluster']) {
  const node = registry.get(id);
  const kids = node.children.length;
  if (!kids) throw new Error(`#${id} rendered empty`);
  console.log(`  #${id}: ${kids} child node(s)`);
}

// Bare text directly inside a <tr> means a table cell was not wrapped.
const tables = registry.get('errNodes').find((n) => n.tagName === 'TR');
for (const tr of tables) {
  for (const kid of tr.children) {
    if (!(kid instanceof Node)) throw new Error(`unwrapped table cell: "${kid.data}"`);
    if (!['TD', 'TH'].includes(kid.tagName)) throw new Error(`row child is <${kid.tagName}>, expected TD/TH`);
  }
}
console.log(`  node table rows: ${tables.length} (all cells wrapped)`);

console.log('--- expanding every event and counter row ---');
let opened = 0;
for (const box of ['errEvents', 'errCounters']) {
  for (const row of registry.get(box).children) {
    const head = row.children[0];
    head.fire('click');
    opened += 1;
  }
}
console.log(`  expanded ${opened} row(s) without error`);

console.log('--- toggling routine cluster chatter ---');
registry.get('hideRoutine').checked = false;
registry.get('hideRoutine').fire('change');
console.log(`  #errCluster: ${registry.get('errCluster').children.length} child node(s)`);

console.log('--- switching tabs ---');
function assertOneTabVisible(expected) {
  const dash = registry.get('dash').hidden;
  const err = registry.get('errdash').hidden;
  const visible = [!dash && 'latency', !err && 'errors'].filter(Boolean);
  if (visible.length > 1) throw new Error(`both tabs visible at once: ${visible.join(' + ')}`);
  if (expected && visible[0] !== expected) {
    throw new Error(`expected the ${expected} tab visible, got ${visible[0] || 'nothing'}`);
  }
  console.log(`  visible: ${visible[0] || 'neither'} (dash hidden=${dash}, errdash hidden=${err})`);
}
tabButtons[1].fire('click');
assertOneTabVisible('errors');

// No latency run yet, so the latency tab must show the hint, not empty panels.
api.state.files = [{
  id: 'f1', name: 'aerospike.log', size: 53_000_000, dumps: 1240,
  node: { ip: '10.12.72.18', port: 3000, ip_source: 'log', node_id: 'bb9', build: '6.3.0.32' },
  histograms: [{ name: '{ns}-read', dumps: 1240, unit: 'msec' }],
  first_iso: '2026-09-23 20:15:00', last_iso: '2026-09-23 20:30:00',
}];
tabButtons[0].fire('click');
assertOneTabVisible(null);
if (registry.get('tabHint').hidden) throw new Error('latency tab is empty but no hint is shown');
console.log(`  hint shown: "${registry.get('tabHint').textContent}"`);

if (latency) {
  console.log('--- rendering latency tab with the same session ---');
  api.state.tab = 'latency';
  api.state.payload = latency;
  api.renderDashboard(latency);
  assertOneTabVisible('latency');
  for (const id of ['statTiles', 'headlinePanel', 'opsChart', 'latChart', 'opTable', 'nodeTable', 'detailList']) {
    if (!registry.get(id).children.length) throw new Error(`#${id} rendered empty`);
  }
  console.log('  latency panels still render');
  const rows = registry.get('detailList').children;
  rows.slice(0, 3).forEach((row) => row.children[0].fire('click'));
  console.log(`  expanded ${Math.min(3, rows.length)} latency detail row(s)`);
  if (!registry.get('tabHint').hidden) throw new Error('hint still shown after a latency run');

  console.log('--- round trip: errors -> latency -> errors ---');
  tabButtons[1].fire('click');
  assertOneTabVisible('errors');
  tabButtons[0].fire('click');
  assertOneTabVisible('latency');
  tabButtons[1].fire('click');
  assertOneTabVisible('errors');
}

console.log('--- timezone conversion ---');
const SAMPLE = '2026-09-23 20:19:44';   // GMT, as the backend emits it
const expect = (got, want, what) => {
  if (got !== want) throw new Error(`${what}: got "${got}", expected "${want}"`);
  console.log(`  ${what}: ${got}`);
};

// Default must be IST, since that is what the logs get read in.
expect(api.tz().label, 'IST', 'default zone');
expect(api.fmtStamp(SAMPLE), '2026-09-24 01:49:44', 'GMT stamp shown in IST');

// Prose composed server-side has its stamps and its "GMT" word rewritten together.
const prose = api.localize(`node evicted at ${SAMPLE} GMT on 10.12.72.18`);
expect(prose, 'node evicted at 2026-09-24 01:49:44 IST on 10.12.72.18', 'headline prose');
if (/GMT/.test(prose)) throw new Error('prose still claims GMT after localizing');

// A raw log line must stay verbatim so it can still be grepped out of the file.
const raw = 'Sep 23 2026 20:19:44 GMT: WARNING (hb): (hb.c:4728) node arrived';
if (api.localize(raw) !== raw.replace('GMT', 'IST')) {
  console.log('  note: raw log lines keep their own stamp (only the zone word changes)');
}

// The window fields read in the display zone but must reach the backend as GMT.
registry.get('windowMode').value = 'range';
registry.get('fromDt').value = '2026-09-24T01:45:00';
registry.get('toDt').value = '2026-09-24T02:00:00';
const sent = api.requestedWindow();
expect(`${sent.from} → ${sent.to}`, '2026-09-23 20:15:00 → 2026-09-23 20:30:00',
  'IST window sent to backend as GMT');

// Switching zones shifts the labels but must not move the selected instant.
api.applyTz('gmt');
expect(api.tz().label, 'GMT', 'switched zone');
expect(registry.get('fromDt').value, '2026-09-23T20:15:00', 'field rewritten in GMT');
const same = api.requestedWindow();
expect(`${same.from} → ${same.to}`, '2026-09-23 20:15:00 → 2026-09-23 20:30:00',
  'same instant after switching');
expect(api.fmtStamp(SAMPLE), SAMPLE, 'GMT mode shows the log stamp unchanged');
if (store.get('aslat.tz') !== 'gmt') throw new Error('zone choice was not persisted');
console.log('  choice persisted as', store.get('aslat.tz'));

api.applyTz('ist');
expect(registry.get('fromDt').value, '2026-09-24T01:45:00', 'field back in IST');
if (registry.get('dash').hidden && registry.get('errdash').hidden) {
  throw new Error('switching zones left both dashboards hidden');
}
assertOneTabVisible(null);

// The log card shows each file's coverage, which must follow the display zone too.
const card = registry.get('logsGrid').text();
if (!card.includes('2026-09-24 01:45:00') || !card.includes('IST')) {
  throw new Error(`log card coverage not in IST: ${card.slice(0, 200)}`);
}
console.log('  log card coverage: 2026-09-24 01:45:00 → … IST');

const headline = registry.get('errHeadline').text();
if (/GMT/.test(headline)) throw new Error('errors headline still shows GMT while in IST mode');
console.log('  errors headline carries no GMT in IST mode');
console.log('\nheadline text rendered:');
headline.split('  ').filter((s) => s.trim()).slice(0, 8).forEach((s) => console.log('   ', s.trim()));
console.log('\nALL RENDER CHECKS PASSED');
}

(async function checkClearUploads() {
  console.log('--- clearing manual uploads for a new analysis ---');
  api.state.dataSource = 'upload';
  registry.get('dataSource').value = 'upload';
  Object.assign(api.state, {
    session: 'upload-session', files: [{ id: 'uploaded-log' }],
    payload: {}, errors: {}, range: [1, 2], errorsKey: 'old-analysis',
  });
  api.state.selectedFiles.add('uploaded-log');
  api.state.selectedHists.add('read');
  registry.get('windowMode').value = 'range';
  registry.get('fromDt').value = '2026-11-23T10:00:00';
  registry.get('toDt').value = '2026-11-23T11:00:00';
  api.showTab();
  if (registry.get('clearUploadsBtn').hidden) throw new Error('Clear is hidden after manual analysis');
  const clear = registry.get('clearUploadsBtn').listeners.click[0];
  if (clear !== registry.get('clearSession').listeners.click[0]) throw new Error('Clear buttons use different resets');
  const originalFetch = global.fetch;
  try {
    global.fetch = async () => ({ ok: false, status: 500 });
    await clear();
    if (api.state.session !== 'upload-session') throw new Error('failed reset discarded the session');
    global.fetch = async (url, options) => {
      if (url !== '/api/reset' || options.method !== 'POST'
        || JSON.parse(options.body).session !== 'upload-session') throw new Error('incorrect reset request');
      return { ok: true };
    };
    await clear();
  } finally { global.fetch = originalFetch; }
  if (api.state.session || api.state.files.length || api.state.payload || api.state.errors
    || api.state.range || api.state.errorsKey || api.state.selectedFiles.size || api.state.selectedHists.size) {
    throw new Error('Clear left old logs or analysis in state');
  }
  if (api.state.dataSource !== 'upload' || registry.get('dropzone').hidden
    || !registry.get('dash').hidden || !registry.get('errdash').hidden
    || !registry.get('clearUploadsBtn').hidden || registry.get('windowMode').value !== 'full'
    || registry.get('fromDt').value || registry.get('toDt').value
    || registry.get('logsCount').textContent !== '0') throw new Error('Clear did not reset the upload UI');
  const clicks = registry.get('fileInput').clicks;
  registry.get('uploadBtn').fire('click');
  if (registry.get('fileInput').clicks !== clicks + 1) throw new Error('new upload picker failed after Clear');
  console.log('  reset failure, successful clear, empty state, and next upload passed');
}()).catch((err) => { console.error(err); process.exitCode = 1; });
