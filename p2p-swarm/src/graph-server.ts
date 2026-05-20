// File: GRAPH SERVER — visualizador del swarm en vivo con panel lateral.
// Created: 2026-05-19
// Updated: 2026-05-19
// Author: Erick Hernández Silva

import http from 'node:http';
import { type AddressInfo } from 'node:net';
import { createLogger } from './logger.js';

const log = createLogger('graph-server');

export interface GraphSnapshot {
  nodes: Array<{ id: string; label: string; color: { background: string; border: string }; size: number; title?: string }>;
  edges: Array<{ id: string; from: string; to: string; dashes: boolean; color: { color: string }; width: number }>;
  stats: { total: number; mutuals: number; oneway: number; density: string; maxPeers: number };
  directPeers: string[];
  ts: number;
}

export type GraphEvent =
  | ({ kind: 'snapshot' } & GraphSnapshot)
  | { kind: 'toast'; text: string }
  | { kind: 'search-path'; searchId: string; path: string[]; status: 'found' | 'timeout' }
  | { kind: 'msg-received'; fromId: string; text: string; hops: number };

export interface GraphActions {
  sendMsg(targetId: string, text: string): Promise<void>;
  findPeer(targetId: string): Promise<string[]>;
}

export class GraphServer {
  private server?: http.Server;
  private clients = new Set<http.ServerResponse>();
  private port = 0;
  private lastSnap?: GraphSnapshot;
  private actions?: GraphActions;

  async start(): Promise<number> {
    this.server = http.createServer((req, res) => { void this.handle(req, res); });
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(0, '127.0.0.1', () => {
        this.port = (this.server!.address() as AddressInfo).port;
        resolve();
      });
    });
    log.info(`graph server en ${this.url()}`);
    return this.port;
  }

  url(): string { return `http://127.0.0.1:${this.port}/`; }

  setActions(actions: GraphActions): void { this.actions = actions; }

  emit(snap: GraphSnapshot): void {
    this.lastSnap = snap;
    this.broadcast({ kind: 'snapshot', ...snap });
  }

  toast(text: string): void { this.broadcast({ kind: 'toast', text }); }

  searchPath(searchId: string, path: string[], status: 'found' | 'timeout'): void {
    this.broadcast({ kind: 'search-path', searchId, path, status });
  }

  msgReceived(fromId: string, text: string, hops: number): void {
    this.broadcast({ kind: 'msg-received', fromId, text, hops });
  }

  hasClients(): boolean { return this.clients.size > 0; }

  stop(): void {
    for (const c of this.clients) { try { c.end(); } catch { /* ignore */ } }
    this.clients.clear();
    this.server?.close();
  }

  private broadcast(ev: GraphEvent): void {
    const payload = `data: ${JSON.stringify(ev)}\n\n`;
    for (const c of this.clients) { try { c.write(payload); } catch { /* ignore */ } }
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (req.url === '/events') { this.handleSse(req, res); return; }

    if (req.method === 'POST' && req.url === '/api/send') {
      const body = await readBody(req);
      try {
        const { targetId, text } = JSON.parse(body) as { targetId: string; text: string };
        await this.actions?.sendMsg(targetId, text);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: (err as Error).message }));
      }
      return;
    }

    if (req.method === 'POST' && req.url === '/api/find') {
      const body = await readBody(req);
      try {
        const { targetId } = JSON.parse(body) as { targetId: string };
        const path = await this.actions?.findPeer(targetId) ?? [];
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, path }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: (err as Error).message }));
      }
      return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
  }

  private handleSse(_req: http.IncomingMessage, res: http.ServerResponse): void {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write(': hola\n\n');
    this.clients.add(res);
    if (this.lastSnap) {
      try { res.write(`data: ${JSON.stringify({ kind: 'snapshot', ...this.lastSnap })}\n\n`); } catch { /* ignore */ }
    }
    _req.on('close', () => this.clients.delete(res));
  }
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

// ---------------------------------------------------------------------------
// HTML embebido — vis-network + panel lateral + tema oscuro.
// Todo contenido dinámico se inserta via textContent / createElement.
// ---------------------------------------------------------------------------

const PAGE_HTML = `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>p2p-swarm — grafo en vivo</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"><\/script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0f1117; color: #e2e8f0; font: 13px/1.5 monospace; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }
#hdr { padding: 8px 14px; background: #1a1f2e; border-bottom: 1px solid #2d3748; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
#hdr h1 { font-size: 14px; color: #63b3ed; }
#status { font-size: 12px; color: #48bb78; }
#status.off { color: #fc8181; }
#stats { font-size: 11px; color: #718096; flex: 1; text-align: right; }
#main { display: flex; flex: 1; overflow: hidden; }
#net { flex: 1; }
#panel { width: 300px; background: #1a1f2e; border-left: 1px solid #2d3748; display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; }
.panel-section { display: flex; flex-direction: column; overflow: hidden; }
.panel-section.peers { flex: 0 0 180px; border-bottom: 1px solid #2d3748; }
.panel-section.log   { flex: 1; border-bottom: 1px solid #2d3748; }
.panel-title { font-size: 11px; color: #718096; padding: 6px 10px; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3748; flex-shrink: 0; }
#peers-list, #msg-log { flex: 1; overflow-y: auto; padding: 4px; }
.peer-item { padding: 4px 8px; cursor: pointer; border-radius: 3px; font-size: 12px; display: flex; align-items: center; gap: 6px; }
.peer-item:hover { background: #2d3748; }
.pdot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.pdot.direct { background: #48bb78; }
.pdot.gossip { background: #4a5568; }
.msg-item { padding: 3px 8px; font-size: 11px; border-radius: 2px; margin-bottom: 2px; }
.msg-item.recv { color: #90cdf4; border-left: 2px solid #3182ce; }
.msg-item.sent { color: #9ae6b4; border-left: 2px solid #38a169; }
.msg-item.info { color: #718096; font-style: italic; }
#compose { padding: 8px; display: flex; flex-direction: column; gap: 6px; flex-shrink: 0; }
#target-in, #text-in { background: #0f1117; border: 1px solid #2d3748; color: #e2e8f0; padding: 5px 8px; font: 12px monospace; border-radius: 3px; width: 100%; }
#target-in:focus, #text-in:focus { outline: none; border-color: #4a5568; }
#text-in { resize: none; height: 52px; }
#btn-row { display: flex; gap: 6px; }
.btn { background: #2d3748; border: 1px solid #4a5568; color: #e2e8f0; padding: 5px 10px; font: 12px monospace; border-radius: 3px; cursor: pointer; flex: 1; }
.btn:hover { background: #4a5568; }
.btn.pri { background: #2b6cb0; border-color: #3182ce; }
.btn.pri:hover { background: #3182ce; }
#legend { padding: 6px 14px; background: #1a1f2e; border-top: 1px solid #2d3748; font-size: 11px; color: #718096; display: flex; gap: 16px; align-items: center; flex-shrink: 0; }
.lme { color: #68d391; } .lpeer { color: #63b3ed; } .lmut { color: #48bb78; } .lone { color: #718096; }
#slog { position: fixed; bottom: 40px; left: 14px; width: 280px; max-height: 160px; overflow-y: auto; display: flex; flex-direction: column; gap: 4px; pointer-events: none; }
.slog-item { background: #2d3748cc; border-left: 3px solid #f6ad55; padding: 4px 8px; font-size: 11px; border-radius: 3px; animation: fi .2s ease-out, fo .4s ease-in 4s forwards; }
.slog-item.found { border-left-color: #68d391; }
.slog-item.timeout { border-left-color: #fc8181; }
@keyframes fi { from { opacity:0; transform: translateX(-10px) } to { opacity:1; transform: translateX(0) } }
@keyframes fo { to { opacity:0 } }
#toasts { position: fixed; bottom: 14px; left: 14px; display: flex; flex-direction: column; gap: 6px; pointer-events: none; }
.toast { background: #2d3748; border: 1px solid #4a5568; padding: 6px 12px; border-radius: 4px; font-size: 12px; animation: fi .25s ease-out, fo .4s ease-in 3.6s forwards; }
</style>
</head>
<body>
<div id="hdr">
  <h1>p2p-swarm</h1>
  <span id="status" class="off">conectando…</span>
  <span id="stats"></span>
</div>
<div id="main">
  <div id="net"></div>
  <div id="panel">
    <div class="panel-section peers">
      <div class="panel-title">Peers</div>
      <div id="peers-list"></div>
    </div>
    <div class="panel-section log">
      <div class="panel-title">Mensajes</div>
      <div id="msg-log"></div>
    </div>
    <div id="compose">
      <input id="target-in" type="text" placeholder="peer ID o prefijo" list="peer-dl">
      <datalist id="peer-dl"></datalist>
      <textarea id="text-in" placeholder="mensaje…"></textarea>
      <div id="btn-row">
        <button class="btn" id="btn-find">Find</button>
        <button class="btn pri" id="btn-send">Send</button>
      </div>
    </div>
  </div>
</div>
<div id="legend">
  <span class="lme">● tú</span>
  <span class="lpeer">● peers</span>
  <span class="lmut">— mutua</span>
  <span class="lone">- - unidireccional</span>
  <span style="margin-left:auto">MAX_PEERS=<span id="maxp">?</span> · click nodo → selecciona</span>
</div>
<div id="slog"></div>
<div id="toasts"></div>
<script>
(function () {
  'use strict';
  const vNodes   = new vis.DataSet([]);
  const vEdges   = new vis.DataSet([]);
  const network  = new vis.Network(document.getElementById('net'), { nodes: vNodes, edges: vEdges }, {
    nodes:   { shape: 'dot', font: { color: '#fff', size: 13, face: 'monospace' } },
    edges:   { smooth: { type: 'continuous' } },
    physics: { stabilization: false, barnesHut: { springLength: 200, gravitationalConstant: -4000, damping: 0.4 } },
    interaction: { hover: true, dragNodes: true, tooltipDelay: 100 },
  });

  const statusEl  = document.getElementById('status');
  const statsEl   = document.getElementById('stats');
  const maxpEl    = document.getElementById('maxp');
  const slogEl    = document.getElementById('slog');
  const toastsEl  = document.getElementById('toasts');
  const peersEl   = document.getElementById('peers-list');
  const msgLogEl  = document.getElementById('msg-log');
  const targetIn  = document.getElementById('target-in');
  const textIn    = document.getElementById('text-in');
  const peerDl    = document.getElementById('peer-dl');
  const origAttrs = new Map();
  let directSet   = new Set();

  // ── Peer list ──────────────────────────────────────────────────────────────

  function updatePeers(nodeList, dPeers) {
    directSet = new Set(dPeers);
    // Repopulate peers list — DOM only, no string injection
    while (peersEl.firstChild) peersEl.removeChild(peersEl.firstChild);
    while (peerDl.firstChild)  peerDl.removeChild(peerDl.firstChild);
    for (const n of nodeList) {
      const row  = document.createElement('div');
      row.className = 'peer-item';
      row.title = n.id;
      const dot  = document.createElement('span');
      dot.className = 'pdot ' + (directSet.has(n.id) ? 'direct' : 'gossip');
      const lbl  = document.createElement('span');
      lbl.textContent = n.label || n.id.slice(0, 8);
      row.appendChild(dot);
      row.appendChild(lbl);
      row.addEventListener('click', () => selectPeer(n.id));
      peersEl.appendChild(row);
      const opt  = document.createElement('option');
      opt.value  = n.id.slice(0, 8);
      peerDl.appendChild(opt);
    }
  }

  function selectPeer(id) {
    targetIn.value = id.slice(0, 8);
    targetIn.focus();
  }

  // ── Message log ────────────────────────────────────────────────────────────

  function addLog(type, text) {
    const el = document.createElement('div');
    el.className = 'msg-item ' + type;
    el.textContent = text;
    msgLogEl.appendChild(el);
    msgLogEl.scrollTop = msgLogEl.scrollHeight;
  }

  // ── Snapshot ───────────────────────────────────────────────────────────────

  function applySnapshot(snap) {
    const prevNodes = new Set(vNodes.getIds());
    const prevEdges = new Set(vEdges.getIds());
    const newNodes  = new Set(snap.nodes.map(n => n.id));
    const newEdges  = new Set(snap.edges.map(e => e.id));

    for (const id of prevNodes) if (!newNodes.has(id)) { vNodes.remove(id); origAttrs.delete(id); }
    for (const id of prevEdges) if (!newEdges.has(id)) vEdges.remove(id);

    const arriving = snap.nodes.filter(n => !prevNodes.has(n.id));
    if (arriving.length) {
      vNodes.update(arriving.map(n => ({ ...n, size: 3 })));
      setTimeout(() => vNodes.update(arriving.map(n => ({ id: n.id, size: n.size }))), 120);
    }
    vNodes.update(snap.nodes.filter(n => prevNodes.has(n.id)));

    const newE = snap.edges.filter(e => !prevEdges.has(e.id));
    vEdges.update(snap.edges);
    if (newE.length) {
      setTimeout(() => {
        vEdges.update(newE.map(e => ({ id: e.id, color: { color: '#f1c40f' }, width: (e.width || 1) + 3 })));
        setTimeout(() => vEdges.update(newE.map(e => ({ id: e.id, color: e.color, width: e.width }))), 700);
      }, 200);
    }

    for (const n of snap.nodes) origAttrs.set(n.id, { color: n.color, size: n.size });

    const s = snap.stats;
    statsEl.textContent = 'nodos=' + s.total + ' · mutuas=' + s.mutuals +
      ' · un-sentido=' + s.oneway + ' · densidad=' + s.density +
      '  ·  ' + new Date(snap.ts).toLocaleTimeString();
    if (maxpEl) maxpEl.textContent = '' + s.maxPeers;
    updatePeers(snap.nodes, snap.directPeers || []);
  }

  // ── Search path ────────────────────────────────────────────────────────────

  function applySearchPath(ev) {
    const cls     = ev.status === 'found' ? 'found' : 'timeout';
    const icon    = ev.status === 'found' ? '🎯' : '⏱';
    const pathStr = ev.path.map(id => id.slice(0, 8)).join(' → ');
    const el      = document.createElement('div');
    el.className  = 'slog-item ' + cls;
    el.textContent = icon + ' [' + ev.searchId.slice(0, 6) + '] ' + pathStr;
    slogEl.appendChild(el);
    setTimeout(() => el.remove(), 5000);

    if (ev.status !== 'found') return;

    const PATH_COLOR = '#e74c3c';
    const STEP_MS    = 250;
    const HOLD_MS    = 2500;

    ev.path.forEach((nid, i) => {
      setTimeout(() => {
        const orig = origAttrs.get(nid);
        if (!orig) return;
        vNodes.update({ id: nid, color: { background: PATH_COLOR, border: PATH_COLOR }, size: orig.size + 14 });
        setTimeout(() => vNodes.update({ id: nid, color: orig.color, size: orig.size }), HOLD_MS);
      }, i * STEP_MS);
    });

    for (let i = 0; i < ev.path.length - 1; i++) {
      const a = ev.path[i], b = ev.path[i + 1];
      const eid = a < b ? a + '|' + b : b + '|' + a;
      setTimeout(() => {
        const edge = vEdges.get(eid);
        if (!edge) return;
        const oc = edge.color, ow = edge.width;
        vEdges.update({ id: eid, color: { color: PATH_COLOR }, width: (ow || 1) + 5 });
        setTimeout(() => vEdges.update({ id: eid, color: oc, width: ow }), HOLD_MS);
      }, i * STEP_MS + STEP_MS / 2);
    }

    if (ev.path.length > 0) {
      const target = ev.path[ev.path.length - 1];
      addLog('info', '🎯 ' + target.slice(0, 8) + ' — ' + (ev.path.length - 1) + ' salto(s)');
    }
  }

  // ── Msg received ───────────────────────────────────────────────────────────

  function applyMsgReceived(ev) {
    addLog('recv', '[' + ev.fromId.slice(0, 8) + ' (' + ev.hops + ' hop' + (ev.hops !== 1 ? 's' : '') + ')]: ' + ev.text);
    const orig = origAttrs.get(ev.fromId);
    if (orig) {
      vNodes.update({ id: ev.fromId, color: { background: '#f39c12', border: '#e67e22' }, size: orig.size + 10 });
      setTimeout(() => vNodes.update({ id: ev.fromId, color: orig.color, size: orig.size }), 1200);
    }
  }

  // ── Toast ──────────────────────────────────────────────────────────────────

  function showToast(text) {
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = text;
    toastsEl.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  // ── Actions ────────────────────────────────────────────────────────────────

  async function doFind() {
    const target = targetIn.value.trim();
    if (!target) return;
    addLog('info', 'buscando ' + target + '…');
    try {
      const r = await fetch('/api/find', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targetId: target }),
      });
      const d = await r.json();
      if (d.ok) addLog('info', '🎯 ruta: ' + d.path.map(id => id.slice(0, 8)).join(' → '));
      else      addLog('info', '✗ ' + d.error);
    } catch (err) { addLog('info', '✗ ' + String(err)); }
  }

  async function doSend() {
    const target = targetIn.value.trim();
    const text   = textIn.value.trim();
    if (!target || !text) return;
    addLog('sent', '[→ ' + target + ']: ' + text);
    textIn.value = '';
    try {
      const r = await fetch('/api/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targetId: target, text }),
      });
      const d = await r.json();
      if (!d.ok) addLog('info', '✗ ' + d.error);
    } catch (err) { addLog('info', '✗ ' + String(err)); }
  }

  document.getElementById('btn-find').addEventListener('click', doFind);
  document.getElementById('btn-send').addEventListener('click', doSend);
  textIn.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doSend(); }
  });

  network.on('selectNode', (p) => {
    const id = p.nodes[0];
    if (id) { selectPeer(id); showToast(id); }
  });

  // ── SSE ────────────────────────────────────────────────────────────────────

  const sse = new EventSource('/events');
  sse.onopen  = () => { statusEl.textContent = 'en vivo'; statusEl.classList.remove('off'); };
  sse.onerror = () => { statusEl.textContent = 'reconectando…'; statusEl.classList.add('off'); };
  sse.onmessage = (m) => {
    try {
      const data = JSON.parse(m.data);
      if      (data.kind === 'snapshot')      applySnapshot(data);
      else if (data.kind === 'search-path')   applySearchPath(data);
      else if (data.kind === 'msg-received')  applyMsgReceived(data);
      else if (data.kind === 'toast')         showToast(data.text);
    } catch (err) { console.error('evento inválido', err); }
  };
})();
<\/script>
</body>
</html>`;
