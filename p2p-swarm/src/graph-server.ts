// File: GRAPH SERVER — visualizador del swarm en vivo.
// Created: 2026-05-19
// Updated: 2026-05-19
// Author: Erick Hernández Silva

/**
 * GRAPH SERVER — visualizador del swarm en vivo.
 *
 * Igual que en p2p-chat: HTTP local + SSE. Añade un evento extra
 * `search-path` que anima el camino encontrado por el flood de búsqueda.
 */

import http from 'node:http';
import { type AddressInfo } from 'node:net';
import { createLogger } from './logger.js';

const log = createLogger('graph-server');

export interface GraphSnapshot {
  nodes: Array<{ id: string; label: string; color: { background: string; border: string }; size: number; title?: string }>;
  edges: Array<{ id: string; from: string; to: string; dashes: boolean; color: { color: string }; width: number }>;
  stats: { total: number; mutuals: number; oneway: number; density: string; maxPeers: number };
  ts: number;
}

export type GraphEvent =
  | ({ kind: 'snapshot' } & GraphSnapshot)
  | { kind: 'toast'; text: string }
  | { kind: 'search-path'; searchId: string; path: string[]; status: 'found' | 'timeout' };

export class GraphServer {
  private server?: http.Server;
  private clients = new Set<http.ServerResponse>();
  private port = 0;
  private lastSnap?: GraphSnapshot;

  async start(): Promise<number> {
    this.server = http.createServer((req, res) => this.handle(req, res));
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

  emit(snap: GraphSnapshot): void {
    this.lastSnap = snap;
    this.broadcast({ kind: 'snapshot', ...snap });
  }

  toast(text: string): void { this.broadcast({ kind: 'toast', text }); }

  searchPath(searchId: string, path: string[], status: 'found' | 'timeout'): void {
    this.broadcast({ kind: 'search-path', searchId, path, status });
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

  private handle(req: http.IncomingMessage, res: http.ServerResponse): void {
    if (req.url === '/events') return this.handleSse(req, res);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
  }

  private handleSse(_req: http.IncomingMessage, res: http.ServerResponse): void {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write(': hola\n\n');
    this.clients.add(res);
    if (this.lastSnap) {
      try { res.write(`data: ${JSON.stringify({ kind: 'snapshot', ...this.lastSnap })}\n\n`); } catch { /* ignore */ }
    }
    _req.on('close', () => this.clients.delete(res));
  }
}

// ---------------------------------------------------------------------------
// HTML embebido — vis-network + tema oscuro + animación de camino de búsqueda.
// ---------------------------------------------------------------------------

const PAGE_HTML = `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>p2p-swarm — grafo en vivo</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0f1117; color: #e2e8f0; font: 13px/1.5 monospace; display: flex; flex-direction: column; height: 100vh; }
#header { padding: 8px 14px; background: #1a1f2e; border-bottom: 1px solid #2d3748; display: flex; align-items: center; gap: 16px; }
#header h1 { font-size: 14px; color: #63b3ed; }
#status { font-size: 12px; color: #48bb78; }
#status.off { color: #fc8181; }
#stats { font-size: 11px; color: #718096; flex: 1; text-align: right; }
#net { flex: 1; }
#legend { padding: 6px 14px; background: #1a1f2e; border-top: 1px solid #2d3748; font-size: 11px; color: #718096; display: flex; gap: 16px; align-items: center; }
.me { color: #68d391; }
.peer { color: #63b3ed; }
.edge-mutual { color: #48bb78; }
.edge-one { color: #718096; }
#search-log { position: fixed; bottom: 40px; right: 14px; width: 320px; max-height: 160px; overflow-y: auto; display: flex; flex-direction: column; gap: 4px; pointer-events: none; }
.slog { background: #2d3748cc; border-left: 3px solid #f6ad55; padding: 4px 8px; font-size: 11px; border-radius: 3px; animation: fadein .2s ease-out, fadeout .4s ease-in 4s forwards; }
.slog.found { border-left-color: #68d391; }
.slog.timeout { border-left-color: #fc8181; }
@keyframes fadein { from { opacity:0; transform: translateX(10px) } to { opacity:1; transform: translateX(0) } }
@keyframes fadeout { to { opacity:0 } }
#toast-container { position: fixed; bottom: 14px; right: 14px; display: flex; flex-direction: column; gap: 6px; pointer-events: none; }
.toast { background: #2d3748; border: 1px solid #4a5568; padding: 6px 12px; border-radius: 4px; font-size: 12px; animation: fadein .25s ease-out, fadeout .4s ease-in 3.6s forwards; }
</style>
</head>
<body>
<div id="header">
  <h1>p2p-swarm</h1>
  <span id="status" class="off">conectando…</span>
  <span id="stats"></span>
</div>
<div id="net"></div>
<div id="legend">
  <span class="me">● tú</span>
  <span class="peer">● peers</span>
  <span class="edge-mutual">— mutua</span>
  <span class="edge-one">- - unidireccional</span>
  <span style="margin-left:auto">MAX_PEERS=<span id="maxp">?</span> · click nodo para info</span>
</div>
<div id="search-log"></div>
<div id="toast-container"></div>
<script>
(function () {
  const nodes = new vis.DataSet([]);
  const edges = new vis.DataSet([]);
  const network = new vis.Network(document.getElementById('net'), { nodes, edges }, {
    nodes: { shape: 'dot', font: { color: '#fff', size: 13, face: 'monospace' } },
    edges: { smooth: { type: 'continuous' } },
    physics: { stabilization: false, barnesHut: { springLength: 200, gravitationalConstant: -4000, damping: 0.4 } },
    interaction: { hover: true, dragNodes: true, tooltipDelay: 100 },
  });

  const statusEl    = document.getElementById('status');
  const statsEl     = document.getElementById('stats');
  const maxpEl      = document.getElementById('maxp');
  const searchLogEl = document.getElementById('search-log');
  const toastEl     = document.getElementById('toast-container');
  const origAttrs   = new Map();

  function applySnapshot(snap) {
    const prevNodeIds = new Set(nodes.getIds());
    const prevEdgeIds = new Set(edges.getIds());
    const newNodeIds  = new Set(snap.nodes.map(n => n.id));
    const newEdgeIds  = new Set(snap.edges.map(e => e.id));

    for (const id of prevNodeIds) if (!newNodeIds.has(id)) { nodes.remove(id); origAttrs.delete(id); }
    for (const id of prevEdgeIds) if (!newEdgeIds.has(id)) edges.remove(id);

    const arriving = snap.nodes.filter(n => !prevNodeIds.has(n.id));
    if (arriving.length) {
      nodes.update(arriving.map(n => ({ ...n, size: 3 })));
      setTimeout(() => nodes.update(arriving.map(n => ({ id: n.id, size: n.size }))), 120);
    }
    nodes.update(snap.nodes.filter(n => prevNodeIds.has(n.id)));

    const newEdges = snap.edges.filter(e => !prevEdgeIds.has(e.id));
    edges.update(snap.edges);
    if (newEdges.length) {
      setTimeout(() => {
        edges.update(newEdges.map(e => ({ id: e.id, color: { color: '#f1c40f' }, width: (e.width||1)+3 })));
        setTimeout(() => edges.update(newEdges.map(e => ({ id: e.id, color: e.color, width: e.width }))), 700);
      }, 200);
    }

    for (const n of snap.nodes) origAttrs.set(n.id, { color: n.color, size: n.size });

    const s = snap.stats;
    statsEl.textContent = 'nodos=' + s.total + ' · mutuas=' + s.mutuals + ' · un-sentido=' + s.oneway + ' · densidad=' + s.density + '  ·  ' + new Date(snap.ts).toLocaleTimeString();
    if (maxpEl) maxpEl.textContent = '' + s.maxPeers;
  }

  function applySearchPath(ev) {
    const cls = ev.status === 'found' ? 'found' : 'timeout';
    const icon = ev.status === 'found' ? '🎯' : '⏱';
    const pathStr = ev.path.map(id => id.slice(0,8)).join(' → ');
    const el = document.createElement('div');
    el.className = 'slog ' + cls;
    el.textContent = icon + ' [' + ev.searchId.slice(0,6) + '] ' + pathStr;
    searchLogEl.appendChild(el);
    setTimeout(() => el.remove(), 5000);

    if (ev.status !== 'found') return;

    // Animar path: nodos y arcos uno a uno con retraso acumulativo.
    const PATH_COLOR = '#e74c3c';
    const STEP_MS    = 250;
    const HOLD_MS    = 2500;

    ev.path.forEach((nodeId, i) => {
      const delay = i * STEP_MS;
      setTimeout(() => {
        const orig = origAttrs.get(nodeId);
        if (!orig) return;
        nodes.update({ id: nodeId, color: { background: PATH_COLOR, border: PATH_COLOR }, size: orig.size + 14 });
        setTimeout(() => nodes.update({ id: nodeId, color: orig.color, size: orig.size }), HOLD_MS);
      }, delay);
    });

    for (let i = 0; i < ev.path.length - 1; i++) {
      const a = ev.path[i], b = ev.path[i + 1];
      const edgeId = a < b ? a + '|' + b : b + '|' + a;
      const delay = i * STEP_MS + STEP_MS / 2;
      setTimeout(() => {
        const edge = edges.get(edgeId);
        if (!edge) return;
        const origC = edge.color, origW = edge.width;
        edges.update({ id: edgeId, color: { color: PATH_COLOR }, width: (origW||1) + 5 });
        setTimeout(() => edges.update({ id: edgeId, color: origC, width: origW }), HOLD_MS);
      }, delay);
    }
  }

  function showToast(text) {
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = text;
    toastEl.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  network.on('selectNode', (p) => {
    const id = p.nodes[0];
    if (id) showToast(id);
  });

  const ev = new EventSource('/events');
  ev.onopen  = () => { statusEl.textContent = 'en vivo'; statusEl.classList.remove('off'); };
  ev.onerror = () => { statusEl.textContent = 'reconectando…'; statusEl.classList.add('off'); };
  ev.onmessage = (m) => {
    try {
      const data = JSON.parse(m.data);
      if (data.kind === 'snapshot')    applySnapshot(data);
      else if (data.kind === 'search-path') applySearchPath(data);
      else if (data.kind === 'toast')  showToast(data.text);
    } catch (err) { console.error('evento inválido', err); }
  };
})();
</script>
</body>
</html>`;
